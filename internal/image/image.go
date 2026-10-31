// Package image pulls OCI images through the image proxy with the workload's
// pull credential and unpacks them into directory trees that serve as a
// container's read-only lower layer on the host, once per image digest.
package image

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"go.uber.org/zap"

	"github.com/agynio/k8s-microvm-runner/internal/share"
)

// Meta is what the guest needs to run an image besides its bytes.
type Meta struct {
	Ref        string   `json:"ref"`
	Digest     string   `json:"digest"`
	Entrypoint []string `json:"entrypoint"`
	Cmd        []string `json:"cmd"`
	Env        []string `json:"env"`
	WorkingDir string   `json:"working_dir"`
	User       string   `json:"user"`
	UID        uint32   `json:"uid"`
	GID        uint32   `json:"gid"`
	// Dir is the unpacked root filesystem.
	Dir       string `json:"dir"`
	SizeBytes int64  `json:"size_bytes"`
}

// Credential is a pull credential for one registry host.
type Credential struct {
	Registry string
	Username string
	Password string
}

// Store unpacks images below Dir, one tree per digest.
type Store struct {
	Dir  string
	Arch string
	// CAFile adds a private CA to the system roots, for an image proxy whose
	// certificate is not publicly trusted.
	CAFile string
	Log    *zap.Logger

	transport http.RoundTripper
	mu        sync.Mutex
	locks     map[string]*sync.Mutex
}

// NewStore readies a store below dir. The transport trusts the system roots
// plus caFile, when given.
func NewStore(dir, caFile string, log *zap.Logger) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}
	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("read image proxy CA %s: %w", caFile, err)
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("image proxy CA %s holds no certificate", caFile)
		}
	}
	tr := remote.DefaultTransport.(*http.Transport).Clone()
	tr.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	if log == nil {
		log = zap.NewNop()
	}
	return &Store{Dir: dir, Arch: runtime.GOARCH, CAFile: caFile, Log: log, transport: tr, locks: map[string]*sync.Mutex{}}, nil
}

func (s *Store) lockFor(key string) *sync.Mutex {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.locks[key]
	if !ok {
		m = &sync.Mutex{}
		s.locks[key] = m
	}
	return m
}

// Ensure returns the unpacked rootfs for ref, pulling it on first use. The
// reference is resolved against the registry every time, so a tag that
// moved is noticed; the digest it resolves to is what is cached.
func (s *Store) Ensure(ctx context.Context, ref string, creds []Credential) (*Meta, error) {
	parsed, auth, err := s.resolve(ref, creds)
	if err != nil {
		return nil, err
	}
	t0 := time.Now()
	img, err := remote.Image(parsed, remote.WithContext(ctx), remote.WithAuth(auth),
		remote.WithTransport(s.transport),
		remote.WithPlatform(v1.Platform{OS: "linux", Architecture: s.Arch}))
	if err != nil {
		return nil, fmt.Errorf("pull %s: %w", ref, err)
	}
	dg, err := img.Digest()
	if err != nil {
		return nil, fmt.Errorf("digest %s: %w", ref, err)
	}
	l := s.lockFor(dg.String())
	l.Lock()
	defer l.Unlock()

	metaPath := filepath.Join(s.Dir, share.ImageDirName(dg.String())+".json")
	if data, err := os.ReadFile(metaPath); err == nil {
		var m Meta
		if json.Unmarshal(data, &m) == nil && m.Dir != "" {
			if _, err := os.Stat(m.Dir); err == nil {
				m.Ref = ref
				return &m, nil
			}
		}
	}

	cfg, err := img.ConfigFile()
	if err != nil {
		return nil, fmt.Errorf("config %s: %w", ref, err)
	}
	// The tree is unpacked under a name derived from the digest, so the same
	// image lands at the same path on every slot: a restored guest's open
	// files are re-opened by path.
	dir := filepath.Join(s.Dir, share.ImageDirName(dg.String()))
	if _, err := os.Stat(dir); err != nil {
		extract := dir + ".extract"
		_ = os.RemoveAll(extract)
		if err := os.MkdirAll(extract, 0o755); err != nil {
			return nil, err
		}
		rc := mutate.Extract(img)
		extractErr := share.ExtractTar(rc, extract)
		rc.Close()
		if extractErr != nil {
			_ = os.RemoveAll(extract)
			return nil, fmt.Errorf("extract %s: %w", ref, extractErr)
		}
		// The container mounts /proc, /sys and /dev over these; minimal
		// images lack them.
		for _, d := range []string{"proc", "sys", "dev", "etc"} {
			_ = os.Mkdir(filepath.Join(extract, d), 0o755)
		}
		if err := os.Rename(extract, dir); err != nil {
			return nil, err
		}
	}
	m := &Meta{
		Ref:        ref,
		Digest:     dg.String(),
		Entrypoint: cfg.Config.Entrypoint,
		Cmd:        cfg.Config.Cmd,
		Env:        cfg.Config.Env,
		WorkingDir: cfg.Config.WorkingDir,
		User:       cfg.Config.User,
		Dir:        dir,
		SizeBytes:  share.TreeBytes(dir),
	}
	m.UID, m.GID = resolveUser(dir, cfg.Config.User)
	data, _ := json.MarshalIndent(m, "", "  ")
	if err := os.WriteFile(metaPath, data, 0o644); err != nil {
		return nil, err
	}
	s.Log.Info("image unpacked", zap.String("ref", ref), zap.String("digest", dg.String()), zap.Int64("bytes", m.SizeBytes), zap.Duration("took", time.Since(t0).Round(time.Millisecond)))
	return m, nil
}

// Cached returns the unpacked image for a digest the slot already holds,
// or nil when it does not.
func (s *Store) Cached(digest string) *Meta {
	metaPath := filepath.Join(s.Dir, share.ImageDirName(digest)+".json")
	data, err := os.ReadFile(metaPath)
	if err != nil {
		return nil
	}
	var m Meta
	if json.Unmarshal(data, &m) != nil || m.Dir == "" {
		return nil
	}
	if _, err := os.Stat(m.Dir); err != nil {
		return nil
	}
	return &m
}

// ByDigest returns the unpacked image for repository@digest, pulling it when
// the slot lacks it. A wake pulls this way: the snapshot records the digest
// each container ran, and a tag that moved since must not change it.
func (s *Store) ByDigest(ctx context.Context, ref, digest string, creds []Credential) (*Meta, error) {
	if m := s.Cached(digest); m != nil {
		m.Ref = ref
		return m, nil
	}
	parsed, err := name.ParseReference(ref)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", ref, err)
	}
	return s.Ensure(ctx, parsed.Context().Name()+"@"+digest, creds)
}

func (s *Store) resolve(ref string, creds []Credential) (name.Reference, authn.Authenticator, error) {
	parsed, err := name.ParseReference(ref)
	if err != nil {
		return nil, nil, fmt.Errorf("parse %s: %w", ref, err)
	}
	host := parsed.Context().RegistryStr()
	var auth authn.Authenticator = authn.Anonymous
	for _, c := range creds {
		if registryMatches(c.Registry, host) {
			auth = &authn.Basic{Username: c.Username, Password: c.Password}
		}
	}
	return parsed, auth, nil
}

func registryMatches(declared, host string) bool {
	declared = strings.TrimSpace(strings.ToLower(declared))
	declared = strings.TrimPrefix(strings.TrimPrefix(declared, "https://"), "http://")
	declared = strings.TrimSuffix(declared, "/")
	host = strings.ToLower(host)
	if declared == host {
		return true
	}
	// docker.io is normalized to index.docker.io by the reference parser.
	return (declared == "docker.io" || declared == "index.docker.io") && (host == "docker.io" || host == "index.docker.io")
}

// resolveUser maps an image's USER directive to numeric ids using the
// image's own passwd and group files.
func resolveUser(root, user string) (uint32, uint32) {
	user = strings.TrimSpace(user)
	if user == "" {
		return 0, 0
	}
	parts := strings.SplitN(user, ":", 2)
	uid, uidOK := parseID(parts[0])
	var gid uint32
	gidOK := false
	if len(parts) == 2 {
		gid, gidOK = parseID(parts[1])
	}
	if !uidOK || (len(parts) == 2 && !gidOK) {
		pu, pg, found := lookupPasswd(filepath.Join(root, "etc/passwd"), parts[0])
		if found {
			if !uidOK {
				uid = pu
			}
			if !gidOK {
				gid = pg
				gidOK = true
			}
		}
	}
	if len(parts) == 2 && !gidOK {
		if g, ok := lookupGroup(filepath.Join(root, "etc/group"), parts[1]); ok {
			gid = g
			gidOK = true
		}
	}
	if !gidOK && uidOK {
		if _, pg, found := lookupPasswdByUID(filepath.Join(root, "etc/passwd"), uid); found {
			gid = pg
		}
	}
	return uid, gid
}

func parseID(s string) (uint32, bool) {
	n, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		return 0, false
	}
	return uint32(n), true
}

func lookupPasswd(path, name string) (uint32, uint32, bool) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, false
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Split(sc.Text(), ":")
		if len(fields) >= 4 && fields[0] == name {
			u, _ := parseID(fields[2])
			g, _ := parseID(fields[3])
			return u, g, true
		}
	}
	return 0, 0, false
}

func lookupPasswdByUID(path string, uid uint32) (uint32, uint32, bool) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, false
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Split(sc.Text(), ":")
		if len(fields) >= 4 {
			if u, ok := parseID(fields[2]); ok && u == uid {
				g, _ := parseID(fields[3])
				return u, g, true
			}
		}
	}
	return 0, 0, false
}

func lookupGroup(path, name string) (uint32, bool) {
	f, err := os.Open(path)
	if err != nil {
		return 0, false
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Split(sc.Text(), ":")
		if len(fields) >= 3 && fields[0] == name {
			return parseID(fields[2])
		}
	}
	return 0, false
}
