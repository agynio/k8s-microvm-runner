// Package store is the runner's client of the snapshot store: an
// S3-compatible bucket holding templates, workload snapshots and volume
// deltas under separate prefixes. Both the runner and the slot supervisors
// use it; the supervisors receive the configuration with the calls that
// need it.
package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Config reaches one bucket.
type Config struct {
	Endpoint  string
	Bucket    string
	Region    string
	AccessKey string
	SecretKey string
	UseTLS    bool
	PathStyle bool
	// Prefixes for the three kinds of object.
	SnapshotsPrefix string
	VolumesPrefix   string
	TemplatesPrefix string
}

// Default prefixes.
const (
	DefaultSnapshotsPrefix = "snapshots"
	DefaultVolumesPrefix   = "volumes"
	DefaultTemplatesPrefix = "templates"
)

// WithDefaults fills the prefixes left empty.
func (c Config) WithDefaults() Config {
	if c.SnapshotsPrefix == "" {
		c.SnapshotsPrefix = DefaultSnapshotsPrefix
	}
	if c.VolumesPrefix == "" {
		c.VolumesPrefix = DefaultVolumesPrefix
	}
	if c.TemplatesPrefix == "" {
		c.TemplatesPrefix = DefaultTemplatesPrefix
	}
	return c
}

// Configured reports whether a store is reachable at all.
func (c Config) Configured() bool { return c.Endpoint != "" && c.Bucket != "" }

// Client is one store.
type Client struct {
	cfg Config
	api *minio.Client
}

// New connects to the bucket.
func New(cfg Config) (*Client, error) {
	cfg = cfg.WithDefaults()
	if !cfg.Configured() {
		return nil, errors.New("snapshot store: endpoint and bucket are required")
	}
	lookup := minio.BucketLookupAuto
	if cfg.PathStyle {
		lookup = minio.BucketLookupPath
	}
	api, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:        credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure:       cfg.UseTLS,
		Region:       cfg.Region,
		BucketLookup: lookup,
	})
	if err != nil {
		return nil, fmt.Errorf("snapshot store: %w", err)
	}
	return &Client{cfg: cfg, api: api}, nil
}

// Config is what the client was built with, prefixes filled in.
func (c *Client) Config() Config { return c.cfg }

// Key joins a prefix and parts into one object key.
func Key(prefix string, parts ...string) string {
	return path.Join(append([]string{strings.Trim(prefix, "/")}, parts...)...)
}

// PutFile uploads a local file. It returns the object's size.
func (c *Client) PutFile(ctx context.Context, bucket, key, localPath string) (int64, error) {
	if bucket == "" {
		bucket = c.cfg.Bucket
	}
	info, err := c.api.FPutObject(ctx, bucket, key, localPath, minio.PutObjectOptions{})
	if err != nil {
		return 0, fmt.Errorf("put %s: %w", key, err)
	}
	return info.Size, nil
}

// PutBytes uploads a small object.
func (c *Client) PutBytes(ctx context.Context, bucket, key string, data []byte, contentType string) (int64, error) {
	if bucket == "" {
		bucket = c.cfg.Bucket
	}
	info, err := c.api.PutObject(ctx, bucket, key, strings.NewReader(string(data)), int64(len(data)), minio.PutObjectOptions{ContentType: contentType})
	if err != nil {
		return 0, fmt.Errorf("put %s: %w", key, err)
	}
	return info.Size, nil
}

// GetFile downloads an object to a local path.
func (c *Client) GetFile(ctx context.Context, bucket, key, localPath string) error {
	if bucket == "" {
		bucket = c.cfg.Bucket
	}
	if err := os.MkdirAll(path.Dir(localPath), 0o755); err != nil {
		return err
	}
	if err := c.api.FGetObject(ctx, bucket, key, localPath, minio.GetObjectOptions{}); err != nil {
		return fmt.Errorf("get %s: %w", key, err)
	}
	return nil
}

// GetBytes downloads a small object. A missing object is ErrNotFound.
func (c *Client) GetBytes(ctx context.Context, bucket, key string) ([]byte, error) {
	if bucket == "" {
		bucket = c.cfg.Bucket
	}
	obj, err := c.api.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("get %s: %w", key, err)
	}
	defer obj.Close()
	data, err := io.ReadAll(obj)
	if err != nil {
		if isNotFound(err) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get %s: %w", key, err)
	}
	return data, nil
}

// ErrNotFound says the object is not in the store.
var ErrNotFound = errors.New("object not found")

func isNotFound(err error) bool {
	var resp minio.ErrorResponse
	if errors.As(err, &resp) {
		return resp.Code == "NoSuchKey" || resp.StatusCode == 404
	}
	return false
}

// Stat returns an object's size, or ErrNotFound.
func (c *Client) Stat(ctx context.Context, bucket, key string) (int64, error) {
	if bucket == "" {
		bucket = c.cfg.Bucket
	}
	info, err := c.api.StatObject(ctx, bucket, key, minio.StatObjectOptions{})
	if err != nil {
		if isNotFound(err) {
			return 0, ErrNotFound
		}
		return 0, fmt.Errorf("stat %s: %w", key, err)
	}
	return info.Size, nil
}

// Object is one listed object.
type Object struct {
	Key  string
	Size int64
}

// List lists every object below prefix.
func (c *Client) List(ctx context.Context, bucket, prefix string) ([]Object, error) {
	if bucket == "" {
		bucket = c.cfg.Bucket
	}
	var out []Object
	for obj := range c.api.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if obj.Err != nil {
			return nil, fmt.Errorf("list %s: %w", prefix, obj.Err)
		}
		out = append(out, Object{Key: obj.Key, Size: obj.Size})
	}
	return out, nil
}

// DeletePrefix removes every object below prefix. Removing what is already
// gone is not an error.
func (c *Client) DeletePrefix(ctx context.Context, bucket, prefix string) error {
	if bucket == "" {
		bucket = c.cfg.Bucket
	}
	objects, err := c.List(ctx, bucket, prefix)
	if err != nil {
		return err
	}
	for _, obj := range objects {
		if err := c.api.RemoveObject(ctx, bucket, obj.Key, minio.RemoveObjectOptions{}); err != nil && !isNotFound(err) {
			return fmt.Errorf("delete %s: %w", obj.Key, err)
		}
	}
	return nil
}

// Ping checks the bucket is reachable.
func (c *Client) Ping(ctx context.Context) error {
	ok, err := c.api.BucketExists(ctx, c.cfg.Bucket)
	if err != nil {
		return fmt.Errorf("snapshot store: %w", err)
	}
	if !ok {
		return fmt.Errorf("snapshot store: bucket %s does not exist", c.cfg.Bucket)
	}
	return nil
}
