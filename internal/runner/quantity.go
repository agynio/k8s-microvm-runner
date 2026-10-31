package runner

import (
	"encoding/json"

	"k8s.io/apimachinery/pkg/api/resource"
)

func resourceParse(s string) (int64, error) {
	q, err := resource.ParseQuantity(s)
	if err != nil {
		return 0, err
	}
	return q.Value(), nil
}

func marshal(v any) ([]byte, error) { return json.Marshal(v) }
