package runner

import "encoding/json"

func unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }
