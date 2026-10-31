package guest

import (
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func protoJSON(msg proto.Message) ([]byte, error) {
	return protojson.MarshalOptions{Multiline: true, Indent: "  "}.Marshal(msg)
}
