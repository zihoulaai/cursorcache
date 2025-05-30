package cursorcache

import (
	"encoding/base64"
	"encoding/json"
)

func computeCursors[T any](data []T, extractCursor func(item T) string) (prev string, next string) {
	if len(data) == 0 {
		return "", ""
	}
	return extractCursor(data[0]), extractCursor(data[len(data)-1])
}

func EncodeCursor(c *ComplexCursor) string {
	if c == nil {
		return ""
	}
	b, _ := json.Marshal(c)
	return base64.StdEncoding.EncodeToString(b)
}

func DecodeCursor(s string) (*ComplexCursor, error) {
	var c ComplexCursor
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return &c, err
	}
	err = json.Unmarshal(b, &c)
	return &c, err
}
