package cursorcache

import (
	"encoding/base64"
	"encoding/json"
)

// computeCursors 根据数据切片和提取游标的函数，返回前后游标字符串。
// data: 数据切片。
// extractCursor: 用于从每个元素提取游标的函数。
// 返回值: prev 为第一页元素的游标，next 为最后一页元素的游标。若数据为空，均返回空字符串。
func computeCursors[T any](data []T, extractCursor func(item T) string) (prev string, next string) {
	if len(data) == 0 {
		return "", ""
	}
	return extractCursor(data[0]), extractCursor(data[len(data)-1])
}

// EncodeCursor 将 ComplexCursor 对象编码为 base64 字符串。
// c: 需要编码的 ComplexCursor 指针。
// 返回值: 编码后的 base64 字符串，若 c 为 nil 返回空字符串。
func EncodeCursor(c *ComplexCursor) string {
	if c == nil {
		return ""
	}
	b, _ := json.Marshal(c)
	return base64.StdEncoding.EncodeToString(b)
}

// DecodeCursor 将 base64 字符串解码为 ComplexCursor 对象。
// s: 需要解码的 base64 字符串。
// 返回值: 解码后的 ComplexCursor 指针和错误信息。
func DecodeCursor(s string) (*ComplexCursor, error) {
	var c ComplexCursor
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return &c, err
	}
	err = json.Unmarshal(b, &c)
	return &c, err
}
