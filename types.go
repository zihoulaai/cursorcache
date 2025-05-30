package cursorcache

import (
	"context"
	"time"
)

type CursorDirection string

const (
	NextPage CursorDirection = "next"
	PrevPage CursorDirection = "prev"
)

type PageRequest[T any] struct {
	Cursor      string
	Direction   CursorDirection
	PageSize    int
	KeyPrefix   string
	Fetcher     FetcherFunc[T]
	TTL         time.Duration
	RandomRange int // 随机TTL范围，单位秒
	Bloom       BloomFilter
}

type PageResponse[T any] struct {
	Data       []T
	NextCursor string
	PrevCursor string
	FromCache  bool
}

type FetcherFunc[T any] func(ctx context.Context, cursor string, size int, dir CursorDirection) ([]T, string, string, error)

type BloomFilter interface {
	Exists(ctx context.Context, key string) (bool, error)
	Add(ctx context.Context, key string)
}

// ComplexCursor 复合游标类型
type ComplexCursor struct {
	CreatedAt time.Time `json:"created_at"`
	ID        int64     `json:"id"`
}
