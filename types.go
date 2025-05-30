// Package cursorcache 提供基于游标的分页缓存工具，支持自定义数据类型和布隆过滤器。
package cursorcache

import (
	"context"
	"time"
)

// CursorDirection 表示分页的方向（下一页或上一页）。
type CursorDirection string

const (
	// NextPage 表示请求下一页数据。
	NextPage CursorDirection = "next"
	// PrevPage 表示请求上一页数据。
	PrevPage CursorDirection = "prev"
)

// PageRequest 封装分页请求的参数，包括游标、方向、页大小、键前缀、数据获取函数、TTL、随机TTL范围和布隆过滤器。
type PageRequest[T any] struct {
	Cursor      string          // 当前游标
	Direction   CursorDirection // 分页方向
	PageSize    int             // 每页数据量
	KeyPrefix   string          // 缓存键前缀
	Fetcher     FetcherFunc[T]  // 数据获取函数
	TTL         time.Duration   // 缓存过期时间
	RandomRange int             // 随机TTL范围，单位秒
	Bloom       BloomFilter     // 布隆过滤器
}

// PageResponse 封装分页响应的数据，包括数据列表、下一页和上一页游标，以及是否来自缓存的标记。
type PageResponse[T any] struct {
	Data       []T    // 当前页数据
	NextCursor string // 下一页游标
	PrevCursor string // 上一页游标
	FromCache  bool   // 是否来自缓存
}

// FetcherFunc 定义分页数据的获取函数签名。
type FetcherFunc[T any] func(ctx context.Context, cursor string, size int, dir CursorDirection) ([]T, string, string, error)

// BloomFilter 定义布隆过滤器接口，用于判断键是否存在及添加新键。
type BloomFilter interface {
	// Exists 判断指定键是否存在于布隆过滤器中。
	Exists(ctx context.Context, key string) (bool, error)
	// Add 向布隆过滤器中添加指定键。
	Add(ctx context.Context, key string)
}

// ComplexCursor 复合游标类型，包含创建时间和唯一ID。
type ComplexCursor struct {
	CreatedAt time.Time `json:"created_at"` // 创建时间
	ID        int64     `json:"id"`         // 唯一ID
}
