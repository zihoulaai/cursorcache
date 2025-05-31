package cursorcache

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/go-redis/cache/v9"
	"golang.org/x/exp/slices"
)

func init() {
	rand.New(rand.NewSource(time.Now().UnixNano()))
}

// 常量定义
const (
	DefaultTTL         = 10 * time.Second // 默认缓存过期时间
	DefaultRandomRange = 10               // 默认随机抖动范围（秒）
	DefaultMinEmptyTTL = 30 * time.Second // 空数据时的最小过期时间
)

// CursorDirection 游标方向类型
type CursorDirection string

const (
	NextPage CursorDirection = "next" // 下一页
	PrevPage CursorDirection = "prev" // 上一页
)

// FetcherFunc 数据获取函数类型，返回数据、下一页游标、上一页游标和错误
type FetcherFunc[T any] func(ctx context.Context, cursor string, size int, dir CursorDirection) ([]T, string, string, error)

// TTLFunc 自定义 TTL 计算函数类型
type TTLFunc func(ctx context.Context, data interface{}) time.Duration

// BloomFilter 布隆过滤器接口
type BloomFilter interface {
	Exists(ctx context.Context, key string) (bool, error) // 判断 key 是否存在
	Add(ctx context.Context, key string) error            // 添加 key
}

// ComplexCursor 复合游标结构体
type ComplexCursor struct {
	CreatedAt time.Time `json:"created_at"` // 创建时间
	ID        int64     `json:"id"`         // 唯一标识
}

// CacheConfig 缓存配置
type CacheConfig struct {
	BloomFilter BloomFilter   // 布隆过滤器
	DefaultTTL  time.Duration // 默认 TTL
	RandomRange int           // 随机抖动范围
	CacheStore  *cache.Cache  // 缓存存储
}

// PageRequest 分页请求参数
type PageRequest[T any] struct {
	Cursor    string          // 当前游标
	Direction CursorDirection // 分页方向
	PageSize  int             // 每页大小
	KeyPrefix string          // 缓存键前缀
	Fetcher   FetcherFunc[T]  // 数据获取函数
}

// PageResponse 分页响应
type PageResponse[T any] struct {
	Data       []T    // 数据
	NextCursor string // 下一页游标
	PrevCursor string // 上一页游标
	FromCache  bool   // 是否来自缓存
}

// CursorCache 游标分页缓存核心结构体
type CursorCache[T any] struct {
	bloom       BloomFilter   // 布隆过滤器
	defaultTTL  time.Duration // 默认 TTL
	randomRange int           // 随机抖动范围
	cacheStore  *cache.Cache  // 缓存存储
}

// Option 配置函数类型
type Option[T any] func(*CursorCache[T])

// WithBloomFilter 设置布隆过滤器
func WithBloomFilter[T any](bf BloomFilter) Option[T] {
	return func(c *CursorCache[T]) {
		c.bloom = bf
	}
}

// WithTTL 设置默认 TTL
func WithTTL[T any](ttl time.Duration) Option[T] {
	return func(c *CursorCache[T]) {
		c.defaultTTL = ttl
	}
}

// WithRandomRange 设置随机抖动范围
func WithRandomRange[T any](sec int) Option[T] {
	return func(c *CursorCache[T]) {
		c.randomRange = sec
	}
}

// NewCursorCache 创建 CursorCache 实例
func NewCursorCache[T any](store *cache.Cache, opts ...Option[T]) *CursorCache[T] {
	if store == nil {
		panic("cacheStore is required")
	}
	cc := &CursorCache[T]{
		cacheStore:  store,
		defaultTTL:  DefaultTTL,
		randomRange: DefaultRandomRange,
	}
	for _, opt := range opts {
		opt(cc)
	}
	return cc
}

// Paginate 分页查询，自动缓存结果
func (c *CursorCache[T]) Paginate(ctx context.Context, req PageRequest[T]) (*PageResponse[T], error) {
	cacheKey := c.getCacheKey(req.KeyPrefix, req.Cursor, req.Direction)
	if c.shouldReturnEmpty(ctx, req.Cursor) {
		return &PageResponse[T]{Data: []T{}}, nil
	}
	var resp PageResponse[T]
	fromCache := true
	ttl := c.makeTTL(ctx, nil)
	err := c.cacheStore.Once(&cache.Item{
		Key:   cacheKey,
		Value: &resp,
		TTL:   ttl,
		Do: func(*cache.Item) (any, error) {
			fromCache = false
			data, next, prev, err := req.Fetcher(ctx, req.Cursor, req.PageSize, req.Direction)
			if err != nil {
				return nil, err
			}
			if req.Direction == PrevPage {
				slices.Reverse(data)
			}
			out := &PageResponse[T]{
				Data:       data,
				NextCursor: next,
				PrevCursor: prev,
				FromCache:  false,
			}
			go c.updateBloom(ctx, req.Cursor, next, prev)
			return out, nil
		},
	})
	if err != nil {
		return nil, err
	}
	resp.FromCache = fromCache
	return &resp, nil
}

// getCacheKey 生成缓存键
func (c *CursorCache[T]) getCacheKey(prefix, cursor string, dir CursorDirection) string {
	return fmt.Sprintf("%s:%s:%s", prefix, cursor, string(dir))
}

// shouldReturnEmpty 判断游标是否应返回空数据（布隆过滤器）
func (c *CursorCache[T]) shouldReturnEmpty(ctx context.Context, cursor string) bool {
	if c.bloom == nil || cursor == "" {
		return false
	}
	exists, err := c.bloom.Exists(ctx, cursor)
	return err == nil && !exists
}

// updateBloom 异步更新布隆过滤器
func (c *CursorCache[T]) updateBloom(ctx context.Context, cursors ...string) {
	if c.bloom == nil {
		return
	}
	for _, cur := range cursors {
		if cur != "" {
			_ = c.bloom.Add(ctx, cur)
		}
	}
}

// makeTTL 计算缓存 TTL，支持数据为空时特殊处理和随机抖动
func (c *CursorCache[T]) makeTTL(_ context.Context, data interface{}) time.Duration {
	resp, _ := data.(*PageResponse[T])
	dataLen := 0
	if resp != nil {
		dataLen = len(resp.Data)
	}
	if dataLen == 0 {
		if c.defaultTTL < DefaultMinEmptyTTL {
			return c.defaultTTL
		}
		return DefaultMinEmptyTTL
	}
	jitter := time.Duration(rand.Intn(c.randomRange)) * time.Second
	return c.defaultTTL + jitter
}

// EncodeCursor 编码 ComplexCursor 为 base64 字符串
func EncodeCursor(c *ComplexCursor) string {
	if c == nil {
		return ""
	}
	b, _ := json.Marshal(c)
	return base64.StdEncoding.EncodeToString(b)
}

// DecodeCursor 解码 base64 字符串为 ComplexCursor
func DecodeCursor(s string) (*ComplexCursor, error) {
	if s == "" {
		return nil, nil
	}
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}
	var c ComplexCursor
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, err
	}
	return &c, nil
}
