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

// 常量
const (
	DefaultTTL         = 10 * time.Second // 默认缓存时间
	DefaultRandomRange = 10               // 随机抖动范围
	DefaultMinEmptyTTL = 30 * time.Second
)

// CursorDirection 游标方向
type CursorDirection string

const (
	NextPage CursorDirection = "next"
	PrevPage CursorDirection = "prev"
)

// FetcherFunc 定义
type FetcherFunc[T any] func(ctx context.Context, cursor string, size int, dir CursorDirection) ([]T, string, string, error)

// TTLFunc 定义：注意这里改为 cache.Fetch 接受的签名
type TTLFunc func(ctx context.Context, data interface{}) time.Duration

// BloomFilter 接口（以 bits-and-blooms/bloom/v3 为例）
type BloomFilter interface {
	Exists(ctx context.Context, key string) (bool, error)
	Add(ctx context.Context, key string) error
}

// ComplexCursor 复合游标
type ComplexCursor struct {
	CreatedAt time.Time `json:"created_at"`
	ID        int64     `json:"id"`
}

type CacheConfig struct {
	//RedisClient redis.UniversalClient
	BloomFilter BloomFilter
	DefaultTTL  time.Duration
	RandomRange int
	CacheStore  *cache.Cache
}

type PageRequest[T any] struct {
	Cursor    string
	Direction CursorDirection
	PageSize  int
	KeyPrefix string
	Fetcher   FetcherFunc[T]
}

type PageResponse[T any] struct {
	Data       []T
	NextCursor string
	PrevCursor string
	FromCache  bool
}

type CursorCache[T any] struct {
	//rdb         redis.UniversalClient
	bloom       BloomFilter
	defaultTTL  time.Duration
	randomRange int
	cacheStore  *cache.Cache
}

type Option[T any] func(*CursorCache[T])

func WithBloomFilter[T any](bf BloomFilter) Option[T] {
	return func(c *CursorCache[T]) {
		c.bloom = bf
	}
}

func WithTTL[T any](ttl time.Duration) Option[T] {
	return func(c *CursorCache[T]) {
		c.defaultTTL = ttl
	}
}

func WithRandomRange[T any](sec int) Option[T] {
	return func(c *CursorCache[T]) {
		c.randomRange = sec
	}
}

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

func (c *CursorCache[T]) Paginate(ctx context.Context, req PageRequest[T]) (*PageResponse[T], error) {
	cacheKey := c.getCacheKey(req.KeyPrefix, req.Cursor, req.Direction)

	// 布隆检查
	if c.shouldReturnEmpty(ctx, req.Cursor) {
		return &PageResponse[T]{Data: []T{}}, nil
	}

	var resp PageResponse[T]
	// 使用 cache.Get，内部会自动做锁、防击穿、single flight 等
	var fromCache = true
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

// 生成缓存键
func (c *CursorCache[T]) getCacheKey(prefix, cursor string, dir CursorDirection) string {
	return fmt.Sprintf("%s:%s:%s", prefix, cursor, string(dir))
}

// 布隆判断
func (c *CursorCache[T]) shouldReturnEmpty(ctx context.Context, cursor string) bool {
	if c.bloom == nil || cursor == "" {
		return false
	}
	exists, err := c.bloom.Exists(ctx, cursor)
	return err == nil && !exists
}

// 更新布隆
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

// 自定义 TTL 计算（cache.Fetch 形式签名）
func (c *CursorCache[T]) makeTTL(ctx context.Context, data interface{}) time.Duration {
	// data 类型是 *PageResponse[T]，但这里只根据 Data 长度做随机抖动
	resp, _ := data.(*PageResponse[T])
	dataLen := 0
	if resp != nil {
		dataLen = len(resp.Data)
	}
	if dataLen == 0 {
		// 空结果至少留一定最小时间
		if c.defaultTTL < DefaultMinEmptyTTL {
			return c.defaultTTL
		}
		return DefaultMinEmptyTTL
	}
	// 随机抖动
	jitter := time.Duration(rand.Intn(c.randomRange)) * time.Second
	return c.defaultTTL + jitter
}

// EncodeCursor / DecodeCursor
func EncodeCursor(c *ComplexCursor) string {
	if c == nil {
		return ""
	}
	b, _ := json.Marshal(c)
	return base64.StdEncoding.EncodeToString(b)
}

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
