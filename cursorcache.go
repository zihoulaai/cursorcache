package cursorcache

import (
	"context"
	"errors"
	"time"

	"golang.org/x/exp/rand"
	"golang.org/x/exp/slices"

	"github.com/bsm/redislock"
	"github.com/redis/go-redis/v9"
)

// PaginateWithCache 实现带缓存的游标分页查询。
// 1. 通过布隆过滤器快速判断是否需要返回空数据。
// 2. 查询缓存，若命中直接返回。
// 3. 获取分布式锁防止缓存击穿。
// 4. 再次检查缓存，防止并发穿透。
// 5. 拉取数据，处理游标方向。
// 6. 写入缓存，设置过期时间。
// 7. 更新布隆过滤器。
// ctx: 上下文。
// rdb: Redis 客户端。
// req: 分页请求参数。
// 返回分页响应和错误信息。
func PaginateWithCache[T any](ctx context.Context, rdb redis.UniversalClient, req PageRequest[T]) (*PageResponse[T], error) {
	cacheKey := getCacheKey(req.KeyPrefix, req.Cursor, req.Direction)

	// 1. 布隆过滤器快速判断
	if shouldReturnEmptyByBloom(ctx, req) {
		return &PageResponse[T]{Data: []T{}}, nil
	}

	// 2. 查询缓存
	if cached, ok := tryGetCache[T](ctx, rdb, cacheKey); ok {
		// 如果缓存存在，直接返回
		cached.FromCache = true
		return cached, nil
	}

	// 3. 分布式锁防击穿
	lock, err := obtainLock(ctx, rdb, cacheKey)
	if err != nil {
		if errors.Is(err, redislock.ErrNotObtained) {
			time.Sleep(50 * time.Millisecond)
			return PaginateWithCache[T](ctx, rdb, req)
		}
		return nil, err
	}
	defer lock.Release(ctx)

	// 4. 二次检查缓存
	if cached, ok := tryGetCache[T](ctx, rdb, cacheKey); ok {
		// 如果缓存存在，直接返回
		cached.FromCache = true
		return cached, nil
	}

	// 5. 拉取数据
	data, next, prev, err := req.Fetcher(ctx, req.Cursor, req.PageSize, req.Direction)
	if err != nil {
		return nil, err
	}
	if req.Direction == PrevPage {
		slices.Reverse(data)
	}

	resp := &PageResponse[T]{
		Data:       data,
		NextCursor: next,
		PrevCursor: prev,
		FromCache:  false,
	}

	// 6. 缓存写入
	ttl := calcTTL(req.TTL, len(data), req.RandomRange)
	setCache(ctx, rdb, cacheKey, resp, ttl)

	// 7. 更新布隆过滤器
	updateBloom(ctx, req, data, next, prev)

	return resp, nil
}

// --- 辅助函数 ---

// shouldReturnEmptyByBloom 判断布隆过滤器是否可以直接返回空数据。
// ctx: 上下文。
// req: 分页请求参数。
// 返回 true 表示可直接返回空数据。
func shouldReturnEmptyByBloom[T any](ctx context.Context, req PageRequest[T]) bool {
	if req.Bloom != nil && req.Cursor != "" {
		exists, err := req.Bloom.Exists(ctx, req.Cursor)
		return err == nil && !exists
	}
	return false
}

// tryGetCache 尝试从缓存获取分页数据。
// ctx: 上下文。
// rdb: Redis 客户端。
// key: 缓存键。
// 返回缓存数据和是否命中缓存。
func tryGetCache[T any](ctx context.Context, rdb redis.UniversalClient, key string) (*PageResponse[T], bool) {
	var cached PageResponse[T]
	if err := getCachedPage(ctx, rdb, key, &cached); err == nil {
		cached.FromCache = true
		return &cached, true
	}
	return nil, false
}

// obtainLock 获取分布式锁，防止缓存击穿。
// ctx: 上下文。
// rdb: Redis 客户端。
// cacheKey: 缓存键。
// 返回锁对象和错误信息。
func obtainLock(ctx context.Context, rdb redis.UniversalClient, cacheKey string) (*redislock.Lock, error) {
	locker := redislock.New(rdb)
	return locker.Obtain(ctx, "lock:"+cacheKey, 3*time.Second, &redislock.Options{
		RetryStrategy: redislock.ExponentialBackoff(50*time.Millisecond, 5),
	})
}

// calcTTL 计算缓存的过期时间，支持抖动。
// baseTTL: 基础过期时间。
// dataLen: 数据长度。
// randomRange: 抖动范围（秒）。
// 返回最终过期时间。
func calcTTL(baseTTL time.Duration, dataLen, randomRange int) time.Duration {
	if dataLen == 0 {
		return min(baseTTL, 30*time.Second)
	}
	jitter := time.Duration(rand.Intn(randomRange)) * time.Second
	return baseTTL + jitter
}

// updateBloom 更新布隆过滤器，添加当前游标及前后游标。
// ctx: 上下文。
// req: 分页请求参数。
// data: 当前页数据。
// next: 下一页游标。
// prev: 上一页游标。
func updateBloom[T any](ctx context.Context, req PageRequest[T], data []T, next, prev string) {
	if req.Bloom != nil && len(data) > 0 {
		req.Bloom.Add(ctx, req.Cursor)
		if prev != "" {
			req.Bloom.Add(ctx, prev)
		}
		if next != "" {
			req.Bloom.Add(ctx, next)
		}
	}
}
