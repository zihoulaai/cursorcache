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

func shouldReturnEmptyByBloom[T any](ctx context.Context, req PageRequest[T]) bool {
	if req.Bloom != nil && req.Cursor != "" {
		exists, err := req.Bloom.Exists(ctx, req.Cursor)
		return err == nil && !exists
	}
	return false
}

func tryGetCache[T any](ctx context.Context, rdb redis.UniversalClient, key string) (*PageResponse[T], bool) {
	var cached PageResponse[T]
	if err := getCachedPage(ctx, rdb, key, &cached); err == nil {
		cached.FromCache = true
		return &cached, true
	}
	return nil, false
}

func obtainLock(ctx context.Context, rdb redis.UniversalClient, cacheKey string) (*redislock.Lock, error) {
	locker := redislock.New(rdb)
	return locker.Obtain(ctx, "lock:"+cacheKey, 3*time.Second, &redislock.Options{
		RetryStrategy: redislock.ExponentialBackoff(50*time.Millisecond, 5),
	})
}

func calcTTL(baseTTL time.Duration, dataLen, randomRange int) time.Duration {
	if dataLen == 0 {
		return min(baseTTL, 30*time.Second)
	}
	jitter := time.Duration(rand.Intn(randomRange)) * time.Second
	return baseTTL + jitter
}

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
