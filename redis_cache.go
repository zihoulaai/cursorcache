package cursorcache

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// getCacheKey 生成用于 Redis 缓存的唯一键。
// prefix: 缓存前缀。
// cursor: 游标字符串。
// dir: 游标方向。
// 返回格式为 "prefix:cursor:dir" 的字符串。
func getCacheKey(prefix, cursor string, dir CursorDirection) string {
	return fmt.Sprintf("%s:%s:%s", prefix, cursor, dir)
}

// getCachedPage 从 Redis 获取缓存的分页数据并反序列化到 res。
// ctx: 上下文对象。
// rdb: Redis 客户端。
// key: Redis 缓存键。
// res: 用于存储结果的 PageResponse 指针。
// 若成功则将 res.FromCache 设为 true。
func getCachedPage[T any](ctx context.Context, rdb redis.UniversalClient, key string, res *PageResponse[T]) error {
	val, err := rdb.Get(ctx, key).Result()
	if err != nil {
		return err
	}

	if err := json.Unmarshal([]byte(val), res); err != nil {
		return err
	}
	res.FromCache = true
	return nil
}

// setCache 将分页数据序列化后存入 Redis 缓存。
// ctx: 上下文对象。
// rdb: Redis 客户端。
// key: Redis 缓存键。
// value: 需要缓存的 PageResponse 指针。
// ttl: 缓存过期时间。
func setCache[T any](ctx context.Context, rdb redis.UniversalClient, key string, value *PageResponse[T], ttl time.Duration) {
	data, _ := json.Marshal(value)
	_ = rdb.Set(ctx, key, data, ttl).Err()
}
