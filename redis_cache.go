package cursorcache

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

func getCacheKey(prefix, cursor string, dir CursorDirection) string {
	return fmt.Sprintf("%s:%s:%s", prefix, cursor, dir)
}

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

func setCache[T any](ctx context.Context, rdb redis.UniversalClient, key string, value *PageResponse[T], ttl time.Duration) {
	data, _ := json.Marshal(value)
	_ = rdb.Set(ctx, key, data, ttl).Err()
}
