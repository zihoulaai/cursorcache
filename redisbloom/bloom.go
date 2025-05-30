package redisbloom

import (
	"context"
	"github.com/redis/go-redis/v9"
)

type RedisBloom struct {
	Client redis.UniversalClient
	Key    string
}

func NewRedisBloom(rdb redis.UniversalClient, key string) *RedisBloom {
	return &RedisBloom{
		Client: rdb,
		Key:    key,
	}
}

func (rb *RedisBloom) Init(ctx context.Context, capacity int64, errorRate float64) error {
	return rb.Client.Do(ctx, "BF.RESERVE", rb.Key, errorRate, capacity).Err()
}

func (rb *RedisBloom) Add(ctx context.Context, key string) {
	rb.Client.Do(ctx, "BF.ADD", rb.Key, key)
}

func (rb *RedisBloom) Exists(ctx context.Context, key string) (bool, error) {
	res, err := rb.Client.Do(ctx, "BF.EXISTS", rb.Key, key).Bool()
	if err != nil {
		return true, err
	}
	return res, nil
}
