package redisbloom

import (
	"context"
	"github.com/redis/go-redis/v9"
)

// RedisBloom 封装了对 RedisBloom 过滤器的操作
type RedisBloom struct {
	Client redis.UniversalClient // Redis 客户端
	Key    string                // Bloom 过滤器的 Redis 键名
}

// NewRedisBloom 创建一个新的 RedisBloom 实例
// rdb: Redis UniversalClient 实例
// key: Bloom 过滤器的 Redis 键名
func NewRedisBloom(rdb redis.UniversalClient, key string) *RedisBloom {
	return &RedisBloom{
		Client: rdb,
		Key:    key,
	}
}

// Init 初始化 Bloom 过滤器
// ctx: 上下文
// capacity: 预期插入元素数量
// errorRate: 允许的误判率
// 返回 error
func (rb *RedisBloom) Init(ctx context.Context, capacity int64, errorRate float64) error {
	return rb.Client.Do(ctx, "BF.RESERVE", rb.Key, errorRate, capacity).Err()
}

// Add 向 Bloom 过滤器添加一个元素
// ctx: 上下文
// key: 要添加的元素
func (rb *RedisBloom) Add(ctx context.Context, key string) {
	rb.Client.Do(ctx, "BF.ADD", rb.Key, key)
}

// Exists 检查元素是否存在于 Bloom 过滤器中
// ctx: 上下文
// key: 要检查的元素
// 返回值: 存在返回 true，不存在返回 false，error 表示执行过程中的错误
func (rb *RedisBloom) Exists(ctx context.Context, key string) (bool, error) {
	res, err := rb.Client.Do(ctx, "BF.EXISTS", rb.Key, key).Bool()
	if err != nil {
		return true, err
	}
	return res, nil
}
