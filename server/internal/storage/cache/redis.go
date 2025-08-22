// Package cache /*
package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/mageg-x/boulder/internal/config"
	"github.com/mageg-x/boulder/internal/logger"
	"time"

	"github.com/go-redis/redis/v8"
)

// RedisCache 实现基于Redis的缓存

type RedisCache struct {
	client *redis.Client
	ctx    context.Context
}

// NewRedisCache 创建Redis缓存
func NewRedisCache(c *config.CacheConfig) (*RedisCache, error) {
	logger.GetLogger("boulder").Infof("Creating Redis cache with address: %s", c.Address)
	if c == nil {
		logger.GetLogger("boulder").Errorf("NewRedisCache: nil config")
		return nil, fmt.Errorf("NewRedisCache: nil config")
	}

	ctx := context.Background()

	// 创建Redis客户端
	client := redis.NewClient(&redis.Options{
		Addr:     c.Address,
		Password: c.Password,
		DB:       c.DB,
	})

	// 测试连接
	_, err := client.Ping(ctx).Result()
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to connect to Redis: %v", err)
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	logger.GetLogger("boulder").Infof("Redis cache initialized successfully")
	return &RedisCache{
		client: client,
		ctx:    ctx,
	}, nil
}

// Type 返回缓存类型
func (r *RedisCache) Type() string {
	return "redis"
}

// Set 设置缓存
func (r *RedisCache) Set(key string, value interface{}, ttl time.Duration) error {
	logger.GetLogger("boulder").Debugf("Setting cache for key: %s, ttl: %v", key, ttl)
	// 序列化值
	bytes, err := json.Marshal(value)
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to marshal value for key %s: %v", key, err)
		return fmt.Errorf("failed to marshal value: %w", err)
	}

	// 设置缓存
	err = r.client.Set(r.ctx, key, bytes, ttl).Err()
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to set cache for key %s: %v", key, err)
		return fmt.Errorf("failed to set cache: %w", err)
	}

	logger.GetLogger("boulder").Debugf("Cache set successfully for key: %s", key)
	return nil
}

// Get 获取缓存
func (r *RedisCache) Get(key string, value interface{}) (bool, error) {
	logger.GetLogger("boulder").Debugf("Getting cache for key: %s", key)
	// 获取缓存
	bytes, err := r.client.Get(r.ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			logger.GetLogger("boulder").Debugf("Cache miss for key: %s", key)
			return false, nil
		}
		logger.GetLogger("boulder").Errorf("Failed to get cache for key %s: %v", key, err)
		return false, fmt.Errorf("failed to get cache: %w", err)
	}

	// 反序列化值
	err = json.Unmarshal(bytes, value)
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to unmarshal value for key %s: %v", key, err)
		return false, fmt.Errorf("failed to unmarshal value: %w", err)
	}

	logger.GetLogger("boulder").Debugf("Cache hit for key: %s", key)
	return true, nil
}

// Del 删除缓存
func (r *RedisCache) Del(key string) error {
	logger.GetLogger("boulder").Debugf("Deleting cache for key: %s", key)
	err := r.client.Del(r.ctx, key).Err()
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to delete cache for key %s: %v", key, err)
		return fmt.Errorf("failed to delete cache: %w", err)
	}
	logger.GetLogger("boulder").Debugf("Cache deleted successfully for key: %s", key)
	return nil
}

// BatchSet 批量设置缓存
func (r *RedisCache) BatchSet(items map[string]interface{}, ttl time.Duration) error {
	logger.GetLogger("boulder").Debugf("Batch setting %d cache items with ttl: %v", len(items), ttl)
	pipe := r.client.Pipeline()

	for key, value := range items {
		bytes, err := json.Marshal(value)
		if err != nil {
			logger.GetLogger("boulder").Errorf("Failed to marshal value for key %s in batch set: %v", key, err)
			return fmt.Errorf("failed to marshal value for key %s: %w", key, err)
		}

		pipe.Set(r.ctx, key, bytes, ttl)
	}

	_, err := pipe.Exec(r.ctx)
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to execute batch set: %v", err)
		return fmt.Errorf("failed to execute batch set: %w", err)
	}

	logger.GetLogger("boulder").Debugf("Batch set %d cache items successfully", len(items))
	return nil
}

// BatchDel 批量删除缓存
func (r *RedisCache) BatchDel(keys []string) error {
	logger.GetLogger("boulder").Debugf("Batch deleting %d cache keys", len(keys))
	if len(keys) == 0 {
		logger.GetLogger("boulder").Debugf("No keys to delete in batch operation")
		return nil
	}

	err := r.client.Del(r.ctx, keys...).Err()
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to batch delete cache keys: %v", err)
		return fmt.Errorf("failed to batch delete cache keys: %w", err)
	}

	logger.GetLogger("boulder").Debugf("Batch deleted %d cache keys successfully", len(keys))
	return nil
}

// Keys 获取所有匹配pattern的键
func (r *RedisCache) Keys(pattern string) ([]string, error) {
	logger.GetLogger("boulder").Debugf("Getting keys with pattern: %s", pattern)
	keys, err := r.client.Keys(r.ctx, pattern).Result()
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to get keys with pattern %s: %v", pattern, err)
		return nil, fmt.Errorf("failed to get keys: %w", err)
	}
	logger.GetLogger("boulder").Debugf("Found %d keys matching pattern: %s", len(keys), pattern)
	return keys, nil
}

// Clear 清理所有缓存
func (r *RedisCache) Clear() error {
	logger.GetLogger("boulder").Debugf("Clearing all cache")
	keys, err := r.client.Keys(r.ctx, "*").Result()
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to get all keys for clearing: %v", err)
		return fmt.Errorf("failed to get all keys: %w", err)
	}

	if len(keys) == 0 {
		logger.GetLogger("boulder").Debugf("No cache keys to clear")
		return nil
	}

	err = r.client.Del(r.ctx, keys...).Err()
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to clear cache: %v", err)
		return fmt.Errorf("failed to clear cache: %w", err)
	}

	logger.GetLogger("boulder").Debugf("Cleared %d cache keys successfully", len(keys))
	return nil
}

// Close 关闭Redis连接
func (r *RedisCache) Close() error {
	logger.GetLogger("boulder").Infof("Closing Redis cache connection")
	err := r.client.Close()
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to close Redis connection: %v", err)
		return fmt.Errorf("failed to close Redis connection: %w", err)
	}
	logger.GetLogger("boulder").Infof("Redis connection closed successfully")
	return nil
}
