// Package cache /*
package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dgraph-io/ristretto"
	"github.com/mageg-x/boulder/internal/config"
	"github.com/mageg-x/boulder/internal/logger"
	"time"
)

// RistrettoCache 实现基于Ristretto的内存缓存

type RistrettoCache struct {
	cache *ristretto.Cache
	ctx   context.Context
}

// NewRistrettoCache 创建Ristretto缓存
func NewRistrettoCache(c *config.CacheConfig) (*RistrettoCache, error) {
	logger.GetLogger("boulder").Infof("Creating Ristretto cache with max size: %d", c.MaxSize)
	if c == nil {
		logger.GetLogger("boulder").Errorf("NewRistrettoCache: nil config")
		return nil, fmt.Errorf("NewRistrettoCache: nil config")
	}

	// 创建Ristretto缓存
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: c.NumCounters, // 计数器数量
		MaxCost:     c.MaxSize,     // 最大字节数
		BufferItems: 64,            // 缓冲区大小
	})
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to create Ristretto cache: %v", err)
		return nil, fmt.Errorf("failed to create Ristretto cache: %w", err)
	}

	logger.GetLogger("boulder").Infof("Ristretto cache initialized successfully")
	return &RistrettoCache{
		cache: cache,
		ctx:   context.Background(),
	}, nil
}

// Type 返回缓存类型
func (r *RistrettoCache) Type() string {
	return "ristretto"
}

// Set 设置缓存
func (r *RistrettoCache) Set(key string, value interface{}, ttl time.Duration) error {
	logger.GetLogger("boulder").Debugf("Setting cache for key: %s, ttl: %v", key, ttl)
	// 序列化值
	bytes, err := json.Marshal(value)
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to marshal value for key %s: %v", key, err)
		return fmt.Errorf("failed to marshal value: %w", err)
	}

	// 计算值的成本（字节数）
	cost := int64(len(bytes))

	// 设置缓存
	ok := r.cache.SetWithTTL(key, bytes, cost, ttl)
	if !ok {
		logger.GetLogger("boulder").Errorf("Failed to set cache for key %s: cache is full or invalid", key)
		return fmt.Errorf("failed to set cache: cache is full or invalid")
	}

	logger.GetLogger("boulder").Debugf("Cache set successfully for key: %s", key)
	return nil
}

// Get 获取缓存
func (r *RistrettoCache) Get(key string, value interface{}) (bool, error) {
	logger.GetLogger("boulder").Debugf("Getting cache for key: %s", key)
	// 获取缓存
	val, found := r.cache.Get(key)
	if !found {
		logger.GetLogger("boulder").Debugf("Cache miss for key: %s", key)
		return false, nil
	}

	// 断言为[]byte类型
	bytes, ok := val.([]byte)
	if !ok {
		logger.GetLogger("boulder").Errorf("Invalid type for cached value of key %s", key)
		return false, fmt.Errorf("invalid type for cached value")
	}

	// 反序列化值
	err := json.Unmarshal(bytes, value)
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to unmarshal value for key %s: %v", key, err)
		return false, fmt.Errorf("failed to unmarshal value: %w", err)
	}

	logger.GetLogger("boulder").Debugf("Cache hit for key: %s", key)
	return true, nil
}

// Del 删除缓存
func (r *RistrettoCache) Del(key string) error {
	logger.GetLogger("boulder").Debugf("Deleting cache for key: %s", key)
	r.cache.Del(key)
	logger.GetLogger("boulder").Debugf("Cache deleted successfully for key: %s", key)
	return nil
}

// BatchSet 批量设置缓存
func (r *RistrettoCache) BatchSet(items map[string]interface{}, ttl time.Duration) error {
	logger.GetLogger("boulder").Debugf("Batch setting %d cache items with ttl: %v", len(items), ttl)
	for key, value := range items {
		bytes, err := json.Marshal(value)
		if err != nil {
			logger.GetLogger("boulder").Errorf("Failed to marshal value for key %s in batch set: %v", key, err)
			return fmt.Errorf("failed to marshal value for key %s: %w", key, err)
		}

		cost := int64(len(bytes))
		ok := r.cache.SetWithTTL(key, bytes, cost, ttl)
		if !ok {
			logger.GetLogger("boulder").Errorf("Failed to set cache for key %s in batch set: cache is full or invalid", key)
			return fmt.Errorf("failed to set cache for key %s: cache is full or invalid", key)
		}
	}

	logger.GetLogger("boulder").Debugf("Batch set %d cache items successfully", len(items))
	return nil
}

// BatchDel 批量删除缓存
func (r *RistrettoCache) BatchDel(keys []string) error {
	logger.GetLogger("boulder").Debugf("Batch deleting %d cache keys", len(keys))
	for _, key := range keys {
		r.cache.Del(key)
	}
	logger.GetLogger("boulder").Debugf("Batch deleted %d cache keys successfully", len(keys))
	return nil
}

// Keys 获取所有匹配pattern的键
func (r *RistrettoCache) Keys(pattern string) ([]string, error) {
	logger.GetLogger("boulder").Errorf("Keys method not supported by Ristretto cache")
	return nil, errors.New("Keys method not supported by Ristretto cache")
}

// Clear 清理所有缓存
func (r *RistrettoCache) Clear() error {
	logger.GetLogger("boulder").Debugf("Clearing all cache")
	r.cache.Clear()
	logger.GetLogger("boulder").Debugf("Cache cleared successfully")
	return nil
}

// Close 关闭缓存
func (r *RistrettoCache) Close() error {
	logger.GetLogger("boulder").Infof("Closing Ristretto cache")
	// Ristretto doesn't have a Close method
	logger.GetLogger("boulder").Infof("Ristretto cache closed successfully")
	return nil
}
