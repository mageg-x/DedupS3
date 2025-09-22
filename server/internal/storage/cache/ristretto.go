/*
 * Copyright (C) 2025-2025 raochaoxun <raochaoxun@gmail.com>.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package cache

import (
	"context"
	"errors"
	"github.com/mageg-x/boulder/internal/logger"
	"time"

	"github.com/dgraph-io/ristretto"
)

// Ristretto 基于Ristretto的缓存实现
type Ristretto struct {
	client *ristretto.Cache
	config *ristretto.Config
}

// NewRistretto 创建Ristretto缓存实例
func NewRistretto() (Cache, error) {
	// 创建Ristretto配置
	conf := &ristretto.Config{
		NumCounters: 1 << 26,
		MaxCost:     1 << 30,
		BufferItems: 64,
	}

	// 创建Ristretto缓存
	cache, err := ristretto.NewCache(conf)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to create ristretto cache: %v", err)
		return nil, err
	}

	logger.GetLogger("boulder").Infof("ristretto cache initialized successfully")
	return &Ristretto{
			client: cache,
			config: conf,
		},
		nil
}

// Set 设置键值对
func (r *Ristretto) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	if ctx.Err() != nil {
		logger.GetLogger("boulder").Errorf("context error when setting cache item %s: %v", key, ctx.Err())
		return ctx.Err()
	}

	// 对于Ristretto，我们将TTL转换为成本
	// 这里简单使用1作为成本
	cost := int64(1)

	// 设置缓存项
	ok := r.client.SetWithTTL(key, value, cost, ttl)
	if !ok {
		logger.GetLogger("boulder").Errorf("failed to set cache item: %s", key)
		return errors.New("failed to set cache item")
	}

	logger.GetLogger("boulder").Debugf("successfully set cache item: %s", key)
	return nil
}

// Get 获取缓存值
func (r *Ristretto) Get(ctx context.Context, key string) (interface{}, bool, error) {
	if ctx.Err() != nil {
		logger.GetLogger("boulder").Errorf("context error when getting cache item %s: %v", key, ctx.Err())
		return nil, false, ctx.Err()
	}

	value, found := r.client.Get(key)
	if found {
		logger.GetLogger("boulder").Debugf("successfully got cache item: %s", key)
	} else {
		logger.GetLogger("boulder").Infof("failed got cache item: %s", key)
		return value, found, nil
	}
	return value, found, nil
}

// Del 删除缓存
func (r *Ristretto) Del(ctx context.Context, key string) error {
	if ctx.Err() != nil {
		logger.GetLogger("boulder").Errorf("context error when deleting cache item %s: %v", key, ctx.Err())
		return ctx.Err()
	}

	r.client.Del(key)
	logger.GetLogger("boulder").Debugf("successfully deleted cache item: %s", key)
	return nil
}

// MSet 批量设置键值对
func (r *Ristretto) MSet(ctx context.Context, items map[string]Item) error {
	if ctx.Err() != nil {
		logger.GetLogger("boulder").Errorf("context error when batch setting cache items: %v", ctx.Err())
		return ctx.Err()
	}

	for key, item := range items {
		cost := int64(1)
		r.client.SetWithTTL(key, item.Value, cost, item.TTL)
		logger.GetLogger("boulder").Debugf("set cache item in batch: %s", key)
	}

	logger.GetLogger("boulder").Debugf("successfully batch set %d cache items", len(items))
	return nil
}

// MGet 批量获取缓存值
func (r *Ristretto) MGet(ctx context.Context, keys []string) (map[string]interface{}, error) {
	if ctx.Err() != nil {
		logger.GetLogger("boulder").Errorf("context error when batch getting cache items: %v", ctx.Err())
		return nil, ctx.Err()
	}

	result := make(map[string]interface{})

	for _, key := range keys {
		if value, found := r.client.Get(key); found {
			result[key] = value
			logger.GetLogger("boulder").Debugf("got cache item in batch: %s", key)
		} else {
			logger.GetLogger("boulder").Debugf("cache item not found in batch: %s", key)
		}
	}

	logger.GetLogger("boulder").Debugf("successfully batch got %d cache items", len(result))
	return result, nil
}

// MDel 批量删除缓存
func (r *Ristretto) MDel(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	if ctx.Err() != nil {
		logger.GetLogger("boulder").Errorf("context error when batch deleting cache items: %v", ctx.Err())
		return ctx.Err()
	}

	for _, key := range keys {
		r.client.Del(key)
		logger.GetLogger("boulder").Debugf("deleted cache item in batch: %s", key)
	}

	logger.GetLogger("boulder").Debugf("successfully batch deleted %d cache items", len(keys))
	return nil
}

// Exists 检查键是否存在
func (r *Ristretto) Exists(ctx context.Context, key string) (bool, error) {
	if ctx.Err() != nil {
		logger.GetLogger("boulder").Errorf("context error when checking existence of %s: %v", key, ctx.Err())
		return false, ctx.Err()
	}

	_, found := r.client.Get(key)
	logger.GetLogger("boulder").Debugf("cache item %s exists: %v", key, found)
	return found, nil
}

// Clear 清空缓存
func (r *Ristretto) Clear(ctx context.Context) error {
	if ctx.Err() != nil {
		logger.GetLogger("boulder").Errorf("context error when clearing cache: %v", ctx.Err())
		return ctx.Err()
	}

	// Ristretto不直接支持清空缓存
	// 这里我们创建一个新的缓存实例来替代
	newCache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: r.config.NumCounters,
		MaxCost:     r.config.MaxCost,
		BufferItems: r.config.BufferItems,
	})
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to create new cache during clear: %v", err)
		return err
	}

	r.client = newCache
	logger.GetLogger("boulder").Debugf("successfully cleared ristretto cache")
	return nil
}

// Close 关闭缓存连接
func (r *Ristretto) Close() error {
	// Ristretto不需要关闭
	logger.GetLogger("boulder").Debugf("ristretto cache connection closed")
	return nil
}
