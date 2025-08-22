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
		return nil, err
	}

	return &Ristretto{
			client: cache,
			config: conf,
		},
		nil
}

// Set 设置键值对
func (r *Ristretto) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// 对于Ristretto，我们将TTL转换为成本
	// 这里简单使用1作为成本
	cost := int64(1)

	// 设置缓存项
	ok := r.client.SetWithTTL(key, value, cost, ttl)
	if !ok {
		return errors.New("failed to set cache item")
	}

	return nil
}

// Get 获取缓存值
func (r *Ristretto) Get(ctx context.Context, key string) (interface{}, bool, error) {
	if ctx.Err() != nil {
		return nil, false, ctx.Err()
	}

	value, found := r.client.Get(key)
	return value, found, nil
}

// Del 删除缓存
func (r *Ristretto) Del(ctx context.Context, key string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	r.client.Del(key)
	return nil
}

// BatchSet 批量设置键值对
func (r *Ristretto) BatchSet(ctx context.Context, items map[string]Item) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	for key, item := range items {
		cost := int64(1)
		r.client.SetWithTTL(key, item.Value, cost, item.TTL)
	}

	return nil
}

// BatchGet 批量获取缓存值
func (r *Ristretto) BatchGet(ctx context.Context, keys []string) (map[string]interface{}, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	result := make(map[string]interface{})

	for _, key := range keys {
		if value, found := r.client.Get(key); found {
			result[key] = value
		}
	}

	return result, nil
}

// BatchDel 批量删除缓存
func (r *Ristretto) BatchDel(ctx context.Context, keys []string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	for _, key := range keys {
		r.client.Del(key)
	}

	return nil
}

// Clear 清空缓存
func (r *Ristretto) Clear(ctx context.Context) error {
	if ctx.Err() != nil {
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
		return err
	}

	r.client = newCache
	return nil
}

// Close 关闭缓存连接
func (r *Ristretto) Close() error {
	// Ristretto不需要关闭
	return nil
}

// Ping 检查缓存连接
func (r *Ristretto) Ping(ctx context.Context) error {
	// Ristretto是内存缓存，不需要Ping
	return nil
}

// TTL 获取键的过期时间
func (r *Ristretto) TTL(ctx context.Context, key string) (time.Duration, error) {
	// Ristretto不支持直接获取TTL
	return 0, errors.New("ristretto does not support TTL retrieval")
}

// Exists 检查键是否存在
func (r *Ristretto) Exists(ctx context.Context, key string) (bool, error) {
	if ctx.Err() != nil {
		return false, ctx.Err()
	}

	_, found := r.client.Get(key)
	return found, nil
}

// Increment 增加键的值
func (r *Ristretto) Increment(ctx context.Context, key string, value int64) (int64, error) {
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}

	// Ristretto不直接支持原子增量
	current, found := r.client.Get(key)
	if !found {
		current = int64(0)
	}

	currentInt, ok := current.(int64)
	if !ok {
		return 0, errors.New("value is not an int64")
	}

	newValue := currentInt + value
	r.client.Set(key, newValue, 1)
	return newValue, nil
}

// Decrement 减少键的值
func (r *Ristretto) Decrement(ctx context.Context, key string, value int64) (int64, error) {
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}

	// Ristretto不直接支持原子减量
	current, found := r.client.Get(key)
	if !found {
		current = int64(0)
	}

	currentInt, ok := current.(int64)
	if !ok {
		return 0, errors.New("value is not an int64")
	}

	newValue := currentInt - value
	r.client.Set(key, newValue, 1)
	return newValue, nil
}

// Keys 获取匹配模式的所有键
func (r *Ristretto) Keys(ctx context.Context, pattern string) ([]string, error) {
	// Ristretto不支持键模式匹配
	return nil, errors.New("ristretto does not support key pattern matching")
}
