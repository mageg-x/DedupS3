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
	"fmt"
	"github.com/mageg-x/boulder/internal/config"
	"sync"
	"time"
)

// Item 缓存项
type Item struct {
	Value interface{}
	TTL   time.Duration
}

var (
	cacheInst Cache
	mu        sync.Mutex
)

// Cache 接口
type Cache interface {
	Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error
	Get(ctx context.Context, key string) (interface{}, bool, error)
	Del(ctx context.Context, key string) error
	BatchSet(ctx context.Context, items map[string]Item) error
	BatchGet(ctx context.Context, keys []string) (map[string]interface{}, error)
	BatchDel(ctx context.Context, keys []string) error
	Clear(ctx context.Context) error
	Close() error
	Ping(ctx context.Context) error
	TTL(ctx context.Context, key string) (time.Duration, error)
	Exists(ctx context.Context, key string) (bool, error)
	Increment(ctx context.Context, key string, value int64) (int64, error)
	Decrement(ctx context.Context, key string, value int64) (int64, error)
	Keys(ctx context.Context, pattern string) ([]string, error)
}

func GetCache() Cache {
	mu.Lock()
	defer mu.Unlock()
	if cacheInst != nil {
		return cacheInst
	}

	cfg := config.Get()
	if cfg.Cache.Redis.
}

// NewCache 创建缓存实例
func NewCache(cacheType string) (Cache, error) {
	var cache Cache
	var err error

	switch cacheType {
	case "redis":
		cache, err = NewRedis()
	case "ristretto":
		cache, err = NewRistretto()
	default:
		return nil, fmt.Errorf("unsupported cache type: %s", cacheType)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create cache: %v", err)
	}

	return cache, nil
}
