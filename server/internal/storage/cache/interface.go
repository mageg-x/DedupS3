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
	MSet(ctx context.Context, items map[string]Item) error
	MGet(ctx context.Context, keys []string) (map[string]interface{}, error)
	MDel(ctx context.Context, keys []string) error
	Exists(ctx context.Context, key string) (bool, error)
	Clear(ctx context.Context) error
	Close() error
}

func GetCache() (Cache, error) {
	mu.Lock()
	defer mu.Unlock()
	if cacheInst != nil {
		return cacheInst, nil
	}

	cfg := config.Get()
	var err error
	if cfg.Cache.Redis == nil {
		cacheInst, err = NewRistretto()
	} else {
		cacheInst, err = NewRedis(cfg.Cache.Redis)
	}
	return cacheInst, err
}
