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
	"encoding/json"
	"errors"
	"fmt"
	"github.com/mageg-x/boulder/internal/config"
	"time"

	"github.com/redis/go-redis/v9"
)

// Redis 基于 go-redis 的实现
type Redis struct {
	client *redis.ClusterClient
	config *config.RedisConfig
}

// NewRedis 创建 Redis/Valkey 客户端
func NewRedis(cfg *config.RedisConfig) (*Redis, error) {
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: cfg.Addrs,
		// 可选配置
		Password: cfg.Password,
		PoolSize: cfg.PoolSize,
	})

	// 可选：测试连接
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to redis: %v", err)
	}

	return &Redis{client: client}, nil
}

// Set 设置值
func (r *Redis) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	serialized, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to serialize value: %v", err)
	}

	var e *redis.StatusCmd
	if ttl > 0 {
		e = r.client.Set(ctx, key, serialized, ttl)
	} else {
		e = r.client.Set(ctx, key, serialized, 0)
	}

	if err := e.Err(); err != nil {
		return fmt.Errorf("failed to set key: %v", err)
	}

	return nil
}

// Get 获取值
func (r *Redis) Get(ctx context.Context, key string) (interface{}, bool, error) {
	val, err := r.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("failed to get key: %v", err)
	}

	var value interface{}
	if err := json.Unmarshal([]byte(val), &value); err != nil {
		return nil, false, fmt.Errorf("failed to deserialize value: %v", err)
	}

	return value, true, nil
}

// Del 删除
func (r *Redis) Del(ctx context.Context, key string) error {
	if err := r.client.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("failed to delete key: %v", err)
	}
	return nil
}

func (r *Redis) BatchSet(ctx context.Context, items map[string]Item) error {
	pipe := r.client.Pipeline()

	for key, item := range items {
		serialized, err := json.Marshal(item.Value)
		if err != nil {
			return fmt.Errorf("failed to serialize value for key %s: %v", key, err) // 删除 pipe.Close()
		}

		cmd := pipe.Set(ctx, key, serialized, item.TTL)
		if cmd.Err() != nil {
			return fmt.Errorf("pipeline set error for key %s: %v", key, cmd.Err())
		}
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to execute pipeline: %v", err)
	}

	return nil // ✅ 完美，无需 Close
}

// BatchGet 批量获取
func (r *Redis) BatchGet(ctx context.Context, keys []string) (map[string]interface{}, error) {
	if len(keys) == 0 {
		return map[string]interface{}{}, nil
	}

	vals, err := r.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("mget failed: %v", err)
	}

	values := make(map[string]interface{})
	for i, v := range vals {
		if v == nil {
			continue
		}
		// v 是 string 类型
		var value interface{}
		if err := json.Unmarshal([]byte(v.(string)), &value); err != nil {
			return nil, fmt.Errorf("unmarshal failed for key %s: %v", keys[i], err)
		}
		values[keys[i]] = value
	}

	return values, nil
}

// BatchDel 批量删除
func (r *Redis) BatchDel(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	if err := r.client.Del(ctx, keys...).Err(); err != nil {
		return fmt.Errorf("batch delete failed: %v", err)
	}
	return nil
}

// Clear 清空当前 DB
func (r *Redis) Clear(ctx context.Context) error {
	if err := r.client.FlushDB(ctx).Err(); err != nil {
		return fmt.Errorf("flushdb failed: %v", err)
	}
	return nil
}

// Close 关闭连接
func (r *Redis) Close() error {
	return r.client.Close()
}

// Ping 健康检查
func (r *Redis) Ping(ctx context.Context) error {
	return r.client.Ping(ctx).Err()
}

// TTL 获取剩余时间
func (r *Redis) TTL(ctx context.Context, key string) (time.Duration, error) {
	ttl, err := r.client.TTL(ctx, key).Result()
	if err != nil {
		return 0, err
	}
	if ttl == -1 {
		return 0, nil // 永不过期
	}
	if ttl == -2 {
		return 0, errors.New("key does not exist")
	}
	return ttl, nil
}

// Exists 检查是否存在
func (r *Redis) Exists(ctx context.Context, key string) (bool, error) {
	n, err := r.client.Exists(ctx, key).Result()
	return n > 0, err
}

// Increment 递增
func (r *Redis) Increment(ctx context.Context, key string, value int64) (int64, error) {
	if value == 1 {
		return r.client.Incr(ctx, key).Result()
	}
	return r.client.IncrBy(ctx, key, value).Result()
}

// Decrement 递减
func (r *Redis) Decrement(ctx context.Context, key string, value int64) (int64, error) {
	if value == 1 {
		return r.client.Decr(ctx, key).Result()
	}
	return r.client.DecrBy(ctx, key, value).Result()
}

// Keys 模糊匹配
func (r *Redis) Keys(ctx context.Context, pattern string) ([]string, error) {
	return r.client.Keys(ctx, pattern).Result()
}
