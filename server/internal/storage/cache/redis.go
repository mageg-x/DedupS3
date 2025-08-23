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
	"fmt"
	"github.com/mageg-x/boulder/internal/config"
	"github.com/mageg-x/boulder/internal/logger"
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
		logger.GetLogger("boulder").Errorf("Failed to connect to redis: %v", err)
		return nil, fmt.Errorf("failed to connect to redis: %v", err)
	}

	logger.GetLogger("boulder").Infof("Redis client initialized successfully")
	return &Redis{client: client}, nil
}

// Set 设置值
func (r *Redis) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	serialized, err := json.Marshal(value)
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to serialize value for key %s: %v", key, err)
		return fmt.Errorf("failed to serialize value: %v", err)
	}

	var e *redis.StatusCmd
	if ttl > 0 {
		e = r.client.Set(ctx, key, serialized, ttl)
	} else {
		e = r.client.Set(ctx, key, serialized, 0)
	}

	if err := e.Err(); err != nil {
		logger.GetLogger("boulder").Errorf("Failed to set key %s: %v", key, err)
		return fmt.Errorf("failed to set key: %v", err)
	}

	logger.GetLogger("boulder").Debugf("Successfully set key in Redis: %s", key)
	return nil
}

// Get 获取值
func (r *Redis) Get(ctx context.Context, key string) (interface{}, bool, error) {
	val, err := r.client.Get(ctx, key).Result()
	if err == redis.Nil {
		logger.GetLogger("boulder").Debugf("Key not found in Redis: %s", key)
		return nil, false, nil
	}
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to get key %s: %v", key, err)
		return nil, false, fmt.Errorf("failed to get key: %v", err)
	}

	var value interface{}
	if err := json.Unmarshal([]byte(val), &value); err != nil {
		logger.GetLogger("boulder").Errorf("Failed to deserialize value for key %s: %v", key, err)
		return nil, false, fmt.Errorf("failed to deserialize value: %v", err)
	}

	logger.GetLogger("boulder").Debugf("Successfully got key from Redis: %s", key)
	return value, true, nil
}

// Del 删除
func (r *Redis) Del(ctx context.Context, key string) error {
	if err := r.client.Del(ctx, key).Err(); err != nil {
		logger.GetLogger("boulder").Errorf("Failed to delete key %s: %v", key, err)
		return fmt.Errorf("failed to delete key: %v", err)
	}

	logger.GetLogger("boulder").Debugf("Successfully deleted key from Redis: %s", key)
	return nil
}

func (r *Redis) BatchSet(ctx context.Context, items map[string]Item) error {
	pipe := r.client.Pipeline()

	for key, item := range items {
		serialized, err := json.Marshal(item.Value)
		if err != nil {
			logger.GetLogger("boulder").Errorf("Failed to serialize value for key %s: %v", key, err)
			return fmt.Errorf("failed to serialize value for key %s: %v", key, err)
		}

		cmd := pipe.Set(ctx, key, serialized, item.TTL)
		if cmd.Err() != nil {
			logger.GetLogger("boulder").Errorf("Pipeline set error for key %s: %v", key, cmd.Err())
			return fmt.Errorf("pipeline set error for key %s: %v", key, cmd.Err())
		}
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to execute pipeline: %v", err)
		return fmt.Errorf("failed to execute pipeline: %v", err)
	}

	logger.GetLogger("boulder").Debugf("Successfully batch set %d items in Redis", len(items))
	return nil
}

// BatchGet 批量获取
func (r *Redis) BatchGet(ctx context.Context, keys []string) (map[string]interface{}, error) {
	if len(keys) == 0 {
		logger.GetLogger("boulder").Debugf("No keys provided for batch get")
		return map[string]interface{}{}, nil
	}

	vals, err := r.client.MGet(ctx, keys...).Result()
	if err != nil {
		logger.GetLogger("boulder").Errorf("MGET failed: %v", err)
		return nil, fmt.Errorf("mget failed: %v", err)
	}

	values := make(map[string]interface{})
	for i, v := range vals {
		if v == nil {
			logger.GetLogger("boulder").Debugf("Key not found in batch get: %s", keys[i])
			continue
		}
		// v 是 string 类型
		var value interface{}
		if err := json.Unmarshal([]byte(v.(string)), &value); err != nil {
			logger.GetLogger("boulder").Errorf("Unmarshal failed for key %s: %v", keys[i], err)
			return nil, fmt.Errorf("unmarshal failed for key %s: %v", keys[i], err)
		}
		values[keys[i]] = value
	}

	logger.GetLogger("boulder").Debugf("Successfully batch got %d keys from Redis", len(values))
	return values, nil
}

// BatchDel 批量删除
func (r *Redis) BatchDel(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		logger.GetLogger("boulder").Debugf("No keys provided for batch delete")
		return nil
	}
	if err := r.client.Del(ctx, keys...).Err(); err != nil {
		logger.GetLogger("boulder").Errorf("Batch delete failed: %v", err)
		return fmt.Errorf("batch delete failed: %v", err)
	}

	logger.GetLogger("boulder").Debugf("Successfully batch deleted %d keys from Redis", len(keys))
	return nil
}

// Exists 检查是否存在
func (r *Redis) Exists(ctx context.Context, key string) (bool, error) {
	n, err := r.client.Exists(ctx, key).Result()
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to check existence of key %s: %v", key, err)
	}

	logger.GetLogger("boulder").Debugf("Key %s exists: %v", key, n > 0)
	return n > 0, err
}

// Clear 清空当前 DB
func (r *Redis) Clear(ctx context.Context) error {
	if err := r.client.FlushDB(ctx).Err(); err != nil {
		logger.GetLogger("boulder").Errorf("FlushDB failed: %v", err)
		return fmt.Errorf("flushdb failed: %v", err)
	}

	logger.GetLogger("boulder").Debugf("Successfully cleared Redis database")
	return nil
}

// Close 关闭连接
func (r *Redis) Close() error {
	err := r.client.Close()
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to close Redis connection: %v", err)
	}

	logger.GetLogger("boulder").Debugf("Redis connection closed")
	return err
}
