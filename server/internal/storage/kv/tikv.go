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
package kv

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/mageg-x/boulder/internal/logger"

	xconf "github.com/mageg-x/boulder/internal/config"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/rawkv"
)

// TiKVStore 基于TiKV的KV存储实现
type TiKVStore struct {
	client *rawkv.Client
}

// InitTiKVStore 初始化TiKV存储
func InitTiKVStore(cfg *xconf.TiKVConfig) (*TiKVStore, error) {
	// 创建安全配置
	security := config.Security{
		// 根据需要配置TLS
	}

	client, err := rawkv.NewClient(context.TODO(), cfg.PDAddrs, security)
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to create TiKV client: %v", err)
		return nil, fmt.Errorf("failed to create TiKV client: %w", err)
	}

	logger.GetLogger("boulder").Infof("TiKV store initialized successfully")
	return &TiKVStore{client: client}, nil
}

// Put 存储键值对
func (t *TiKVStore) Put(ctx context.Context, key string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		logger.GetLogger("boulder").Errorf("JSON marshal error for key %s: %v", key, err)
		return fmt.Errorf("json marshal error: %w", err)
	}

	err = t.client.Put(ctx, []byte(key), data)
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to put key %s: %v", key, err)
	}
	return err
}

// Get 获取值并反序列化到指定结构
func (t *TiKVStore) Get(ctx context.Context, key string, value interface{}) (bool, error) {
	data, err := t.client.Get(ctx, []byte(key))
	if err != nil {
		logger.GetLogger("boulder").Errorf("Error getting key %s: %v", key, err)
		return false, err
	}

	if data == nil {
		logger.GetLogger("boulder").Debugf("Key not found: %s", key)
		return false, nil
	}

	if err := json.Unmarshal(data, value); err != nil {
		logger.GetLogger("boulder").Errorf("JSON unmarshal error for key %s: %v", key, err)
		return true, fmt.Errorf("json unmarshal error: %w", err)
	}
	logger.GetLogger("boulder").Debugf("Successfully got value for key: %s", key)
	return true, nil
}

// GetRaw 获取原始字节数据
func (t *TiKVStore) GetRaw(ctx context.Context, key string) ([]byte, bool, error) {
	data, err := t.client.Get(ctx, []byte(key))
	if err != nil {
		logger.GetLogger("boulder").Errorf("Error getting raw data for key %s: %v", key, err)
		return nil, false, err
	}

	if data == nil {
		logger.GetLogger("boulder").Debugf("Key not found: %s", key)
		return nil, false, nil
	}

	logger.GetLogger("boulder").Debugf("Successfully got raw data for key: %s", key)
	return data, true, nil
}

// Delete 删除键
func (t *TiKVStore) Delete(ctx context.Context, key string) error {
	err := t.client.Delete(ctx, []byte(key))
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to delete key %s: %v", key, err)
	} else {
		logger.GetLogger("boulder").Debugf("Successfully deleted key: %s", key)
	}
	return err
}

func (t *TiKVStore) DeletePrefix(ctx context.Context, prefix string) error {
	// 计算结束前缀：例如 user: -> user; （字节序上刚好大于 user: 的最小前缀）
	endPrefix := make([]byte, len(prefix))
	copy(endPrefix, prefix)
	endPrefix[len(endPrefix)-1]++ // 简单递增最后一个字节
	err := t.client.DeleteRange(ctx, []byte(prefix), endPrefix)
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to delete key %s: %v", prefix, err)
	} else {
		logger.GetLogger("boulder").Debugf("Successfully deleted key: %s", prefix)
	}
	return err
}

// Scan 扫描指定前缀的键
func (t *TiKVStore) Scan(ctx context.Context, prefix, startKey string, limit int) ([]string, string, error) {
	// 确定起始键
	start := []byte(startKey)
	if startKey == "" {
		start = []byte(prefix)
	}
	// 计算结束前缀：例如 user: -> user; （字节序上刚好大于 user: 的最小前缀）
	end := make([]byte, len(prefix))
	copy(end, prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xFF {
			end[i]++
			end = end[:i+1]
			break
		}
		// 如果字节是0xFF，继续向前寻找可以递增的字节
		if i == 0 {
			// 整个前缀都是0xFF，需要特殊处理
			end = []byte(append([]byte(prefix), 0x00))
		}
	}
	// 执行扫描
	ks, _, err := t.client.Scan(ctx, start, end, limit)
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to scan key-value pairs with prefix %s: %v", prefix, err)
		return nil, "", err
	} else {
		logger.GetLogger("boulder").Debugf("Successfully scanned key-value pairs with prefix %s: %d", prefix, len(ks))
	}

	// 转换为字符串键
	keys := make([]string, 0, len(ks))
	for _, key := range ks {
		keys = append(keys, string(key))
	}

	// 确定下一个起始键
	var nextKey string
	if len(ks) == limit && len(ks) > 0 {
		lastKey := ks[len(ks)-1]
		nextKey = string(lastKey)
	}

	return keys, nextKey, nil
}

// Close 关闭连接
func (t *TiKVStore) Close() error {
	err := t.client.Close()
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to close TiKV client: %v", err)
		return fmt.Errorf("failed to close TiKV client: %w", err)
	}

	logger.GetLogger("boulder").Debugf("TiKV store closed successfully")
	return nil
}
