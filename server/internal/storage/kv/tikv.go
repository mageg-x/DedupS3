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

	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/rawkv"
)

// TiKVStore 基于TiKV的KV存储实现
type TiKVStore struct {
	client *rawkv.Client
}

// InitTiKVStore 初始化TiKV存储
func InitTiKVStore(cfg TiKVConfig) (*TiKVStore, error) {
	// 创建安全配置
	security := config.Security{
		// 根据需要配置TLS
	}

	client, err := rawkv.NewClient(context.TODO(), cfg.PDAddrs, security)
	if err != nil {
		return nil, fmt.Errorf("failed to create TiKV client: %w", err)
	}

	return &TiKVStore{client: client}, nil
}

// Put 存储键值对
func (t *TiKVStore) Put(ctx context.Context, key string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("json marshal error: %w", err)
	}

	return t.client.Put(ctx, []byte(key), data)
}

// Get 获取值并反序列化到指定结构
func (t *TiKVStore) Get(ctx context.Context, key string, value interface{}) (bool, error) {
	data, err := t.client.Get(ctx, []byte(key))
	if err != nil {
		return false, err
	}

	if data == nil {
		return false, nil
	}

	if err := json.Unmarshal(data, value); err != nil {
		return true, fmt.Errorf("json unmarshal error: %w", err)
	}
	return true, nil
}

// GetRaw 获取原始字节数据
func (t *TiKVStore) GetRaw(ctx context.Context, key string) ([]byte, bool, error) {
	data, err := t.client.Get(ctx, []byte(key))
	if err != nil {
		return nil, false, err
	}

	if data == nil {
		return nil, false, nil
	}

	return data, true, nil
}

// Delete 删除键
func (t *TiKVStore) Delete(ctx context.Context, key string) error {
	return t.client.Delete(ctx, []byte(key))
}

// Scan 扫描指定前缀的键
func (t *TiKVStore) Scan(ctx context.Context, prefix string) ([]string, string, error) {
	return t.scanInternal(ctx, prefix, "", 0)
}

// ScanWithValues 扫描指定前缀的键值对
func (t *TiKVStore) ScanWithValues(ctx context.Context, prefix string) (map[string][]byte, error) {
	result := make(map[string][]byte)

	startKey := []byte(prefix)
	endKey := []byte(prefix + "\xFF") // 前缀结束标记

	keys, values, err := t.client.Scan(ctx, startKey, endKey, 1000)
	if err != nil {
		return nil, err
	}

	for i, key := range keys {
		result[string(key)] = values[i]
	}

	return result, nil
}

// ScanPage 分页扫描键
func (t *TiKVStore) ScanPage(ctx context.Context, prefix, startKey string, limit int) ([]string, string, error) {
	return t.scanInternal(ctx, prefix, startKey, limit)
}

// scanInternal 内部扫描实现
func (t *TiKVStore) scanInternal(ctx context.Context, prefix, startKey string, limit int) ([]string, string, error) {
	var keys []string
	var nextKey string

	// 确定起始键
	start := []byte(prefix)
	if startKey != "" {
		start = []byte(startKey)
	}

	// 确定结束键
	end := []byte(prefix + "\xFF") // 前缀结束标记

	// 计算实际扫描大小
	scanSize := 100
	if limit > 0 {
		scanSize = limit
	}

	scannedKeys, _, err := t.client.Scan(ctx, start, end, scanSize)
	if err != nil {
		return nil, "", err
	}

	// 转换为字符串键
	for _, key := range scannedKeys {
		keys = append(keys, string(key))
	}

	// 如果扫描结果达到限制大小，设置下一个起始键
	if len(scannedKeys) == scanSize && len(scannedKeys) > 0 {
		lastKey := scannedKeys[len(scannedKeys)-1]
		nextKey = string(lastKey)
	}

	return keys, nextKey, nil
}

// Close 关闭连接
func (t *TiKVStore) Close() error {
	return t.client.Close()
}

// BeginTxn 开始一个新事务
func (t *TiKVStore) BeginTxn(ctx context.Context) (Txn, error) {
	// 对于TiKV，我们需要使用事务客户端而不是原始客户端
	// 这里创建一个简单的事务封装
	return &TiKVTxn{
		client: t.client,
		ctx:    ctx,
		writes: make([]writeOperation, 0),
	}, nil
}

// TiKVTxn 基于TiKV的事务实现
type TiKVTxn struct {
	client *rawkv.Client
	ctx    context.Context
	writes []writeOperation
}

// writeOperation 表示一个写操作
type writeOperation struct {
	key   string
	value []byte
	op    string // "put" 或 "delete"
}

// Put 在事务中存储键值对
func (t *TiKVTxn) Put(key string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("json marshal error: %w", err)
	}

	t.writes = append(t.writes, writeOperation{
		key:   key,
		value: data,
		op:    "put",
	})
	return nil
}

// Get 在事务中获取值
func (t *TiKVTxn) Get(key string, value interface{}) (bool, error) {
	data, exists, err := t.GetRaw(key)
	if err != nil || !exists {
		return exists, err
	}

	if err := json.Unmarshal(data, value); err != nil {
		return true, fmt.Errorf("json unmarshal error: %w", err)
	}
	return true, nil
}

// GetRaw 在事务中获取原始字节数据
func (t *TiKVTxn) GetRaw(key string) ([]byte, bool, error) {
	data, err := t.client.Get(t.ctx, []byte(key))
	if err != nil {
		return nil, false, err
	}

	if data == nil {
		return nil, false, nil
	}

	return data, true, nil
}

// Delete 在事务中删除键
func (t *TiKVTxn) Delete(key string) error {
	t.writes = append(t.writes, writeOperation{
		key: key,
		op:  "delete",
	})
	return nil
}

// Commit 提交事务
func (t *TiKVTxn) Commit(ctx context.Context) error {
	// 对于rawkv客户端，我们只能模拟事务，这里使用简单的批量操作
	// 注意：这不是真正的原子事务，只是批量操作
	for _, op := range t.writes {
		if op.op == "put" {
			if err := t.client.Put(ctx, []byte(op.key), op.value); err != nil {
				return err
			}
		} else if op.op == "delete" {
			if err := t.client.Delete(ctx, []byte(op.key)); err != nil {
				return err
			}
		}
	}
	return nil
}

// Rollback 回滚事务
func (t *TiKVTxn) Rollback(_ context.Context) error {
	// 对于rawkv客户端，我们只需清除未提交的写操作
	t.writes = nil
	return nil
}
