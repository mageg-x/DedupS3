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
	"errors"
	"fmt"

	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"

	xconf "github.com/mageg-x/boulder/internal/config"
	"github.com/mageg-x/boulder/internal/logger"
)

// TiKVStore 基于TiKV的KV存储实现
type TiKVStore struct {
	client *tikv.KVStore
}

// TiKVTxn 基于TiKV的事务实现
type TiKVTxn struct {
	txn *tikv.KVTxn
}

// InitTiKVStore 初始化TiKV存储
func InitTiKVStore(cfg *xconf.TiKVConfig) (*TiKVStore, error) {
	// 创建安全配置
	security := pd.SecurityOption{}

	pdCli, err := pd.NewClient(cfg.PDAddrs, security)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to initialize TiKV pd client: %v", err)
		return nil, err
	}
	client, err := tikv.NewKVStore("tikv", pdCli, nil, nil)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to initialize TiKV  client: %v", err)
		return nil, err
	}
	return &TiKVStore{client: client}, nil
}

// Get 非事务版
func (t *TiKVStore) Get(key string, value interface{}) (bool, error) {
	// 直接使用 TiKV 快照 Get 方法进行快照读取
	snapshot := t.client.GetSnapshot(0)
	if snapshot == nil {
		logger.GetLogger("boulder").Errorf("failed to get snapshot for key: %s", key)
		return false, fmt.Errorf("failed to get snapshot for key: %s", key)
	}

	data, err := snapshot.Get(context.Background(), []byte(key))
	if err != nil {
		if errors.Is(err, tikverr.ErrNotExist) {
			return false, nil
		}
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

func (t *TiKVStore) GetRaw(key string) ([]byte, bool, error) {
	// 直接使用 TiKV 快照 Get 方法进行快照读取
	snapshot := t.client.GetSnapshot(0)
	if snapshot == nil {
		logger.GetLogger("boulder").Errorf("failed to get snapshot for key: %s", key)
		return nil, false, fmt.Errorf("failed to get snapshot for key: %s", key)
	}

	data, err := snapshot.Get(context.Background(), []byte(key))
	if err != nil {
		if errors.Is(err, tikverr.ErrNotExist) {
			return nil, false, nil
		}
		logger.GetLogger("boulder").Errorf("Error getting key %s: %v", key, err)
		return nil, false, err
	}

	return data, true, nil
}

func (t *TiKVStore) BatchGet(keys []string) (map[string][]byte, error) {
	// 直接使用 TiKV 快照 Get 方法进行快照读取
	snapshot := t.client.GetSnapshot(0)
	if snapshot == nil {
		logger.GetLogger("boulder").Errorf("failed to get snapshot for  batch get key")
		return nil, fmt.Errorf("failed to get snapshot for batch get key")
	}
	ks := make([][]byte, 0, len(keys))
	for _, key := range keys {
		ks = append(ks, []byte(key))
	}
	values, err := snapshot.BatchGet(context.Background(), ks)
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to BatchGet keys: %v", err)
		return nil, fmt.Errorf("failed to BatchGet keys: %w", err)
	}

	return values, nil
}

func (t *TiKVStore) Set(key string, value interface{}) error {
	txn, err := t.BeginTxn(context.Background(), nil)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to begin transaction: %v", err)
		return err
	}
	defer txn.Rollback()
	err = txn.Set(key, value)
	if err == nil {
		logger.GetLogger("boulder").Infof("Successfully set value for key: %s", key)
		err = txn.Commit()
		if err != nil {
			logger.GetLogger("boulder").Errorf("failed to commit transaction: %v", err)
		}
	}
	return err
}

// BeginTxn 开始一个新事务
func (t *TiKVStore) BeginTxn(_ context.Context, _ *TxnOpt) (Txn, error) {
	txn, err := t.client.Begin()
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to begin txn: %v", err)
		return nil, err
	}
	return &TiKVTxn{txn: txn}, nil
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

func (t *TiKVTxn) Commit() error {
	err := t.txn.Commit(context.Background())
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to commit txn: %v", err)
		return fmt.Errorf("failed to commit txn: %w", err)
	}
	logger.GetLogger("boulder").Debugf("Committed txn")
	return nil
}

func (t *TiKVTxn) Rollback() error {
	err := t.txn.Rollback()
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to rollback txn: %v", err)
		return fmt.Errorf("failed to rollback txn: %w", err)
	}
	logger.GetLogger("boulder").Debugf("Rolled back txn")
	return nil
}

func (t *TiKVTxn) Get(key string, value interface{}) (bool, error) {
	data, err := t.txn.Get(context.Background(), []byte(key))
	if err != nil {
		if errors.Is(err, tikverr.ErrNotExist) {
			return false, nil
		}
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

func (t *TiKVTxn) GetRaw(key string) ([]byte, bool, error) {
	data, err := t.txn.Get(context.Background(), []byte(key))
	if err != nil {
		if errors.Is(err, tikverr.ErrNotExist) {
			return nil, false, nil
		}
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

func (t *TiKVTxn) BatchGet(keys []string) (map[string][]byte, error) {
	ks := make([][]byte, 0, len(keys))
	for _, key := range keys {
		ks = append(ks, []byte(key))
	}
	values, err := t.txn.BatchGet(context.Background(), ks)
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to BatchGet keys: %v", err)
		return nil, fmt.Errorf("failed to BatchGet keys: %w", err)
	}

	return values, nil
}

func (t *TiKVTxn) Set(key string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		logger.GetLogger("boulder").Errorf("JSON marshal error for key %s: %v", key, err)
		return fmt.Errorf("json marshal error: %w", err)
	}

	err = t.txn.Set([]byte(key), data)
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to put key %s: %v", key, err)
	}
	return err
}

func (t *TiKVTxn) SetNX(key string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		logger.GetLogger("boulder").Errorf("JSON marshal error for key %s: %v", key, err)
		return fmt.Errorf("json marshal error: %w", err)
	}
	// 检查key是否存在
	_, err = t.txn.Get(context.Background(), []byte(key))
	if err == nil {
		logger.GetLogger("boulder").Errorf("Error getting key %s: %v", key, err)
		return ErrKeyExists
	}

	// key不存在，设置值
	err = t.txn.Set([]byte(key), data)
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to put key %s: %v", key, err)
		return err
	}
	return nil
}

func (t *TiKVTxn) BatchSet(kvs map[string]interface{}) error {
	// 遍历所有键值对
	for key, value := range kvs {
		// 序列化值
		data, err := json.Marshal(value)
		if err != nil {
			logger.GetLogger("boulder").Errorf("JSON marshal error for key %s: %v", key, err)
			return fmt.Errorf("json marshal error for key %s: %w", key, err)
		}

		// 设置键值对
		err = t.txn.Set([]byte(key), data)
		if err != nil {
			logger.GetLogger("boulder").Errorf("Failed to set key %s: %v", key, err)
			return fmt.Errorf("failed to set key %s: %w", key, err)
		}
	}

	return nil
}

func (t *TiKVTxn) Delete(key string) error {
	err := t.txn.Delete([]byte(key))
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to delete key %s: %v", key, err)
	} else {
		logger.GetLogger("boulder").Debugf("Successfully deleted key: %s", key)
	}
	return err
}

func (t *TiKVTxn) DeletePrefix(prefix string, limit int32) error {
	// 计算结束前缀：例如 user: -> user; （字节序上刚好大于 user: 的最小前缀）
	endPrefix := make([]byte, len(prefix))
	copy(endPrefix, prefix)
	for i := len(endPrefix) - 1; i >= 0; i-- {
		if endPrefix[i] < 0xFF {
			endPrefix[i]++
			endPrefix = endPrefix[:i+1]
			break
		}
		// 如果字节是0xFF，继续向前寻找可以递增的字节
		if i == 0 {
			// 整个前缀都是0xFF，需要特殊处理
			endPrefix = append([]byte(prefix), 0x00)
		}
	}

	// 创建迭代器
	iter, err := t.txn.Iter([]byte(prefix), endPrefix)
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to create iterator for prefix %s: %v", prefix, err)
		return fmt.Errorf("failed to create iterator: %w", err)
	}
	defer iter.Close()

	var count int32
	// 遍历所有匹配前缀的键并删除
	for iter.Valid() && (limit == 0 || count < limit) {
		key := iter.Key()
		count += 1
		// 删除键
		err := t.txn.Delete(key)
		if err != nil {
			logger.GetLogger("boulder").Errorf("Failed to delete key %s: %v", string(key), err)
			return fmt.Errorf("failed to delete key %s: %w", string(key), err)
		}

		err = iter.Next()
		if err != nil {
			logger.GetLogger("boulder").Errorf("Failed to advance iterator: %v", err)
			return fmt.Errorf("failed to advance iterator: %w", err)
		}
	}

	return nil
}

func (t *TiKVTxn) Scan(prefix string, startKey string, limit int) ([]string, string, error) {
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
			end = append([]byte(prefix), 0x00)
		}
	}

	// 执行扫描
	// 创建迭代器
	iter, err := t.txn.Iter(start, end)
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to create iterator for prefix %s: %v", prefix, err)
		return nil, "", fmt.Errorf("failed to create iterator: %w", err)
	}
	defer iter.Close()

	keys := make([]string, 0)
	for iter.Valid() && len(keys) < limit {
		key := iter.Key()
		keys = append(keys, string(key))
	}

	// 确定下一个起始键
	var nextKey string
	if len(keys) == limit && len(keys) > 0 {
		lastKey := keys[len(keys)-1]
		nextKey = lastKey
	}
	return keys, nextKey, nil
}
