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
package storage

import (
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"

	xconf "github.com/mageg-x/dedups3/internal/config"
	"github.com/mageg-x/dedups3/internal/logger"
	"github.com/mageg-x/dedups3/internal/utils"
	"github.com/mageg-x/dedups3/meta"
	block2 "github.com/mageg-x/dedups3/plugs/block"
	pconf "github.com/mageg-x/dedups3/plugs/config"
)

// StorageService 管理多个存储实例
type StorageService struct {
	mutex  sync.RWMutex
	conf   pconf.KVConfigClient
	stores map[string]*meta.Storage
}

const (
	STORAGE_PREFIX = "dedups3:default:storage-endpoint:"
)

var (
	// 全局存储管理器实例
	instance *StorageService
	mu       = sync.Mutex{}
)

// GetStorageService 获取全局存储管理器实例
func GetStorageService() *StorageService {
	mu.Lock()
	defer mu.Unlock()
	if instance == nil || instance.conf == nil {
		logger.GetLogger("dedups3").Infof("initializing storage service")
		cfg := xconf.Get()
		c, err := pconf.NewKVConfig(&pconf.Args{
			Driver:    cfg.Database.Driver,
			DSN:       cfg.Database.DSN,
			AuthToken: cfg.Database.AuthToken,
		})
		if err != nil || c == nil {
			logger.GetLogger("dedups3").Errorf("failed to get kv config : %v", err)
			return nil
		}

		instance = &StorageService{
			conf:   c,
			mutex:  sync.RWMutex{},
			stores: make(map[string]*meta.Storage),
		}
		logger.GetLogger("dedups3").Infof("storage service initialized successfully")
	}

	if instance == nil || instance.conf == nil {
		logger.GetLogger("dedups3").Error("storage service instance is nil or kvstore is nil")
		return nil
	}
	return instance
}

// AddStorage 注册新的存储实例
func (s *StorageService) AddStorage(strType, strClass string, conf xconf.StorageConfig) (*meta.Storage, error) {
	// 每种class 只能有一个存储
	err := utils.WrapFunction(func() error {
		sl := s.GetStoragesByClass(strClass)
		if sl != nil && len(sl) > 0 {
			if len(sl) == 1 {
				s0 := sl[0]
				if s0.Type == strType && s0.Class == strClass && s0.Conf.Equal(&conf) {
					return nil
				}
			}
			logger.GetLogger("dedups3").Errorf("storage already has a storage with class %s", strClass)
			return errors.New("storage already has a storage with class " + strClass)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	// 检查 ID 是否已存在
	id := conf.ID
	if id == "" {
		switch strings.ToLower(strType) {
		case meta.S3_TYPE_STORAGE:
			//根据 Endpoint, bucket, region 生成uuid
			id = hex.EncodeToString(utils.HmacSHA256([]byte(conf.S3.Region+conf.S3.Endpoint+conf.S3.Bucket), "aws:storage"))
			id = id[0:24]
			logger.GetLogger("dedups3").Debugf("generated s3 storage id: %s", id)
		case meta.DISK_TYPE_STORAGE:
			if path, err := filepath.Abs(conf.Disk.Path); err == nil {
				conf.Disk.Path = path
			}
			id = hex.EncodeToString(utils.HmacSHA256([]byte(conf.Disk.Path), "aws:storage"))
			id = id[0:24]
			logger.GetLogger("dedups3").Debugf("generated disk storage id: %s", id)
		}
	}

	if strType == meta.DISK_TYPE_STORAGE || strType == meta.S3_TYPE_STORAGE {
		if strType == meta.S3_TYPE_STORAGE {
			// 测试读写权限
			if err = block2.TestS3AccessPermissions(conf.S3); err != nil {
				logger.GetLogger("dedups3").Errorf("test s3 access permissions failed: %v", err)
				return nil, fmt.Errorf("test s3 access permissions failed: %w", err)
			}
		} else {
			if err = block2.TestDiskAccessPermissions(conf.Disk); err != nil {
				logger.GetLogger("dedups3").Errorf("test disk access permissions failed: %v", err)
				return nil, fmt.Errorf("test disk access permissions failed: %w", err)
			}
		}
	} else {
		logger.GetLogger("dedups3").Errorf("unknown storage type: %s id %s", strType, id)
		return nil, fmt.Errorf("unknown storage type: %s", strType)
	}

	txn, err := s.conf.TxnBegin()
	if err != nil || txn == "" {
		logger.GetLogger("dedups3").Errorf("failed to initialize kvstore txn: %v", err)
		return nil, fmt.Errorf("failed to initialize kvstore txn: %w", err)
	}
	defer func() {
		if txn != "" {
			_ = s.conf.TxnRollback(txn)
		}
	}()

	// 先查询是否已经存在
	sKey := STORAGE_PREFIX + id
	var _st meta.Storage
	st, err := s.conf.TxnGetKv(txn, sKey, _st)
	if err == nil && st != nil {
		logger.GetLogger("dedups3").Errorf("storage %s  already exists: %v", sKey, err)
		return nil, fmt.Errorf("storage %s  already exists: %w", sKey, err)
	}
	defaultChunkConfig := &meta.ChunkConfig{
		ChunkSize: 1024,
		FixSize:   false,
		Encrypt:   true,
		Compress:  true,
	}
	// 创建并存储新的 Storage 对象
	storage := &meta.Storage{
		ID:    id,
		Type:  strType,
		Class: strClass,
		Conf:  conf,
		Chunk: defaultChunkConfig,
	}

	if err := s.conf.TxnSetKv(txn, sKey, storage); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to store storage id %s: %v", id, err)
		return nil, fmt.Errorf("failed to store storage id %s: %w", id, err)
	}
	if err := s.conf.TxnCommit(txn); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to commit transaction: %v", err)
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}
	txn = ""

	logger.GetLogger("dedups3").Infof("successfully added storage with id: %s, type: %s", id, strType)
	return s.GetStorage(id)
}

// GetStorage 根据 ID 获取存储实例
func (s *StorageService) GetStorage(id string) (*meta.Storage, error) {
	if id == "" {
		logger.GetLogger("dedups3").Errorf("no storage id provided")
		return nil, errors.New("empty storage id")
	}
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.stores != nil && s.stores[id] != nil {
		logger.GetLogger("dedups3").Debugf("found storage id %s in local cache", id)
		return s.stores[id], nil
	}

	sKey := STORAGE_PREFIX + id
	var _ss meta.Storage
	ss, err := s.conf.Get(sKey, _ss)
	if err != nil || ss == nil {
		logger.GetLogger("dedups3").Errorf("storage with id %s does not exist", id)
		return nil, fmt.Errorf("storage with id %s does not exist", id)
	}
	_storage, ok := ss.(*meta.Storage)
	if !ok || _storage == nil {
		logger.GetLogger("dedups3").Errorf("storage  %s unmarshal failed", id)
		return nil, fmt.Errorf("storage  %s unmarshal failed", id)
	}

	// 根据存储类别创建对应的存储实例
	var inst block2.BlockStore
	switch _storage.Type {
	case meta.S3_TYPE_STORAGE:
		if _storage.Conf.S3 == nil {
			logger.GetLogger("dedups3").Error("s3 storage not configured")
			return nil, errors.New("s3 storage not configured")
		}
		logger.GetLogger("dedups3").Debugf("creating s3 block store for bucket: %s", _storage.Conf.S3.Bucket)
		inst, err = block2.NewS3Store(id, _storage.Class, _storage.Conf.S3)
	case meta.DISK_TYPE_STORAGE:
		if _storage.Conf.Disk == nil {
			logger.GetLogger("dedups3").Error("disk storage not configured")
			return nil, errors.New("disk storage not configured")
		}
		logger.GetLogger("dedups3").Debugf("creating disk block store at path: %s", _storage.Conf.Disk.Path)
		inst, err = block2.NewDiskStore(id, _storage.Class, _storage.Conf.Disk)
	default:
		logger.GetLogger("dedups3").Errorf("unknown storage type: %s", _storage.Type)
		return nil, errors.New("unknown storage type")
	}

	if err != nil && inst != nil {
		logger.GetLogger("dedups3").Errorf("error creating block store for storage id %s: %v", id, err)
		return nil, fmt.Errorf("error creating block store: %w", err)
	}

	_storage.Instance = inst

	s.stores[id] = _storage
	logger.GetLogger("dedups3").Infof("successfully retrieved storage with id: %s", id)
	return _storage, nil
}

// ListStorages 返回所有已注册的存储实例
func (s *StorageService) ListStorages() []*meta.Storage {
	var _ss meta.Storage
	sl, _, err := s.conf.List(STORAGE_PREFIX, "", 1000, _ss)
	if err != nil || sl == nil {
		logger.GetLogger("dedups3").Errorf("failed to scan storages: %v", err)
		return []*meta.Storage{}
	}
	var storages []*meta.Storage
	for k, ss := range sl {
		_storage, ok := ss.(*meta.Storage)
		if !ok || _storage == nil {
			logger.GetLogger("dedups3").Errorf("failed unmarshal %s storage", k)
			continue
		}
		storages = append(storages, _storage)
	}

	logger.GetLogger("dedups3").Infof("found %d storages", len(storages))
	return storages
}

// GetStoragesByClass 根据类别获取存储实例
func (s *StorageService) GetStoragesByClass(class string) []*meta.Storage {
	logger.GetLogger("dedups3").Debugf("listing storages by class: %s", class)

	storages := s.ListStorages()
	var result []*meta.Storage
	for _, storage := range storages {
		if storage.Class == class {
			result = append(result, storage)
		}
	}

	logger.GetLogger("dedups3").Infof("found %d storages with class: %s", len(result), class)
	return result
}

// GetStoragesByType 根据类型获取存储实例
func (s *StorageService) GetStoragesByType(strType string) []*meta.Storage {
	logger.GetLogger("dedups3").Debugf("listing storages by type: %s", strType)
	storages := s.ListStorages()
	var result []*meta.Storage
	for _, storage := range storages {
		if strType == storage.Type {
			result = append(result, storage)
		}
	}

	logger.GetLogger("dedups3").Infof("found %d storages with type: %s", len(result), strType)
	return result
}

// RemoveStorage 删除指定 ID 的存储实例
func (s *StorageService) RemoveStorage(id string) bool {
	logger.GetLogger("dedups3").Debugf("removing storage with id: %s", id)
	txn, err := s.conf.TxnBegin()
	if err != nil || txn == "" {
		logger.GetLogger("dedups3").Errorf("failed to initialize kvstore txn: %v", err)
		return false
	}
	defer func() {
		if txn != "" {
			_ = s.conf.TxnRollback(txn)
		}
	}()

	sKey := STORAGE_PREFIX + id
	if err = s.conf.TxnDelKv(txn, sKey); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to delete storage with id %s: %v", id, err)
		return false
	}

	if err := s.conf.TxnCommit(txn); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to commit transaction: %v", err)
		return false
	}
	txn = ""

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.stores != nil {
		delete(s.stores, id)
		logger.GetLogger("dedups3").Debugf("removed storage id %s from local cache", id)
	}

	logger.GetLogger("dedups3").Infof("successfully removed storage with id: %s", id)
	return true
}

func (s *StorageService) ListChunkConfig(storageID string) (map[string]*meta.ChunkConfig, error) {
	defaultChunkConfig := &meta.ChunkConfig{
		ChunkSize: 1024 * 1024,
		FixSize:   false,
		Encrypt:   true,
		Compress:  true,
	}

	storages := s.ListStorages()
	result := make(map[string]*meta.ChunkConfig)
	for _, _s := range storages {
		if _s.Chunk != nil {
			result[_s.ID] = _s.Chunk
		} else {
			result[_s.ID] = defaultChunkConfig
		}
	}
	return result, nil
}

func (s *StorageService) SetChunkConfig(storageID string, chunk *meta.ChunkConfig) error {
	if chunk == nil {
		logger.GetLogger("dedups3").Debugf("chunk is nil for storage %s", storageID)
		return errors.New("chunk is nil")
	}

	txn, err := s.conf.TxnBegin()
	if err != nil || txn == "" {
		logger.GetLogger("dedups3").Errorf("failed to initialize kvstore txn: %v", err)
		return fmt.Errorf("failed to initialize kvstore txn: %w", err)
	}
	defer func() {
		if txn != "" {
			_ = s.conf.TxnRollback(txn)
		}
	}()

	sKey := STORAGE_PREFIX + storageID
	var _ss meta.Storage
	ss, err := s.conf.TxnGetKv(txn, sKey, _ss)
	if err != nil || ss == nil {
		logger.GetLogger("dedups3").Errorf("failed to get storage %s: %v", sKey, err)
		return fmt.Errorf("failed to get storage %s: %w", sKey, err)
	}
	_storage, ok := ss.(*meta.Storage)
	if !ok || _storage == nil {
		logger.GetLogger("dedups3").Errorf("failed unmarshal %s storage", sKey)
		return fmt.Errorf("failed unmarshal %s storage", sKey)
	}
	_storage.Chunk = chunk
	if err := s.conf.TxnSetKv(txn, sKey, _storage); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to set storage %s: %v", sKey, err)
		return fmt.Errorf("failed to set storage %s: %w", sKey, err)
	}
	if err := s.conf.TxnCommit(txn); err != nil {
		logger.GetLogger("dedups3").Errorf("failed to commit transaction: %v", err)
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	txn = ""
	return nil
}
