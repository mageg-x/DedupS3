package storage

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"

	"github.com/mageg-x/boulder/internal/logger"
	"github.com/mageg-x/boulder/internal/storage/block"
	"github.com/mageg-x/boulder/internal/utils"

	"github.com/mageg-x/boulder/internal/config"
	"github.com/mageg-x/boulder/internal/storage/kv"
	"github.com/mageg-x/boulder/meta"
)

// StorageService 管理多个存储实例
type StorageService struct {
	mutex   sync.RWMutex
	kvStore kv.KVStore
	stores  map[string]*meta.Storage
}

var (
	// 全局存储管理器实例
	instance *StorageService
	mu       = sync.Mutex{}
)

// GetStorageService 获取全局存储管理器实例
func GetStorageService() *StorageService {
	mu.Lock()
	defer mu.Unlock()
	if instance == nil || instance.kvStore == nil {
		logger.GetLogger("boulder").Infof("Initializing storage service")
		kvStore, err := kv.GetKvStore()
		if err != nil {
			logger.GetLogger("boulder").Errorf("Failed to get kv store: %v", err)
			return nil
		}
		instance = &StorageService{
			kvStore: kvStore,
			mutex:   sync.RWMutex{},
		}
		logger.GetLogger("boulder").Infof("Storage service initialized successfully")
	}

	if instance == nil || instance.kvStore == nil {
		logger.GetLogger("boulder").Error("Storage service instance is nil or kvStore is nil")
		return nil
	}
	return instance
}

// AddStorage 注册新的存储实例
func (s *StorageService) AddStorage(strType, strClass string, conf config.BlockConfig) (*meta.Storage, error) {
	// 每种class 只能有一个存储
	sl := s.GetStoragesByClass(strClass)
	if sl != nil && len(sl) > 0 {
		logger.GetLogger("boulder").Warnf("Storage already has a storage with class %s", strClass)
		return nil, errors.New("Storage already has a storage with class " + strClass)
	}
	// 检查 ID 是否已存在
	id := ""
	switch strType {
	case "s3":
		//根据 Endpoint, bucket, region 生成uuid
		id = hex.EncodeToString(utils.HmacSHA256([]byte(conf.S3.Region+conf.S3.Endpoint+conf.S3.Bucket), "aws:storage"))
		id = id[0:24]
		logger.GetLogger("boulder").Debugf("Generated S3 storage ID: %s", id)
	case "disk":
		id = hex.EncodeToString(utils.HmacSHA256([]byte(conf.Disk.Path), "aws:storage"))
		id = id[0:24]
		logger.GetLogger("boulder").Debugf("Generated disk storage ID: %s", id)
	default:
		logger.GetLogger("boulder").Errorf("Unknown storage type: %s", strType)
		return nil, fmt.Errorf("unknown storage type: %s", strType)
	}

	txn, err := s.kvStore.BeginTxn(context.Background(), nil)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to initialize kvstore txn: %v", err)
		return nil, err
	}
	defer txn.Rollback()

	key := "aws:storage:" + id
	var existing meta.Storage
	if ok, err := txn.Get(key, &existing); err == nil && ok {
		logger.GetLogger("boulder").Warnf("Storage with ID %s already exists", id)
		return nil, errors.New("storage with this ID already exists")
	}

	// 创建并存储新的 Storage 对象
	storage := &meta.Storage{
		ID:    id,
		Type:  strType,
		Class: strClass,
		Conf:  conf,
	}

	if err := txn.Set(key, storage); err != nil {
		logger.GetLogger("boulder").Errorf("Failed to store storage ID %s: %v", id, err)
		return nil, err
	}
	err = txn.Commit()
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to commit transaction: %v", err)
		return nil, err
	}
	logger.GetLogger("boulder").Infof("Successfully added storage with ID: %s, type: %s", id, strType)
	return storage, nil
}

// GetStorage 根据 ID 获取存储实例
func (s *StorageService) GetStorage(id string) (*meta.Storage, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.stores != nil && s.stores[id] != nil {
		logger.GetLogger("boulder").Debugf("Found storage ID %s in local cache", id)
		return s.stores[id], nil
	}

	key := "aws:storage:" + id
	var storage meta.Storage
	found, err := s.kvStore.Get(key, &storage)
	if err != nil || !found {
		logger.GetLogger("boulder").Warnf("Storage with ID %s does not exist", id)
		return nil, errors.New("storage with this ID does not exist")
	}

	// 根据存储类别创建对应的存储实例
	var inst block.BlockStore

	switch storage.Type {
	case "s3":
		if storage.Conf.S3 == nil {
			logger.GetLogger("boulder").Error("s3 storage not configured")
			return nil, errors.New("s3 storage not configured")
		}
		logger.GetLogger("boulder").Debugf("Creating S3 block store for bucket: %s", storage.Conf.S3.Bucket)
		inst, err = block.NewS3Store(storage.Conf.S3)
	case "disk":
		logger.GetLogger("boulder").Debugf("Creating disk block store at path: %s", storage.Conf.Disk.Path)
		inst, err = block.NewDiskStore(storage.Conf.Disk)
	default:
		logger.GetLogger("boulder").Errorf("Unknown storage type: %s", storage.Type)
		return nil, errors.New("unknown storage type")
	}

	if err != nil {
		logger.GetLogger("boulder").Errorf("Error creating block store for storage ID %s: %v", id, err)
		return nil, fmt.Errorf("error creating block store: %v", err)
	}

	storage.Instance = inst

	s.stores[id] = &storage
	logger.GetLogger("boulder").Infof("Successfully retrieved storage with ID: %s", id)
	return &storage, nil
}

// ListStorages 返回所有已注册的存储实例
func (s *StorageService) ListStorages() []*meta.Storage {
	txn, err := s.kvStore.BeginTxn(context.Background(), nil)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to initialize kvstore txn: %v", err)
		return nil
	}
	defer txn.Rollback()
	// 从KV存储中获取所有存储实例
	keys, _, err := txn.Scan("aws:storage:", "", 1000)
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to scan storages: %v", err)
		return []*meta.Storage{}
	}

	var storages []*meta.Storage
	for _, key := range keys {
		var storage meta.Storage
		if _, err := txn.Get(key, &storage); err == nil {
			storages = append(storages, &storage)
		} else {
			logger.GetLogger("boulder").Warnf("Failed to get storage from key %s: %v", key, err)
		}
	}

	logger.GetLogger("boulder").Infof("Found %d storages", len(storages))
	return storages
}

// GetStoragesByClass 根据类别获取存储实例
func (s *StorageService) GetStoragesByClass(class string) []*meta.Storage {
	logger.GetLogger("boulder").Debugf("Listing storages by class: %s", class)
	txn, err := s.kvStore.BeginTxn(context.Background(), nil)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to initialize kvstore txn: %v", err)
		return nil
	}
	defer txn.Rollback()
	// 从KV存储中获取所有存储实例并筛选
	keys, _, err := txn.Scan("aws:storage:", "", 1000)
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to scan storages: %v", err)
		return []*meta.Storage{}
	}

	var result []*meta.Storage
	for _, key := range keys {
		var storage meta.Storage
		if _, err := s.kvStore.Get(key, &storage); err == nil && storage.Class == class {
			result = append(result, &storage)
		} else if err != nil {
			logger.GetLogger("boulder").Warnf("Failed to get storage from key %s: %v", key, err)
		}
	}

	logger.GetLogger("boulder").Infof("Found %d storages with class: %s", len(result), class)
	return result
}

// GetStoragesByType 根据类型获取存储实例
func (s *StorageService) GetStoragesByType(strType string) []*meta.Storage {
	logger.GetLogger("boulder").Debugf("Listing storages by type: %s", strType)

	txn, err := s.kvStore.BeginTxn(context.Background(), nil)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to initialize kvstore txn: %v", err)
		return nil
	}
	defer txn.Rollback()

	// 从KV存储中获取所有存储实例并筛选
	keys, _, err := txn.Scan("aws:storage:", "", 1000)
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to scan storages: %v", err)
		return []*meta.Storage{}
	}

	var result []*meta.Storage
	for _, key := range keys {
		var storage meta.Storage
		if _, err := s.kvStore.Get(key, &storage); err == nil && storage.Type == strType {
			result = append(result, &storage)
		} else if err != nil {
			logger.GetLogger("boulder").Warnf("Failed to get storage from key %s: %v", key, err)
		}
	}

	logger.GetLogger("boulder").Infof("Found %d storages with type: %s", len(result), strType)
	return result
}

// RemoveStorage 删除指定 ID 的存储实例
func (s *StorageService) RemoveStorage(id string) bool {
	logger.GetLogger("boulder").Debugf("Removing storage with ID: %s", id)
	txn, err := s.kvStore.BeginTxn(context.Background(), nil)
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to initialize kvstore txn: %v", err)
		return false
	}
	defer txn.Rollback()

	key := "aws:storage:" + id
	if err := txn.Delete(key); err != nil {
		logger.GetLogger("boulder").Errorf("Failed to delete storage with ID %s: %v", id, err)
		return false
	}
	if err = txn.Commit(); err == nil {
		s.mutex.Lock()
		defer s.mutex.Unlock()

		if s.stores != nil {
			delete(s.stores, id)
			logger.GetLogger("boulder").Debugf("Removed storage ID %s from local cache", id)
		}

		logger.GetLogger("boulder").Infof("Successfully removed storage with ID: %s", id)
		return true
	}
	return false
}
