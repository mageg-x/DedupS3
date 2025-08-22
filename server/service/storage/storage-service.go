package storage

import (
	"context"
	"errors"
	"fmt"
	"github.com/mageg-x/boulder/internal/logger"
	"github.com/mageg-x/boulder/internal/storage/block"
	"github.com/mageg-x/boulder/internal/utils"
	"sync"

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
		kvStore, err := kv.GetKvStore()
		if err != nil {
			logger.GetLogger("boulder").Errorf("Failed to get kv store: %v", err)
			return nil
		}
		instance = &StorageService{
			kvStore: kvStore,
			mutex:   sync.RWMutex{},
		}
	}

	if instance == nil || instance.kvStore == nil {
		return nil
	}
	return instance
}

// AddStorage 注册新的存储实例
func (s *StorageService) AddStorage(strType, strClass string, conf config.BlockConfig) (*meta.Storage, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// 检查 ID 是否已存在
	id := ""
	switch strType {
	case "s3":
		//根据 Endpoint, bucket, region 生成uuid
		id = string(utils.HmacSHA256([]byte(conf.S3.Region+conf.S3.Endpoint+conf.S3.Bucket), "aws:storage"))
	case "disk":
		id = string(utils.HmacSHA256([]byte(conf.Disk.Path), "aws:storage"))
	default:
		return nil, fmt.Errorf("unknown storage type: %s", strType)
	}

	key := "aws:storage:" + id
	var existing meta.Storage
	if _, err := s.kvStore.Get(context.Background(), key, &existing); err == nil {
		return nil, errors.New("storage with this ID already exists")
	}

	// 创建并存储新的 Storage 对象
	storage := &meta.Storage{
		ID:    id,
		Type:  strType,
		Class: strClass,
		Conf:  conf,
	}

	if err := s.kvStore.Put(context.Background(), key, storage); err != nil {
		return nil, err
	}

	return storage, nil
}

// GetStorage 根据 ID 获取存储实例
func (s *StorageService) GetStorage(id string) (*meta.Storage, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	if s.stores != nil && s.stores[id] != nil {
		return s.stores[id], nil
	}

	key := "aws:storage:" + id
	var storage meta.Storage
	found, err := s.kvStore.Get(context.Background(), key, &storage)
	if err != nil || !found {
		return nil, errors.New("storage with this ID does not exist")
	}

	// 根据存储类别创建对应的存储实例
	var inst block.BlockStore

	switch storage.Type {
	case "s3":
		if storage.Conf.S3 == nil {
			return nil, errors.New("s3 storage not configured")
		}
		inst, err = block.NewS3Store(storage.Conf.S3)
	case "disk":
		inst, err = block.NewDiskStore(storage.Conf.Disk)
	default:
		return nil, errors.New("unknown storage type")
	}

	if err != nil {
		return nil, fmt.Errorf("error creating block store: %v", err)
	}
	store := &meta.Storage{
		ID:       storage.ID,
		Type:     storage.Type,
		Class:    storage.Class,
		Conf:     storage.Conf,
		Instance: inst,
	}
	s.stores[id] = store
	return store, nil
}

// ListStorages 返回所有已注册的存储实例
func (s *StorageService) ListStorages() []*meta.Storage {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	// 从KV存储中获取所有存储实例
	keys, _, err := s.kvStore.Scan(context.Background(), "aws:storage:")
	if err != nil {
		return []*meta.Storage{}
	}

	var storages []*meta.Storage
	for _, key := range keys {
		var storage meta.Storage
		if _, err := s.kvStore.Get(context.Background(), key, &storage); err == nil {
			storages = append(storages, &storage)
		}
	}
	return storages
}

// GetStoragesByClass 根据类别获取存储实例
func (s *StorageService) GetStoragesByClass(class string) []*meta.Storage {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	// 从KV存储中获取所有存储实例并筛选
	keys, _, err := s.kvStore.Scan(context.Background(), "aws:storage:")
	if err != nil {
		return []*meta.Storage{}
	}

	var result []*meta.Storage
	for _, key := range keys {
		var storage meta.Storage
		if _, err := s.kvStore.Get(context.Background(), key, &storage); err == nil && storage.Class == class {
			result = append(result, &storage)
		}
	}
	return result
}

// GetStoragesByClass 根据类别获取存储实例
func (s *StorageService) GetStoragesByType(strType string) []*meta.Storage {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	// 从KV存储中获取所有存储实例并筛选
	keys, _, err := s.kvStore.Scan(context.Background(), "aws:storage:")
	if err != nil {
		return []*meta.Storage{}
	}

	var result []*meta.Storage
	for _, key := range keys {
		var storage meta.Storage
		if _, err := s.kvStore.Get(context.Background(), key, &storage); err == nil && storage.Type == strType {
			result = append(result, &storage)
		}
	}
	return result
}

// RemoveStorage 删除指定 ID 的存储实例
func (s *StorageService) RemoveStorage(id string) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	key := "aws:storage:" + id
	if err := s.kvStore.Delete(context.Background(), key); err != nil {
		return false
	}
	return true
}
