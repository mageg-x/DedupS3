// Package block /*
package block

import (
	"context"
	"fmt"
	xconf "github.com/mageg-x/boulder/internal/config"
	"github.com/mageg-x/boulder/internal/logger"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// DiskStore 实现基于磁盘的存储后端
type DiskStore struct {
	conf *xconf.DiskConfig
	mu   sync.RWMutex
}

// NewDiskStore  创建新的磁盘存储
func NewDiskStore(c *xconf.DiskConfig) (*DiskStore, error) {
	if err := os.MkdirAll(c.Path, 0755); err != nil {
		logger.GetLogger("boulder").Errorf("failed to create disk store directory: %v", err)
		return nil, err
	}
	// 尝试创建测试文件
	testFile := filepath.Join(c.Path, ".write_test")
	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		logger.GetLogger("boulder").Errorf("failed to write test file: %v", err)
		return nil, err
	}
	// 清理测试文件
	os.Remove(testFile)

	ds := &DiskStore{
		conf: c,
		mu:   sync.RWMutex{},
	}

	logger.GetLogger("boulder").Infof("Disk store initialized successfully")
	return ds, nil
}

// Type 返回存储类型
func (d *DiskStore) Type() string {
	return "disk"
}

// WriteBlock 写入块到磁盘
func (d *DiskStore) WriteBlock(ctx context.Context, blockID string, data []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	path := d.blockPath(blockID)

	// 确保目录存在
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		logger.GetLogger("boulder").Errorf("failed to create directory for block %s: %v", blockID, err)
		return fmt.Errorf("failed to create directory for block %s: %w", blockID, err)
	}

	// 使用临时文件写入，然后重命名，确保原子性
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		logger.GetLogger("boulder").Errorf("failed to write block %s: %v", blockID, err)
		return fmt.Errorf("failed to write block %s: %w", blockID, err)
	}

	// 重命名临时文件为最终文件
	if err := os.Rename(tmpPath, path); err != nil {
		// 清理临时文件
		os.Remove(tmpPath)
		logger.GetLogger("boulder").Errorf("failed to commit block %s: %v", blockID, err)
		return fmt.Errorf("failed to commit block %s: %w", blockID, err)
	}

	logger.GetLogger("boulder").Debugf("Successfully wrote block: %s", blockID)
	return nil
}

// ReadBlock 从磁盘读取块
func (d *DiskStore) ReadBlock(blockID string, offset, length int64) ([]byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	path := d.blockPath(blockID)
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			logger.GetLogger("boulder").Debugf("Block %s does not exist", blockID)
			return nil, ErrBlockNotFound
		}
		logger.GetLogger("boulder").Errorf("failed to open block %s: %v", blockID, err)
		return nil, fmt.Errorf("failed to open block %s: %w", blockID, err)
	}
	defer file.Close()

	// 获取文件信息以确定文件大小
	fileInfo, err := file.Stat()
	if err != nil {
		logger.GetLogger("boulder").Errorf("failed to get file info for block %s: %v", blockID, err)
		return nil, fmt.Errorf("failed to get file info for block %s: %w", blockID, err)
	}

	// 如果length为0，读取整个文件
	if length == 0 {
		length = fileInfo.Size() - offset
	}

	// 检查偏移和长度是否有效
	if offset < 0 || offset >= fileInfo.Size() {
		logger.GetLogger("boulder").Errorf("Invalid offset %d for block %s of size %d", offset, blockID, fileInfo.Size())
		return nil, fmt.Errorf("invalid offset %d for block %s of size %d", offset, blockID, fileInfo.Size())
	}

	if offset+length > fileInfo.Size() {
		length = fileInfo.Size() - offset
	}

	if offset > 0 {
		_, err = file.Seek(offset, io.SeekStart)
		if err != nil {
			logger.GetLogger("boulder").Errorf("failed to seek in block %s: %v", blockID, err)
			return nil, fmt.Errorf("failed to seek in block %s: %w", blockID, err)
		}
	}

	data := make([]byte, length)
	n, err := file.Read(data)
	if err != nil && err != io.EOF {
		logger.GetLogger("boulder").Errorf("failed to read block %s: %v", blockID, err)
		return nil, fmt.Errorf("failed to read block %s: %w", blockID, err)
	}

	logger.GetLogger("boulder").Debugf("Successfully read block: %s, read %d bytes", blockID, n)
	return data[:n], nil
}

// DeleteBlock 删除块
func (d *DiskStore) DeleteBlock(blockID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	path := d.blockPath(blockID)
	if err := os.Remove(path); err != nil {
		if os.IsNotExist(err) {
			logger.GetLogger("boulder").Debugf("Block %s does not exist for deletion", blockID)
			return nil
		}
		logger.GetLogger("boulder").Errorf("failed to delete block %s: %v", blockID, err)
		return fmt.Errorf("failed to delete block %s: %w", blockID, err)
	}

	logger.GetLogger("boulder").Debugf("Successfully deleted block: %s", blockID)
	return nil
}

// BlockExists 检查块是否存在
func (d *DiskStore) BlockExists(blockID string) (bool, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	path := d.blockPath(blockID)
	_, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			logger.GetLogger("boulder").Debugf("Block %s does not exist", blockID)
			return false, nil
		}
		logger.GetLogger("boulder").Errorf("failed to check if block %s exists: %v", blockID, err)
		return false, fmt.Errorf("failed to check if block %s exists: %w", blockID, err)
	}
	return true, nil
}

// Location 获取块位置
func (d *DiskStore) Location(blockID string) string {
	return d.blockPath(blockID)
}

// List 递归遍历整个存储，流式返回 blockID
func (d *DiskStore) List() (<-chan string, <-chan error) {
	blockChan := make(chan string)
	errChan := make(chan error)

	go func() {
		defer close(blockChan)
		defer close(errChan)

		d.mu.RLock()
		rootPath := d.conf.Path
		d.mu.RUnlock()

		// 递归遍历目录
		walker := func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			// 如果是文件，检查是否是 block 文件
			if !info.IsDir() && len(info.Name()) >= 20 {
				// 假设 blockID 格式是至少20个字符的文件名
				blockID := info.Name()
				blockChan <- blockID
			}
			return nil
		}

		// 开始递归遍历
		logger.GetLogger("boulder").Infof("starting to list blocks in disk store: %s", rootPath)
		if err := filepath.Walk(rootPath, walker); err != nil {
			logger.GetLogger("boulder").Errorf("error while listing blocks: %v", err)
			errChan <- fmt.Errorf("error while listing blocks: %w", err)
		}
		logger.GetLogger("boulder").Infof("finished listing blocks in disk store")
	}()

	return blockChan, errChan
}

// blockPath 获取块的完整路径
func (d *DiskStore) blockPath(blockID string) string {
	n := len(blockID)
	dir1 := blockID[n-3:]      // 最后3位
	dir2 := blockID[n-6 : n-3] // 倒数第4-6位
	dir3 := blockID[n-9 : n-6] // 倒数第7-9位
	return filepath.Join(d.conf.Path, dir1, dir2, dir3, blockID)
}
