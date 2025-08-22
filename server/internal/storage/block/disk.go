// Package block /*
package block

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	xconf "github.com/mageg-x/boulder/internal/config"
)

// DiskStore 实现基于磁盘的存储后端
type DiskStore struct {
	conf xconf.DiskConfig
	mu   sync.RWMutex
}

// NewDiskStore  创建新的磁盘存储
func NewDiskStore(c xconf.DiskConfig) (*DiskStore, error) {
	if err := os.MkdirAll(c.Path, 0755); err != nil {
		return nil, err
	}
	// 尝试创建测试文件
	testFile := filepath.Join(c.Path, ".write_test")
	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		return nil, err
	}
	// 清理测试文件
	os.Remove(testFile)

	ds := &DiskStore{
		conf: c,
		mu:   sync.RWMutex{},
	}

	return ds, nil
}

// Type 返回存储类型
func (d *DiskStore) Type() string {
	return "disk"
}

// WriteBlock 写入块到磁盘
func (d *DiskStore) WriteBlock(blockID string, data []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	path := d.blockPath(blockID)

	// 确保目录存在
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("failed to create directory for block %s: %w", blockID, err)
	}

	// 使用临时文件写入，然后重命名，确保原子性
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write block %s: %w", blockID, err)
	}

	// 重命名临时文件为最终文件
	if err := os.Rename(tmpPath, path); err != nil {
		// 清理临时文件
		os.Remove(tmpPath)
		return fmt.Errorf("failed to commit block %s: %w", blockID, err)
	}

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
			return nil, fmt.Errorf("block %s does not exist", blockID)
		}
		return nil, fmt.Errorf("failed to open block %s: %w", blockID, err)
	}
	defer file.Close()

	// 获取文件信息以确定文件大小
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file info for block %s: %w", blockID, err)
	}

	// 如果length为0，读取整个文件
	if length == 0 {
		length = fileInfo.Size() - offset
	}

	// 检查偏移和长度是否有效
	if offset < 0 || offset >= fileInfo.Size() {
		return nil, fmt.Errorf("invalid offset %d for block %s of size %d", offset, blockID, fileInfo.Size())
	}

	if offset+length > fileInfo.Size() {
		length = fileInfo.Size() - offset
	}

	if offset > 0 {
		_, err = file.Seek(offset, io.SeekStart)
		if err != nil {
			return nil, fmt.Errorf("failed to seek in block %s: %w", blockID, err)
		}
	}

	data := make([]byte, length)
	n, err := file.Read(data)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to read block %s: %w", blockID, err)
	}

	return data[:n], nil
}

// DeleteBlock 删除块
func (d *DiskStore) DeleteBlock(blockID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	path := d.blockPath(blockID)
	if err := os.Remove(path); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("block %s does not exist", blockID)
		}
		return fmt.Errorf("failed to delete block %s: %w", blockID, err)
	}

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
			return false, nil
		}
		return false, fmt.Errorf("failed to check if block %s exists: %w", blockID, err)
	}
	return true, nil
}

// Location 获取块位置
func (d *DiskStore) Location(blockID string) string {
	return d.blockPath(blockID)
}

// blockPath 获取块的完整路径
func (d *DiskStore) blockPath(blockID string) string {
	dir1 := blockID[:2]
	dir2 := blockID[2:4]
	return filepath.Join(d.conf.Path, dir1, dir2, blockID)
}
