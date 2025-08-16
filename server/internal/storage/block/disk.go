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
package block

import (
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

// DiskStore 实现基于磁盘的存储后端
type DiskStore struct {
	Id       string
	RootPath string
	stats    StoreStats
	mu       sync.RWMutex
}

// InitDiskStore 创建新的磁盘存储
func InitDiskStore(id, rootPath string) (*DiskStore, error) {
	if err := os.MkdirAll(rootPath, 0755); err != nil {
		return nil, err
	}

	ds := &DiskStore{
		Id:       id,
		RootPath: rootPath,
	}

	// 初始化统计信息
	ds.updateStats()

	return ds, nil
}

// ID 返回存储ID
func (d *DiskStore) ID() string {
	return d.Id
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
	if err := os.WriteFile(path, data, 0644); err != nil {
		return err
	}

	d.updateStats()
	return nil
}

// ReadBlock 从磁盘读取块
func (d *DiskStore) ReadBlock(blockID string, offset, length int64) ([]byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	path := d.blockPath(blockID)
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	if offset > 0 {
		_, err = file.Seek(offset, 0)
		if err != nil {
			return nil, err
		}
	}

	data := make([]byte, length)
	n, err := file.Read(data)
	if err != nil {
		return nil, err
	}

	return data[:n], nil
}

// DeleteBlock 删除块
func (d *DiskStore) DeleteBlock(blockID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	path := d.blockPath(blockID)
	if err := os.Remove(path); err != nil {
		return err
	}

	d.updateStats()
	return nil
}

// Location 获取块位置
func (d *DiskStore) Location(blockID string) string {
	return d.blockPath(blockID)
}

// Stats 获取存储统计信息
func (d *DiskStore) Stats() StoreStats {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.stats
}

// blockPath 获取块的完整路径
func (d *DiskStore) blockPath(blockID string) string {
	// 使用两级目录分散文件
	dir1 := blockID[:2]
	dir2 := blockID[2:4]
	targetDir := filepath.Join(d.RootPath, dir1, dir2)

	if err := os.MkdirAll(targetDir, 0755); err != nil {
		// 记录错误但继续
	}

	return filepath.Join(targetDir, blockID)
}

// updateStats 更新磁盘统计信息
func (d *DiskStore) updateStats() {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(d.RootPath, &stat); err != nil {
		return
	}

	// 计算总空间和可用空间
	total := stat.Blocks * uint64(stat.Bsize)
	free := stat.Bfree * uint64(stat.Bsize)
	used := total - free

	// 更新统计信息
	d.stats = StoreStats{
		TotalSpace: int64(total),
		UsedSpace:  int64(used),
		FreeSpace:  int64(free),
	}

	// 计算块数量
	files, _ := filepath.Glob(filepath.Join(d.RootPath, "*", "*", "*"))
	d.stats.BlockCount = len(files)
}
