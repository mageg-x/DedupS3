package fs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mageg-x/boulder/internal/utils"
	"golang.org/x/sys/unix"
)

var (
	ErrInvalidSize    = errors.New("invalid size")
	ErrSpaceExhausted = errors.New("mmap space exhausted")
	ErrFileNotFound   = errors.New("file not found")
	ErrFileTooLarge   = errors.New("file too large")
	ErrOutOfBounds    = errors.New("write out of bounds")
	ErrInvalidPath    = errors.New("invalid path")
	ErrSystemClosed   = errors.New("filesystem closed")
)

// TieredFs 基于 mmap 的二级存储文件系统（优化版）
type TieredFs struct {
	// 基础配置
	diskDir  string
	mmapSize int64
	closed   int32 // 原子操作标记

	// mmap 相关
	mmapData []byte
	mmapFile *os.File
	mmapFD   int

	// 文件管理
	mu    sync.RWMutex
	files map[string]*FileRegion

	// 空间管理
	freeManager *FreeListManager

	// 同步管理
	syncManager *SyncManager

	// 同步目标，disk 或 S3
	syncTarget SyncTarget

	// 统计信息
	stats *Stats
}

// Stats 统计信息
type Stats struct {
	mu           sync.RWMutex
	ReadCount    int64
	WriteCount   int64
	SyncCount    int64
	CacheHits    int64
	CacheMisses  int64
	BytesRead    int64
	BytesWritten int64
	LastSyncTime time.Time
}

// Config 配置选项
type Config struct {
	MmapSize     int64         // mmap大小
	DiskDir      string        // 磁盘目录
	SyncInterval time.Duration // 同步间隔
	BatchSize    int           // 批处理大小
	EnableSync   bool          // 是否启用异步同步
}

// DefaultConfig 默认配置
func DefaultConfig() *Config {
	return &Config{
		MmapSize:     1 << 30, // 1GB
		SyncInterval: 100 * time.Millisecond,
		BatchSize:    10,
		EnableSync:   true,
	}
}

// NewTieredFs 创建新的 TieredFs（优化版）
func NewTieredFs(config *Config) (*TieredFs, error) {
	if config == nil {
		config = DefaultConfig()
	}

	if err := os.MkdirAll(config.DiskDir, 0755); err != nil {
		return nil, fmt.Errorf("create disk dir: %w", err)
	}

	// 创建 mmap 文件
	mmapPath := filepath.Join(config.DiskDir, ".mmap_cache.dat")
	file, err := os.OpenFile(mmapPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("create mmap file: %w", err)
	}

	// 扩展文件
	if err := unix.Ftruncate(int(file.Fd()), config.MmapSize); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("truncate mmap file: %w", err)
	}

	// mmap 映射
	data, err := unix.Mmap(int(file.Fd()), 0, int(config.MmapSize),
		unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("mmap failed: %w", err)
	}

	fs := &TieredFs{
		diskDir:     config.DiskDir,
		mmapSize:    config.MmapSize,
		mmapData:    data,
		mmapFile:    file,
		mmapFD:      int(file.Fd()),
		files:       make(map[string]*FileRegion),
		freeManager: NewFreeListManager(config.MmapSize),
		stats:       &Stats{},
	}

	// 初始化同步管理器
	if config.EnableSync {
		fs.syncManager = NewSyncManager(fs.flushToFile)
	}

	return fs, nil
}

func (fs *TieredFs) AddSyncTarget(target SyncTarget) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.syncTarget = target
	return nil
}

// WriteFile 写入文件（优化版）
func (fs *TieredFs) WriteFile(path string, chunks [][]byte, ver int32) error {
	if atomic.LoadInt32(&fs.closed) == 1 {
		return ErrSystemClosed
	}

	if err := fs.validatePath(path); err != nil {
		return err
	}

	// 计算总长度
	var totalLen int64
	for _, chunk := range chunks {
		totalLen += int64(len(chunk))
	}

	if totalLen == 0 {
		return fs.Remove(path) // 空文件视为删除
	}

	if totalLen > fs.mmapSize {
		return ErrFileTooLarge
	}

	// 分配空间
	offset, err := fs.freeManager.BestFitAlloc(totalLen)
	if err != nil {
		return err
	}

	// 边界检查
	if offset+totalLen > fs.mmapSize {
		fs.freeManager.Free(offset, totalLen)
		return ErrOutOfBounds
	}

	fs.mu.Lock()
	//logger.GetLogger("boulder").Errorf("block %s get write region [%d-%d]", path, offset, offset+totalLen)
	oldRegion := fs.files[path]
	if oldRegion != nil && ver < oldRegion.Ver {
		// 现有的版本更新
		fs.freeManager.Free(offset, totalLen)
		return nil
	}

	// 零拷贝写入
	dest := fs.mmapData[offset : offset+totalLen]
	written := 0
	for _, chunk := range chunks {
		n := copy(dest[written:], chunk)
		written += n
	}

	// 更新文件映射
	newRegion := &FileRegion{
		Region: Region{Start: offset, End: offset + totalLen},
		Path:   path,
		Ver:    ver,
	}

	fs.files[path] = newRegion
	fs.mu.Unlock()

	// 释放旧空间
	if oldRegion != nil {
		fs.freeManager.Free(oldRegion.Start, oldRegion.Size())
	}

	// 更新统计
	fs.stats.mu.Lock()
	fs.stats.WriteCount++
	fs.stats.BytesWritten += totalLen
	fs.stats.mu.Unlock()

	// 异步同步
	if fs.syncManager != nil {
		return fs.syncManager.Submit(newRegion, 1, nil)
	}

	return nil
}

// ReadFile 读取文件（支持偏移量读取）
func (fs *TieredFs) ReadFile(path string, offset, length int64) ([]byte, error) {
	if atomic.LoadInt32(&fs.closed) == 1 {
		return nil, ErrSystemClosed
	}

	if err := fs.validatePath(path); err != nil {
		return nil, err
	}

	// 参数验证
	if offset < 0 {
		return nil, errors.New("invalid offset: must be >= 0")
	}
	if length < 0 {
		return nil, errors.New("invalid length: must be >= 0")
	}

	var data []byte

	err := utils.WrapFunction(func() error {
		fs.mu.RLock()
		region, exists := fs.files[path]
		if exists {
			defer fs.mu.RUnlock()
			fileSize := region.Size()
			if offset > fileSize {
				return fmt.Errorf("offset %d exceeds file size %d", offset, fileSize)
			}

			// 计算实际读取长度
			remaining := fileSize - offset
			var readSize int64
			if length == 0 {
				readSize = remaining // 读取从偏移到文件末尾的所有数据
			} else {
				if length > remaining {
					return fmt.Errorf("read length %d exceeds available data %d from offset %d", length, remaining, offset)
				}
				readSize = length
			}

			// 从内存映射区域读取
			startPos := region.Start + offset
			data = make([]byte, readSize)
			copy(data, fs.mmapData[startPos:startPos+readSize])
			return nil
		} else {
			// 及时释放锁
			fs.mu.RUnlock()
			diskPath := fs.diskPath(path)
			// 缓存未命中，从磁盘读取
			info, err := os.Stat(diskPath)
			if err != nil {
				return err
			}

			fileSize := info.Size()
			if offset > fileSize {
				return fmt.Errorf("offset %d exceeds file size %d", offset, fileSize)
			}

			// 计算实际读取长度
			remaining := fileSize - offset
			var readSize int64
			if length == 0 {
				readSize = remaining // 读取从偏移到文件末尾的所有数据
			} else {
				if length > remaining {
					return fmt.Errorf("read length %d exceeds available data %d from offset %d", length, remaining, offset)
				}
				readSize = length
			}

			// 使用随机读取方式从磁盘读取指定范围
			f, err := os.Open(diskPath)
			if err != nil {
				return err
			}
			defer f.Close()

			// 定位到指定偏移量
			if _, err := f.Seek(offset, io.SeekStart); err != nil {
				return fmt.Errorf("seek to offset %d failed: %w", offset, err)
			}

			data = make([]byte, readSize)
			n, err := io.ReadFull(f, data)
			if err != nil && err != io.EOF && errors.Is(err, io.ErrUnexpectedEOF) {
				return fmt.Errorf("read failed: %w", err)
			}
			// 如果读取的字节数少于请求的字节数（比如文件被截断），调整返回的数据
			if int64(n) < readSize {
				data = data[:n]
			}
			return nil
		}
	})
	if err != nil {
		return nil, err
	}

	// 更新统计信息
	fs.stats.mu.Lock()
	fs.stats.ReadCount++
	fs.stats.CacheMisses++
	fs.stats.BytesRead += int64(len(data))
	fs.stats.mu.Unlock()

	return data, nil
}

// Remove 删除文件（优化版）
func (fs *TieredFs) Remove(path string) error {
	if atomic.LoadInt32(&fs.closed) == 1 {
		return ErrSystemClosed
	}

	if err := fs.validatePath(path); err != nil {
		return err
	}

	fs.mu.Lock()
	region, exists := fs.files[path]
	if exists {
		delete(fs.files, path)
	}
	fs.mu.Unlock()

	if exists {
		fs.freeManager.Free(region.Start, region.Size())
	}

	// 删除磁盘文件（忽略错误）
	_ = os.Remove(fs.diskPath(path))
	return nil
}

// Sync 强制同步指定文件到磁盘
func (fs *TieredFs) Sync(path string) error {
	if atomic.LoadInt32(&fs.closed) == 1 {
		return ErrSystemClosed
	}

	fs.mu.RLock()
	region, exists := fs.files[path]
	fs.mu.RUnlock()

	if !exists {
		return ErrFileNotFound
	}

	err := fs.flushToFile(region)
	if err == nil {
		fs.stats.mu.Lock()
		fs.stats.SyncCount++
		fs.stats.LastSyncTime = time.Now()
		fs.stats.mu.Unlock()
	}
	return err
}

// SyncAll 同步所有文件
func (fs *TieredFs) SyncAll() error {
	if atomic.LoadInt32(&fs.closed) == 1 {
		return ErrSystemClosed
	}

	fs.mu.RLock()
	regions := make([]*FileRegion, 0, len(fs.files))
	for _, region := range fs.files {
		regions = append(regions, region)
	}
	fs.mu.RUnlock()

	var lastErr error
	for _, region := range regions {
		if err := fs.flushToFile(region); err != nil {
			lastErr = err
		}
	}

	if lastErr == nil {
		fs.stats.mu.Lock()
		fs.stats.SyncCount += int64(len(regions))
		fs.stats.LastSyncTime = time.Now()
		fs.stats.mu.Unlock()
	}

	return lastErr
}

// Exists 检查文件是否存在
func (fs *TieredFs) Exists(path string) bool {
	if atomic.LoadInt32(&fs.closed) == 1 {
		return false
	}

	fs.mu.RLock()
	_, exists := fs.files[path]
	fs.mu.RUnlock()

	if exists {
		return true
	}

	// 检查磁盘
	_, err := os.Stat(fs.diskPath(path))
	return err == nil
}

// ListFiles 列出所有文件
func (fs *TieredFs) ListFiles() []string {
	if atomic.LoadInt32(&fs.closed) == 1 {
		return nil
	}

	fs.mu.RLock()
	files := make([]string, 0, len(fs.files))
	for path := range fs.files {
		files = append(files, path)
	}
	fs.mu.RUnlock()

	return files
}

// FreeSpace 获取空闲空间
func (fs *TieredFs) FreeSpace() int64 {
	return fs.freeManager.FreeSpace()
}

// Close 关闭文件系统（优化版）
func (fs *TieredFs) Close() error {
	if !atomic.CompareAndSwapInt32(&fs.closed, 0, 1) {
		return nil // 已经关闭
	}

	var errs []error

	// 关闭同步管理器
	if fs.syncManager != nil {
		if err := fs.syncManager.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close sync manager: %w", err))
		}
	}

	// 最终同步
	if err := fs.SyncAll(); err != nil {
		errs = append(errs, fmt.Errorf("final sync: %w", err))
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()
	// 清理资源
	if fs.mmapData != nil {
		if err := unix.Munmap(fs.mmapData); err != nil {
			errs = append(errs, fmt.Errorf("munmap: %w", err))
		}
		fs.mmapData = nil
	}

	if fs.mmapFile != nil {
		if err := fs.mmapFile.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close mmap file: %w", err))
		}
		fs.mmapFile = nil
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors during close: %v", errs)
	}

	return nil
}

// validatePath 验证路径
func (fs *TieredFs) validatePath(path string) error {
	if path == "" {
		return ErrInvalidPath
	}

	// 防止路径遍历攻击
	if strings.Contains(path, "..") {
		return ErrInvalidPath
	}

	return nil
}

// diskPath 获取磁盘路径
func (fs *TieredFs) diskPath(blockID string) string {
	if fs.syncTarget != nil {
		return fs.syncTarget.BlockPath(blockID)
	}
	return filepath.Clean(blockID)
}

// flushToFile 将 mmap 区域刷入磁盘文件（调用者需持有锁）
// 最终修复版本
func (fs *TieredFs) flushToFile(region *FileRegion) error {
	if region == nil {
		return nil
	}

	return utils.RetryCall(3, func() error {
		fs.mu.RLock()

		// 关键修复：验证区域归属
		currentRegion, exists := fs.files[region.Path]
		if !exists || !region.Equals(currentRegion) {
			fs.mu.RUnlock()
			return nil
		}

		// 复制数据避免竞态
		data := make([]byte, region.Size())
		copy(data, fs.mmapData[region.Start:region.End])
		fs.mu.RUnlock()

		if fs.syncTarget != nil {
			err := fs.syncTarget.WriteBlockDirect(context.Background(), region.Path, data)
			if err == nil {
				fs.mu.Lock()
				// 再次验证
				currentRegion, exists = fs.files[region.Path]
				if exists && region.Equals(currentRegion) {
					delete(fs.files, region.Path)
					fs.freeManager.Free(region.Start, region.Size())
				}
				fs.mu.Unlock()
			}
			return err
		}
		return nil
	})
}
