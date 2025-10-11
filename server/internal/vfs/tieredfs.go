package vfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"

	"github.com/mageg-x/dedups3/internal/logger"
	"github.com/mageg-x/dedups3/internal/utils"
)

const (
	MagicNumber      uint64 = 0x424f554c44455200 // "BOULDER\0"
	MetadataVersion  uint32 = 1
	MetadataMaxFiles        = 200
)

var (
	MetadataTotalSize = int64(unsafe.Sizeof(Metadata{}))
)

var (
	ErrInvalidSize     = errors.New("invalid size")
	ErrSpaceExhausted  = errors.New("mmap space exhausted")
	ErrFileNotFound    = errors.New("file not found")
	ErrFileTooLarge    = errors.New("file too large")
	ErrOutOfBounds     = errors.New("write out of bounds")
	ErrInvalidPath     = errors.New("invalid path")
	ErrSystemClosed    = errors.New("filesystem closed")
	ErrInvalidMetadata = errors.New("invalid metadata")
)

// FileMetadata 用于序列化的文件元数据
type FileMetadata struct {
	StorageID [32]byte
	BlockID   [32]byte // 固定长度路径
	Start     int64
	End       int64
	Ver       int32
	Discard   bool
}

// Metadata 整个文件系统的元数据
type Metadata struct {
	MagicNumber uint64    // 魔数，用于识别文件格式
	Version     uint32    // 元数据版本
	Reserved    [116]byte // 保留字段，用于对齐和未来扩展
	Files       [MetadataMaxFiles]FileMetadata
}

type TargetTask struct {
	storageID string
	blockID   string
	ver       int32
	cancel    context.CancelFunc
}

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

	// target任务map
	targetTasks map[string]*TargetTask

	// 文件管理
	mu    sync.RWMutex
	files map[string]*FileRegion

	// 空间管理
	freeManager *FreeListManager

	// 同步管理
	syncManager *SyncManager

	// 同步目标，disk 或 S3
	syncTargetor map[string]SyncTargetor

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
}

// DefaultConfig 默认配置
func DefaultConfig() *Config {
	return &Config{
		MmapSize:     2 << 30, // 2GB
		SyncInterval: 100 * time.Millisecond,
		BatchSize:    10,
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
	var (
		file *os.File
		err  error
	)

	//# 降低比例阈值，但延长存在时间
	//echo 30 > /proc/sys/vm/dirty_ratio
	//echo 20 > /proc/sys/vm/dirty_background_ratio
	//# 关键：均匀刷盘的核心参数
	//echo 6000 > /proc/sys/vm/dirty_expire_centisecs
	//echo 1000 > /proc/sys/vm/dirty_writeback_centisecs
	// 创建 mmap 文件
	mmapPath := filepath.Join(config.DiskDir, ".mmap_cache.dat")

	file, err = os.OpenFile(mmapPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("create mmap file: %w", err)
	}

	// 扩展文件
	if err = unix.Ftruncate(int(file.Fd()), config.MmapSize); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("failed truncate mmap file: %w", err)
	}

	// 创建文件后，用 fallocate 预分配空间
	err = unix.Fallocate(int(file.Fd()), 0, 0, config.MmapSize)
	if err != nil {
		logger.GetLogger("dedups3").Warnf("failed to fallocate mmap file: %v", err)
	}

	// mmap 映射
	data, err := unix.Mmap(int(file.Fd()), 0, int(config.MmapSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("mmap failed: %w", err)
	}

	fs := &TieredFs{
		diskDir:      config.DiskDir,
		mmapSize:     config.MmapSize,
		mmapData:     data,
		mmapFile:     file,
		mmapFD:       int(file.Fd()),
		targetTasks:  make(map[string]*TargetTask),
		files:        make(map[string]*FileRegion),
		syncTargetor: make(map[string]SyncTargetor),
		freeManager:  NewFreeListManager(config.MmapSize),
		stats:        &Stats{},
	}

	// 初始化同步管理器
	fs.syncManager = NewSyncManager(fs.flushToTarget)

	// 尝试从文件加载元数据
	if err := fs.loadMetadata(); err != nil {
		_ = file.Close()
		// 如果加载失败，记录警告日志并继续，使用空的文件系统
		logger.GetLogger("dedups3").Warnf("Failed to load metadata, starting fresh: %v", err)
		return nil, fmt.Errorf("failed to load metadata: %w", err)
	}

	// 设置“伪析构函数”
	runtime.SetFinalizer(fs, func(f *TieredFs) {
		_ = f.Close()
	})
	return fs, nil
}

// loadMetadata 从mmap_cache.dat文件加载元数据
func (fs *TieredFs) loadMetadata() error {
	// 检查文件大小是否至少有元数据头部大小
	fileInfo, err := fs.mmapFile.Stat()
	if err != nil {
		return fmt.Errorf("stat file: %w", err)
	}

	if fileInfo.Size() < MetadataTotalSize {
		// 文件太小，不可能包含有效元数据
		return ErrInvalidMetadata
	}

	// 恢复文件系统状态
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// 直接将 mmap 内存映射为结构体指针
	metaPtr := (*Metadata)(unsafe.Pointer(&fs.mmapData[0]))

	if metaPtr.MagicNumber == 0 {
		// 未初始化的 内容
		metaPtr.MagicNumber = MagicNumber
		metaPtr.Version = MetadataVersion
		// 保留元数据区域不被分配
		err = fs.freeManager.AllocAt(0, MetadataTotalSize)
		// 异步同步，不阻塞业务
		unix.Msync(fs.mmapData, unix.MS_ASYNC)
		return err
	}

	// 验证魔数
	if metaPtr.MagicNumber != MagicNumber {
		return ErrInvalidMetadata
	}
	// 验证版本
	if metaPtr.Version != MetadataVersion {
		return fmt.Errorf("unsupported metadata version: %d", metaPtr.Version)
	}

	// 保留元数据区域不被分配
	err = fs.freeManager.AllocAt(0, MetadataTotalSize)
	// 保留元数据区域不被分配
	if err != nil {
		logger.GetLogger("dedups3").Errorf("allocate meta region failed: %v", err)
		return fmt.Errorf("allocate meta region failed: %w", err)
	}

	for i := 0; i < MetadataMaxFiles; i++ {
		fileMeta := metaPtr.Files[i]
		// 验证元数据的有效性
		if fileMeta.End <= fileMeta.Start || fileMeta.End > fs.mmapSize {
			fileMeta.Discard = true
		}
		if fileMeta.Discard {
			continue
		}
		storageID := string(fileMeta.StorageID[:])
		blockID := string(fileMeta.BlockID[:])
		// 创建文件区域
		fileRegion := &FileRegion{
			Region:    Region{Start: fileMeta.Start, End: fileMeta.End},
			StorageID: storageID,
			BlockID:   blockID,
			Ver:       fileMeta.Ver,
		}
		// 添加到文件映射表
		fs.files[blockID] = fileRegion
		// 从空闲空间中移除已使用的区域
		fs.freeManager.AllocAt(fileMeta.Start, fileMeta.End)
	}

	// 重建 vfs.syncManager 同步任务
	for _, file := range fs.files {
		logger.GetLogger("dedups3").Debugf("restore file %s meta form mmap", file.BlockID)
		fs.syncManager.Submit(file, 0, nil)
	}
	logger.GetLogger("dedups3").Infof("Loaded %#v files", fs.files)
	return nil
}

// saveMetadata 将元数据保存到mmap_cache.dat文件
func (fs *TieredFs) saveMetadata() error {
	if fs.mmapData == nil || fs.mmapFile == nil {
		logger.GetLogger("dedups3").Errorf("mmap data is empty")
		return fmt.Errorf("mmap data or file is empty")
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	// 直接将 mmap 内存映射为结构体指针
	metaPtr := (*Metadata)(unsafe.Pointer(&fs.mmapData[0]))
	metaPtr.MagicNumber = MagicNumber
	metaPtr.Version = MetadataVersion

	if len(fs.files) > MetadataMaxFiles {
		logger.GetLogger("dedups3").Errorf("too many files")
		return fmt.Errorf("too many files")
	}
	idx := 0
	for _, file := range fs.files {
		var storageID, blockID [32]byte
		// 自动截断超过32字节的部分，不足32字节用 \x00 填充
		copy(storageID[:], file.StorageID)
		copy(blockID[:], file.BlockID)
		metaPtr.Files[idx] = FileMetadata{
			StorageID: storageID,
			BlockID:   blockID,
			Start:     file.Start,
			End:       file.End,
			Ver:       file.Ver,
			Discard:   false,
		}
		idx++
	}

	for ; idx < MetadataMaxFiles; idx++ {
		metaPtr.Files[idx] = FileMetadata{}
	}
	// 异步同步，不阻塞业务
	unix.Msync(fs.mmapData, unix.MS_ASYNC)
	return nil
}

func (fs *TieredFs) AddSyncTargetor(storageID string, targetor SyncTargetor) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.syncTargetor[storageID] = targetor
	return nil
}

// WriteFile 写入文件（优化版）
func (fs *TieredFs) WriteFile(storageID, blockID string, chunks [][]byte, ver int32) error {
	if atomic.LoadInt32(&fs.closed) == 1 {
		return ErrSystemClosed
	}

	if err := fs.validatePath(blockID); err != nil {
		return err
	}

	if len(fs.files) >= MetadataMaxFiles {
		logger.GetLogger("dedups3").Errorf("too many files")
		return fmt.Errorf("too many files")
	}

	// 计算总长度
	var totalLen int64
	for _, chunk := range chunks {
		totalLen += int64(len(chunk))
	}

	if totalLen == 0 {
		logger.GetLogger("dedups3").Errorf("write 0 bytes to file %s", blockID)
		return fs.Remove(storageID, blockID) // 空文件视为删除
	}

	if totalLen > fs.mmapSize {
		return ErrFileTooLarge
	}

	// 分配空间
	offset, err := fs.freeManager.BestFitAlloc(totalLen)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("no space for alloc: %v", err)
		return fmt.Errorf("no space for alloc %w", err)
	}

	// 边界检查
	if offset+totalLen > fs.mmapSize {
		fs.freeManager.Free(offset, totalLen)
		return ErrOutOfBounds
	}

	fs.mu.Lock()
	//logger.GetLogger("dedups3").Errorf("block %s get write region [%d-%d]", path, offset, offset+totalLen)
	oldRegion := fs.files[blockID]
	if oldRegion != nil && ver < oldRegion.Ver {
		// 现有的版本更新
		fs.freeManager.Free(offset, totalLen)
		return nil
	}

	// 更新文件映射
	newRegion := &FileRegion{
		Region:    Region{Start: offset, End: offset + totalLen},
		StorageID: storageID,
		BlockID:   blockID,
		Ver:       ver,
	}

	fs.files[blockID] = newRegion
	fs.mu.Unlock()

	// 释放旧空间
	if oldRegion != nil {
		fs.freeManager.Free(oldRegion.Start, oldRegion.Size())
		fs.discardRegion(oldRegion)
	}

	// 拷贝写入
	dest := fs.mmapData[offset : offset+totalLen]
	written := 0
	for _, chunk := range chunks {
		n := copy(dest[written:], chunk)
		written += n
	}

	fs.saveMetadata()
	if fs.mmapData != nil {
		// 异步同步，不阻塞业务
		unix.Msync(fs.mmapData, unix.MS_ASYNC)
	}

	// 更新统计
	fs.stats.mu.Lock()
	fs.stats.WriteCount++
	fs.stats.BytesWritten += totalLen
	fs.stats.mu.Unlock()

	// 异步同步
	if fs.syncManager != nil {
		priority := 1
		if ver == 0x07FFFF {
			priority = 3
		}
		return fs.syncManager.Submit(newRegion, priority, nil)
	}

	return nil
}

// ReadFile 读取文件（支持偏移量读取）
func (fs *TieredFs) ReadFile(storageID, blockID string, offset, length int64) ([]byte, error) {
	if atomic.LoadInt32(&fs.closed) == 1 {
		return nil, ErrSystemClosed
	}

	if err := fs.validatePath(blockID); err != nil {
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
		region, exists := fs.files[blockID]
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

			diskPath := fs.diskPath(storageID, blockID)
			if diskPath == "" {
				return fmt.Errorf("file not found for %s %s", storageID, blockID)
			}
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
func (fs *TieredFs) Remove(storageID, blockID string) error {
	if atomic.LoadInt32(&fs.closed) == 1 {
		return ErrSystemClosed
	}

	if err := fs.validatePath(blockID); err != nil {
		return err
	}

	fs.mu.Lock()
	region, exists := fs.files[blockID]
	if exists {
		delete(fs.files, blockID)
	}
	fs.mu.Unlock()

	if exists {
		fs.freeManager.Free(region.Start, region.Size())
		fs.discardRegion(region)
	}

	fs.saveMetadata()

	// 删除磁盘文件（忽略错误）
	_path := fs.diskPath(storageID, blockID)
	if _path != "" {
		_ = os.Remove(_path)
	}
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

	err := fs.flushToTarget(region)
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
		if err := fs.flushToTarget(region); err != nil {
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
func (fs *TieredFs) Exists(strorageID, blockID string) bool {
	if atomic.LoadInt32(&fs.closed) == 1 {
		return false
	}

	fs.mu.RLock()
	_, exists := fs.files[blockID]
	fs.mu.RUnlock()

	if exists {
		return true
	} else {
		logger.GetLogger("dedups3").Debugf("file %s %s does not exist in %#v", strorageID, blockID, fs.files)
	}

	// 检查磁盘
	_path := fs.diskPath(strorageID, blockID)
	if _path != "" {
		_, err := os.Stat(_path)
		return err == nil
	}
	return false
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
	// 先移除 finalizer，防止重复调用
	runtime.SetFinalizer(fs, nil)

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

	fs.saveMetadata()

	// 最终同步
	if err := fs.SyncAll(); err != nil {
		errs = append(errs, fmt.Errorf("final sync: %w", err))
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	// 底层存储同步 - 确保 mmap 数据落地
	if fs.mmapData != nil {
		if err := unix.Msync(fs.mmapData, unix.MS_ASYNC); err != nil {
			errs = append(errs, fmt.Errorf("storage layer sync: %w", err))
		}
	}

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
func (fs *TieredFs) diskPath(storageID, blockID string) string {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	if fs.syncTargetor != nil && fs.syncTargetor[storageID] != nil {
		return fs.syncTargetor[storageID].BlockPath(blockID)
	}
	return ""
}

// flushToFile 将 mmap 区域刷入磁盘文件（调用者需持有锁）
// 最终修复版本
func (fs *TieredFs) flushToTarget(region *FileRegion) error {
	if region == nil {
		logger.GetLogger("boudler").Warnf("region is nil: %v", region)
		return nil
	}

	return utils.RetryCall(3, func() error {
		fs.mu.RLock()
		// 关键修复：验证区域归属
		currentRegion, exists := fs.files[region.BlockID]
		if !exists || !region.Equals(currentRegion) {
			fs.mu.RUnlock()
			logger.GetLogger("dedups3").Infof("skip flush req %#v %#v ", currentRegion, region)
			return nil
		}
		// 复制数据避免竞态
		data := make([]byte, region.Size())
		copy(data, fs.mmapData[region.Start:region.End])

		syncTargetor := fs.syncTargetor[region.StorageID]
		fs.mu.RUnlock()

		if syncTargetor != nil {
			// 取消正在上传的老版本任务
			fs.mu.Lock()
			if t := fs.targetTasks[region.BlockID]; t != nil {
				if t.ver < region.Ver && t.cancel != nil {
					logger.GetLogger("dedups3").Errorf("cancel old ver %d for block %s flush target task", t.ver, t.blockID)
					t.cancel()
				}
			}
			fs.mu.Unlock()

			// WithLockKey 避免同一个对象，多个routine在上传
			err := utils.WithLockKey(region.BlockID, func() error {
				ctx, cancel := context.WithCancel(context.Background())
				t := &TargetTask{
					storageID: region.StorageID,
					blockID:   region.BlockID,
					ver:       region.Ver,
					cancel:    cancel,
				}
				fs.mu.Lock()
				fs.targetTasks[region.BlockID] = t
				fs.mu.Unlock()
				defer delete(fs.targetTasks, region.BlockID)
				return syncTargetor.WriteBlockDirect(ctx, region.BlockID, data)
			})

			if err == nil {
				fs.mu.Lock()
				// 再次验证
				currentRegion, exists = fs.files[region.BlockID]
				if exists && region.Equals(currentRegion) {
					delete(fs.files, region.BlockID)
					fs.freeManager.Free(region.Start, region.Size())
					// 上传成功，释放 mmap 页面，避免写回磁盘
					fs.discardRegion(region)
				}
				fs.mu.Unlock()
			}
			return err
		} else {
			logger.GetLogger("dedups3").Debugf("no flush targer to do for block  %s ", region.BlockID)
			return fmt.Errorf("no flush targer to do for %s ", region.BlockID)
		}
	})
}

// discardRegion 在 flushToFile 成功后，释放 mmap 页面
func (fs *TieredFs) discardRegion(region *FileRegion) {
	start := region.Start & ^(int64(os.Getpagesize()) - 1) // 页对齐
	length := ((region.End - start + int64(os.Getpagesize()) - 1) / int64(os.Getpagesize())) * int64(os.Getpagesize())
	// 建议内核丢弃这部分内存（立即释放）
	_, _, errno := unix.Syscall(
		unix.SYS_MADVISE,
		uintptr(unsafe.Pointer(&fs.mmapData[start])),
		uintptr(length),
		uintptr(unix.MADV_DONTNEED),
	)
	if errno != 0 {
		logger.GetLogger("dedups3").Warnf("madvise MADV_DONTNEED failed: %v", errno)
	}
}
