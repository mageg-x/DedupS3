// Package block /*
package block

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	xconf "github.com/mageg-x/boulder/internal/config"
	"github.com/mageg-x/boulder/internal/logger"
	"github.com/mageg-x/boulder/internal/utils"
)

// DiskStore 实现基于磁盘的存储后端
type DiskStore struct {
	BaseBlockStore
	conf          *xconf.DiskConfig
	pendingWrites sync.Map
	pwLocker      sync.Mutex
	mu            sync.RWMutex
	// 添加监控相关字段
	monitorStopChan chan struct{}
	monitorRunning  atomic.Bool
}

// NewDiskStore  创建新的磁盘存储
func NewDiskStore(c *xconf.DiskConfig) (*DiskStore, error) {
	if err := os.MkdirAll(c.Path, 0755); err != nil {
		logger.GetLogger("boulder").Errorf("failed to create disk store directory: %v", err)
		return nil, err
	}

	ds := &DiskStore{
		conf:            c,
		mu:              sync.RWMutex{},
		pendingWrites:   sync.Map{},
		pwLocker:        sync.Mutex{},
		monitorStopChan: make(chan struct{}),
		monitorRunning:  atomic.Bool{},
	}

	// 启动监控，每秒检查一次
	ds.StartMonitor(1 * time.Second)

	logger.GetLogger("boulder").Infof("Disk store initialized successfully")
	return ds, nil
}

// Type 返回存储类型
func (d *DiskStore) Type() string {
	return "disk"
}

// StartMonitor 添加监控方法
func (d *DiskStore) StartMonitor(interval time.Duration) {
	logger.GetLogger("boulder").Infof("DiskStore monitor started with interval %v", interval)
	if d.monitorRunning.Swap(true) {
		return // 已经在运行
	}

	d.monitorStopChan = make(chan struct{})

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		defer d.monitorRunning.Store(false)

		for {
			select {
			case <-ticker.C:
				// 使用 break 跳出 select，保持代码清晰
				break
			case <-d.monitorStopChan:
				return
			}

			var total int
			var zombies []*pendingWrite
			currentTime := time.Now().UTC()
			d.pendingWrites.Range(func(key, value interface{}) bool {
				total++
				pw := value.(*pendingWrite)
				// 检测僵尸任务：运行时间超过30秒的非活跃任务
				if currentTime.Sub(pw.startTime) > 30*time.Second {
					// 检查任务是否真的僵死了（context是否已取消）
					select {
					case <-pw.ctx.Done():
						// context已取消，确实是僵尸
						zombies = append(zombies, pw)
					default:
						// 只有writing 状态才耗时
						if pw.status != "writing" {
							zombies = append(zombies, pw) // 疑似僵尸
						} else {
							logger.GetLogger("boulder").Errorf("long time pengding writing routine for block %s", pw.blockID)
						}
					}
				}
				return true
			})

			// 打印监控信息
			logger.GetLogger("boulder").Errorf("[DiskStore pendingWrite] Total:%d, Zombies:%d", total, len(zombies))
			for _, _pw := range zombies {
				if _pw.cancel != nil {
					_pw.cancel()
				}
				d.pendingWrites.CompareAndDelete(_pw.blockID, _pw)
			}
		}
	}()
}

func (d *DiskStore) StopMonitor() {
	if d.monitorRunning.Load() {
		close(d.monitorStopChan)
	}
}

// WriteBlock 写入块到磁盘
func (d *DiskStore) WriteBlock(ctx context.Context, blockID string, data []byte, ver int32) error {
	return d.WriteBlockDelay(ctx, blockID, data, ver)
}

func (d *DiskStore) WriteBlockDelay(ctx context.Context, blockID string, data []byte, ver int32) error {
	logger.GetLogger("boulder").Debugf("[WriteBlockDelay] blockID=%s, ver=%d, size=%d KB", blockID, ver, len(data)/1024)

	var newPw *pendingWrite
	err := utils.WithLock(&d.pwLocker, func() error {
		// 尝试加载已存在的 pending 任务
		if old, loaded := d.pendingWrites.Load(blockID); loaded {
			pw := old.(*pendingWrite)
			// 如果新版本 >= 旧版本，取消旧写入
			if ver >= pw.ver {
				// 更新状态为取消
				pw.status = "canceled"
				pw.cancel() // 取消旧的写入 goroutine
			} else {
				// 新版本更老，直接丢弃（防止降级）
				logger.GetLogger("boulder").Debugf("[Disk WriteBlockDelay] discard older version, blockID=%s, newVer=%d, oldVer=%d", blockID, ver, pw.ver)
				return fmt.Errorf("older version discarded") // 明确返回错误
			}
		}

		// 创建新的可取消 context 用于控制延迟写入
		writeCtx, cancel := context.WithCancel(ctx)
		// 创建新的pendingWrite实例
		newPw = &pendingWrite{
			ctx:       writeCtx,
			cancel:    cancel,
			ver:       ver,
			startTime: time.Now().UTC(),
			blockID:   blockID,
			status:    "pending",
		}
		// 保存到map中
		d.pendingWrites.Store(blockID, newPw)
		return nil
	})

	if err != nil || newPw == nil {
		return err
	}

	// 启动延迟写入routine， 合并覆写情况
	go func(_pw *pendingWrite, _data []byte) {
		defer func() {
			// 更新状态并清理
			if r := recover(); r != nil {
				_pw.status = "panic"
				logger.GetLogger("boulder").Errorf("[Disk WriteBlockDelay] panic in goroutine, blockID=%s, error=%v", _pw.blockID, r)
			}
			if _pw.cancel != nil {
				_pw.cancel()
			}
			// 安全删除：只有当当前记录仍然是我们的pendingWrite时才删除
			d.pendingWrites.CompareAndDelete(_pw.blockID, _pw)
		}()
		// 更新状态为等待延迟
		_pw.status = "waiting_delay"

		// 非终结块，需延迟写
		if _pw.ver != 0x07FFFF {
			delayTime := 5000 * time.Millisecond
			// 等待 delayTime
			select {
			case <-time.After(delayTime):
				// 延迟结束，继续
				_pw.status = "delay_completed"
			case <-_pw.ctx.Done():
				_pw.status = "context_canceled"
				return // 被取消
			}
		}

		utils.WithLockKey(_pw.blockID, func() error {
			// 锁内立即二次检查 cancel
			select {
			case <-_pw.ctx.Done():
				_pw.status = "context_canceled"
				return nil
			default:
			}

			// 获取锁后， 并二次检查
			if old, loaded := d.pendingWrites.Load(_pw.blockID); loaded {
				curPw := old.(*pendingWrite)
				if curPw.ver > _pw.ver {
					_pw.status = "overridden_newer"
					return nil // 已被更高版本覆盖
				}
			} else {
				_pw.status = "already_processed"
				// 已经被其他goroutine处理了
				return nil
			}

			// 此时才安全执行写入
			_pw.status = "writing"
			if err := d.WriteBlockDirect(_pw.ctx, _pw.blockID, _data, _pw.ver); err != nil {
				_pw.status = "write_failed"
				logger.GetLogger("boulder").Errorf("[Disk WriteBlockDelay] blockID=%s, ver=%d, error=%v", _pw.blockID, _pw.ver, err)
				return err
			}
			_pw.status = "write_completed"
			logger.GetLogger("boulder").Debugf("[Disk WriteBlockDelay] blockID=%s, ver=%d", _pw.blockID, _pw.ver)
			return nil
		})
	}(newPw, data)

	return nil
}

func (d *DiskStore) WriteBlockDirect(ctx context.Context, blockID string, data []byte, ver int32) error {
	//d.mu.Lock()
	//defer d.mu.Unlock()
	logger.GetLogger("boulder").Debugf("[WriteBlockDirect] blockID=%s, ver=%d, size=%d KB", blockID, ver, len(data)/1024)
	path := d.blockPath(blockID)

	// 确保目录存在
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		logger.GetLogger("boulder").Errorf("failed to create directory for block %s: %v", blockID, err)
		return fmt.Errorf("failed to create directory for block %s: %w", blockID, err)
	}

	oldVer := int32(-1)
	if utils.FileExists(path) {
		if v, err := utils.ReadBlockVerFromFile(path); err == nil {
			oldVer = v
		}
	}
	if ver <= oldVer {
		return nil
	}

	// 创建新数据：4字节版本号 + 序列化数据
	versionBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(versionBuf, uint32(ver))

	// 使用临时文件写入，然后重命名，确保原子性
	tmpPath := path + ".tmp"
	if err := utils.WriteFile(tmpPath, [][]byte{versionBuf, data}, 0644); err != nil {
		logger.GetLogger("boulder").Errorf("failed to write block %s: %v", blockID, err)
		return fmt.Errorf("failed to write block %s: %w", blockID, err)
	}

	// 重命名临时文件为最终文件
	if err := os.Rename(tmpPath, path); err != nil {
		logger.GetLogger("boulder").Errorf("failed to commit block %s: %v", blockID, err)
		return fmt.Errorf("failed to commit block %s: %w", blockID, err)
	}

	logger.GetLogger("boulder").Debugf("Successfully wrote block: %s", blockID)
	return nil
}

func (d *DiskStore) ReadBlock(location, blockID string, offset, length int64) ([]byte, error) {
	cfg := xconf.Get()
	rLocation := strings.TrimSpace(location)
	lLocation := cfg.Node.LocalNode

	var err error
	var data []byte
	if rLocation != "" && rLocation != lLocation {
		data, err = d.ReadRemoteBlock(location, blockID, offset, length)
	} else {
		data, err = d.ReadLocalBlock(blockID, offset, length)
	}
	return data, err
}

// ReadLocalBlock 从磁盘读取块
func (d *DiskStore) ReadLocalBlock(blockID string, offset, length int64) ([]byte, error) {
	//d.mu.RLock()
	//defer d.mu.RUnlock()

	path := d.blockPath(blockID)
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			logger.GetLogger("boulder").Errorf("Block %s does not exist in %s", blockID, path)
			return nil, ErrBlockNotFound
		}
		logger.GetLogger("boulder").Errorf("failed to open block %s in %s : %v", blockID, path, err)
		return nil, fmt.Errorf("failed to open block %s in %s : %w", blockID, path, err)
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
	if offset < 0 || offset+4 >= fileInfo.Size() {
		logger.GetLogger("boulder").Errorf("Invalid offset %d for block %s of size %d", offset, blockID, fileInfo.Size())
		return nil, fmt.Errorf("invalid offset %d for block %s of size %d", offset, blockID, fileInfo.Size())
	}

	if offset+length+4 > fileInfo.Size() {
		length = fileInfo.Size() - offset - 4
	}

	if offset >= int64(0) {
		_, err = file.Seek(offset+4, io.SeekStart)
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
	//d.mu.Lock()
	//defer d.mu.Unlock()

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
	//d.mu.RLock()
	//defer d.mu.RUnlock()

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
