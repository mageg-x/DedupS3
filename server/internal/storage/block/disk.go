// Package block /*
package block

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/mageg-x/boulder/internal/utils"
	"os"
	"path/filepath"
	"strings"
	"sync"

	xconf "github.com/mageg-x/boulder/internal/config"
	"github.com/mageg-x/boulder/internal/logger"
)

// DiskStore 实现基于磁盘的存储后端
type DiskStore struct {
	BaseBlockStore
	conf *xconf.DiskConfig
	mu   sync.RWMutex
}

// NewDiskStore  创建新的磁盘存储
func NewDiskStore(c *xconf.DiskConfig) (*DiskStore, error) {
	if err := os.MkdirAll(c.Path, 0755); err != nil {
		logger.GetLogger("boulder").Errorf("failed to create disk store directory: %v", err)
		return nil, err
	}

	ds := &DiskStore{
		conf: c,
		mu:   sync.RWMutex{},
	}

	vfile, err := GetTieredFs()
	if err == nil && vfile != nil {
		vfile.AddSyncTarget(ds)
	} else {
		return nil, fmt.Errorf("failed to get tiered fs: %w", err)
	}
	logger.GetLogger("boulder").Infof("Disk store initialized successfully")
	return ds, nil
}

// Type 返回存储类型
func (d *DiskStore) Type() string {
	return "disk"
}

func (d *DiskStore) WriteBlock(ctx context.Context, blockID string, data []byte, ver int32) error {
	//d.mu.Lock()
	//defer d.mu.Unlock()
	logger.GetLogger("boulder").Debugf("[DiskStore WriteBlock] blockID=%s, ver=%d, size=%d KB", blockID, ver, len(data)/1024)

	vfile, err := GetTieredFs()
	if err != nil || vfile == nil {
		logger.GetLogger("boulder").Errorf("failed to get tiered fs: %v", err)
		return fmt.Errorf("failed to get tiered fs: %v", err)
	}

	oldVer := int32(-1)
	if vfile.Exists(blockID) {
		if v, err := vfile.ReadFile(blockID, 0, 4); err == nil && v != nil {
			oldVer = int32(binary.BigEndian.Uint32(v[:]))
			logger.GetLogger("boulder").Debugf("get block %s old ver %d", blockID, oldVer)
		}
	}

	if ver <= oldVer {
		return nil
	}

	// 创建新数据：4字节版本号 + 序列化数据
	versionBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(versionBuf, uint32(ver))

	if err := vfile.WriteFile(blockID, [][]byte{versionBuf, data}, ver); err != nil {
		logger.GetLogger("boulder").Errorf("failed to write block %s: %v", blockID, err)
		return fmt.Errorf("failed to write block %s: %w", blockID, err)
	}

	logger.GetLogger("boulder").Debugf("Successfully wrote block: %s", blockID)
	return nil
}

func (s *DiskStore) WriteBlockDirect(ctx context.Context, blockID string, data []byte) error {
	logger.GetLogger("boulder").Debugf("[DiskStore WriteBlockDirect] blockID=%s", blockID)
	path := s.BlockPath(blockID)
	// 确保目录存在
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("create dir %s failed: %w", filepath.Dir(path), err)
	}

	// 原子写入
	tmpPath := path + ".tmp"

	if err := utils.WriteFile(tmpPath, [][]byte{data}, 0655); err != nil {
		return fmt.Errorf("write %s data failed: %w", path, err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("rename file: %w", err)
	}

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

// ReadLocalBlock   从磁盘读取块
func (d *DiskStore) ReadLocalBlock(blockID string, offset, length int64) ([]byte, error) {
	//d.mu.RLock()
	//defer d.mu.RUnlock()

	vfile, err := GetTieredFs()
	if err != nil || vfile == nil {
		logger.GetLogger("boulder").Errorf("failed to get tiered fs: %v", err)
		return nil, fmt.Errorf("failed to get tiered fs: %v", err)
	}

	if !vfile.Exists(blockID) {
		logger.GetLogger("boulder").Errorf("Block %s does not exist", blockID)
		return nil, ErrBlockNotFound
	}

	data, err := vfile.ReadFile(blockID, offset+4, length)
	if err != nil || data == nil {
		logger.GetLogger("boulder").Errorf("failed to read block %s: %v", blockID, err)
		return nil, fmt.Errorf("failed to read block %s: %w", blockID, err)
	}

	logger.GetLogger("boulder").Debugf("Successfully read block: %s, read %d bytes", blockID, len(data))
	return data, nil
}

// DeleteBlock 删除块
func (d *DiskStore) DeleteBlock(blockID string) error {
	//d.mu.Lock()
	//defer d.mu.Unlock()

	vfile, err := GetTieredFs()
	if err != nil || vfile == nil {
		logger.GetLogger("boulder").Errorf("failed to get tiered fs: %v", err)
		return fmt.Errorf("failed to get tiered fs: %v", err)
	}

	if err := vfile.Remove(blockID); err != nil {
		if !vfile.Exists(blockID) {
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

	vfile, err := GetTieredFs()
	if err != nil || vfile == nil {
		logger.GetLogger("boulder").Errorf("failed to get tiered fs: %v", err)
		return false, fmt.Errorf("failed to get tiered fs: %v", err)
	}

	if !vfile.Exists(blockID) {
		logger.GetLogger("boulder").Debugf("Block %s does not exist", blockID)
		return false, nil
	}

	return true, nil
}

// Location 获取块位置
func (d *DiskStore) Location(blockID string) string {
	return d.BlockPath(blockID)
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
func (d *DiskStore) BlockPath(blockID string) string {
	n := len(blockID)
	dir1 := blockID[n-3:]      // 最后3位
	dir2 := blockID[n-6 : n-3] // 倒数第4-6位
	dir3 := blockID[n-9 : n-6] // 倒数第7-9位
	path := filepath.Join(d.conf.Path, dir1, dir2, dir3, blockID)
	// 转换绝对路径
	path, _ = filepath.Abs(path)
	return path
}
