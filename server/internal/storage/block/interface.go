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
	"context"
	"errors"
	"fmt"
	xconf "github.com/mageg-x/boulder/internal/config"
	"github.com/mageg-x/boulder/internal/fs"
	"github.com/mageg-x/boulder/internal/utils"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/mageg-x/boulder/internal/logger"
)

var (
	ErrBlockNotFound = errors.New("block not found")
)

var (
	// 全局共享一个缓存文件系统
	mmfile       *fs.TieredFs = nil
	mmfileLocker sync.Mutex   = sync.Mutex{}
)

// BlockStore  存储后端接口
type BlockStore interface {
	Type() string
	WriteBlock(ctx context.Context, blockID string, data []byte, ver int32) error
	ReadBlock(location, blockID string, offset, length int64) ([]byte, error)
	DeleteBlock(blockID string) error
	List() (<-chan string, <-chan error)
	Location(blockID string) string
	BlockExists(blockID string) (bool, error)
}

type BaseBlockStore struct{}

func GetTieredFs() (*fs.TieredFs, error) {
	err := utils.WithLock(&mmfileLocker, func() error {
		if mmfile != nil {
			return nil
		}
		cfg := xconf.Get()

		// 创建配置
		config := &fs.Config{
			MmapSize:   2 << 30, // 2GB
			DiskDir:    filepath.Join(cfg.Node.LocalDir, "cache"),
			EnableSync: true,
		}

		// 创建TieredFs实例
		tfs, err := fs.NewTieredFs(config)

		if err != nil || tfs == nil {
			logger.GetLogger("boulder").Errorf("failed to create TieredFs: %v", err)
			return err
		}
		mmfile = tfs
		return nil
	})
	if err != nil {
		return nil, err
	}
	return mmfile, nil
}

// ReadBlockFromNode 从远程节点读取数据块
func (b *BaseBlockStore) ReadRemoteBlock(nodeURL string, blockID string, offset, size int64) ([]byte, error) {
	logger.GetLogger("boulder").Debugf("Reading block %s from node %s with offset=%d, size=%d", blockID, nodeURL, offset, size)

	// 构造请求URL，包含offset和size参数
	reqURL := fmt.Sprintf("%s/boulder/node/%s?readBlock=&offset=%d&size=%d", strings.TrimSuffix(nodeURL, "/"), blockID, offset, size)

	// 创建HTTP请求
	req, err := http.NewRequest("GET", reqURL, nil)
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to create request for block %s: %v", blockID, err)
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// 添加必要的头部
	req.Header.Set("x-amz-boulder-node-api", "read-block")

	// 发送请求
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		logger.GetLogger("boulder").Errorf("Request to node %s for block %s failed: %v", nodeURL, blockID, err)
		return nil, fmt.Errorf("request to node failed: %w", err)
	}
	defer resp.Body.Close()

	logger.GetLogger("boulder").Debugf("Request to node %s for block %s code %d", nodeURL, blockID, resp.StatusCode)
	// 检查响应状态码
	if resp.StatusCode == http.StatusNotFound {
		return nil, &types.NotFound{} // 直接返回 AWS SDK 类型
	}

	if resp.StatusCode != http.StatusOK {
		logger.GetLogger("boulder").Errorf("Node %s returned non-OK status %d for block %s", nodeURL, resp.StatusCode, blockID)
		return nil, fmt.Errorf("node returned non-OK status: %d", resp.StatusCode)
	}

	// 读取响应内容
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to read response body from node %s for block %s: %v", nodeURL, blockID, err)
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	// 检查数据是否为空
	if len(data) == 0 {
		logger.GetLogger("boulder").Errorf("Received empty data from node %s for block %s", nodeURL, blockID)
		return nil, fmt.Errorf("received empty data")
	}

	logger.GetLogger("boulder").Debugf("Successfully read block %s from node %s, size: %d bytes", blockID, nodeURL, len(data))
	return data, nil
}
