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
package meta

import (
	"crypto/sha256"
	"encoding/hex"
	"time"
)

// Chunk 表示数据块
type Chunk struct {
	Hash         string `json:"hash"`          // 内容的SHA256哈希
	Size         int    `json:"size"`          // 块大小(字节)
	RefCount     int    `json:"ref_count"`     // 引用计数
	BlockID      string `json:"block_id"`      // 所属BlockID
	Data         []byte `json:"-"`             // 仅用于内存操作，不持久化
	LastAccessed int64  `json:"last_accessed"` // 最后访问时间(unix timestamp)
}

// NewChunk 从数据创建新块
func NewChunk(data []byte) *Chunk {
	size := len(data)
	if size == 0 {
		return nil
	}

	hash := calculateHash(data)
	return &Chunk{
		Hash:     hash,
		Size:     size,
		RefCount: 0,
		BlockID:  "",
		Data:     data,
	}
}

// calculateHash 计算数据的SHA256哈希
func calculateHash(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

// AddBlockRef 添加块引用
func (c *Chunk) SetBlockId(blockID string) {
	c.BlockID = blockID
	c.LastAccessed = time.Now().Unix()
}

// Touch 更新访问时间
func (c *Chunk) Touch() {
	c.LastAccessed = time.Now().Unix()
}
