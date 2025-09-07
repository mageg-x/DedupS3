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
	"encoding/hex"
	"lukechampine.com/blake3"

	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/fastcdc"
)

// Chunk 表示数据块
type Chunk struct {
	Hash     string `json:"hash"`      // 内容的哈希
	Size     int32  `json:"size"`      // 块大小(字节)
	RefCount int32  `json:"ref_count"` // 引用计数
	BlockID  string `json:"block_id"`  // 所属BlockID
	Data     []byte `json:"-"`         // 仅用于内存操作，不持久化
}

// NewChunk 从数据创建新块
func NewChunk(data []byte) *Chunk {
	size := len(data)
	if size == 0 {
		return nil
	}

	c := Chunk{
		Size:     int32(size),
		RefCount: 0,
		BlockID:  "",
		Data:     make([]byte, size),
	}
	copy(c.Data, data)
	c.Hash = c.CalcChunkHash()
	return &c
}
func GenChunkKey(strorageID, Hash string) string {
	return "aws:chunk:" + strorageID + ":" + Hash
}

// CalcChunkHash 计算数据的哈希
func (c *Chunk) CalcChunkHash() string {
	fp := blake3.Sum256(c.Data)
	c.Hash = hex.EncodeToString(fp[:20])
	return c.Hash
}

// Clone 创建 Chunk 的深拷贝
// 该方法创建一个新的 Chunk 实例，并复制所有字段的值
// 特别注意：对于 Data 字段([]byte)，会创建新的切片并复制内容，确保是真正的深拷贝
func (c *Chunk) Clone() *Chunk {
	if c == nil {
		return nil
	}

	// 创建新的 Chunk 实例
	clone := &Chunk{
		Hash:     c.Hash,
		Size:     c.Size,
		RefCount: c.RefCount,
		BlockID:  c.BlockID,
	}

	// 深拷贝 Data 字段
	if c.Data != nil {
		clone.Data = make([]byte, len(c.Data))
		copy(clone.Data, c.Data)
	}

	return clone
}
