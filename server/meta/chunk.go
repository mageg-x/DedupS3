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
	"strconv"
)

// Chunk 表示数据块
type Chunk struct {
	Hash      string `json:"hash"`      // 内容的哈希
	ShortHash uint64 `json:"-"`         // 短hash辅助提高效率
	Size      int32  `json:"size"`      // 块大小(字节)
	RefCount  int32  `json:"ref_count"` // 引用计数
	BlockID   string `json:"block_id"`  // 所属BlockID
	Data      []byte `json:"-"`         // 仅用于内存操作，不持久化
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
		Data:     data,
	}
	c.CalcChunkHash()
	c.CalcShortHash()
	return &c
}

// AddBlockRef 添加块引用
func (c *Chunk) SetBlockId(blockID string) {
	c.BlockID = blockID
}

// CalcChunkHash 计算数据的哈希
func (c *Chunk) CalcChunkHash() string {
	fp := blake3.Sum256(c.Data)
	c.Hash = hex.EncodeToString(fp[:20])
	return c.Hash
}

func (c *Chunk) CalcShortHash() uint64 {
	c.ShortHash, _ = strconv.ParseUint(c.Hash[:16], 16, 64)
	return c.ShortHash
}
