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
	"time"

	"github.com/google/uuid"
)

// BlockChunk 表示块中的一个块条目
type BlockChunk struct {
	Hash uint64 `json:"hash" msgpack:"hash"` // 块哈希
	Size int32  `json:"size" msgpack:"size"` // 块大小
}

// BlockData ，存放在磁盘上
type BlockData struct {
	ID        string       `json:"id" msgpack:"id"`                 // 唯一标识符
	TotalSize int64        `json:"total_size" msgpack:"total_size"` // 总大小(字节)
	ChunkList []BlockChunk `json:"chunklist" msgpack:"chunklist"`   // chunk 索引
	Data      []byte       `json:"data" msgpack:"data"`             //数据块
}

// Block 表示存储块, 存在元数据中
type Block struct {
	ID        string    `json:"id" msgpack:"id"`                 // 唯一标识符
	TotalSize int64     `json:"total_size" msgpack:"total_size"` // 总大小(字节)
	ChunkList []Chunk   `json:"chunk_list" msgpack:"chunk_list"` // 包含的块列表
	StorageID string    `json:"storage_id" msgpack:"storage_id"` // 存储后端ID
	CreatedAt time.Time `json:"created_at" msgpack:"created_at"` // 创建时间
	UpdatedAt time.Time `json:"updated_at" msgpack:"updated_at"` // 更新时间
}

// NewBlock 创建新块
func NewBlock(storageID string) *Block {
	return &Block{
		ID:        GenBlockID(),
		StorageID: storageID,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
}

func GenBlockID() string {
	return uuid.New().String()
}
