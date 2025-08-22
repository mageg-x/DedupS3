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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/mageg-x/boulder/internal/storage/block"
	"github.com/mageg-x/boulder/internal/storage/kv"

	"github.com/vmihailenco/msgpack/v5"
)

// BlockChunk 表示块中的一个块条目
type BlockChunk struct {
	Hash [32]byte `json:"hash" msgpack:"hash"` // 块哈希
	Size uint32   `json:"size" msgpack:"size"` // 块大小
	Data []byte   `json:"-" msgpack:"data"`    // 块数据（不在JSON中序列化）
}

// Block 表示存储块
type Block struct {
	ID        string       `json:"id" msgpack:"id"`                 // 唯一标识符
	TotalSize int64        `json:"total_size" msgpack:"total_size"` // 总大小(字节)
	ChunkList []BlockChunk `json:"chunk_list" msgpack:"chunk_list"` // 包含的块列表
	StorageID string       `json:"storage_id" msgpack:"storage_id"` // 存储后端ID
	Location  string       `json:"location" msgpack:"location"`     // 物理位置
	CreatedAt time.Time    `json:"created_at" msgpack:"created_at"` // 创建时间
	UpdatedAt time.Time    `json:"updated_at" msgpack:"updated_at"` // 更新时间
}

// NewBlock 创建新块
func NewBlock(storageID string) *Block {
	return &Block{
		ID:        generateUUID(),
		StorageID: storageID,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
}

// AddChunk 添加块到块
func (b *Block) AddChunk(hash [32]byte, data []byte) {
	chunk := BlockChunk{
		Hash: hash,
		Size: uint32(len(data)),
		Data: data,
	}
	b.ChunkList = append(b.ChunkList, chunk)
	b.TotalSize += int64(len(data))
	b.UpdatedAt = time.Now().UTC()
}

// ToMetaJSON 转换为元数据JSON格式
func (b *Block) ToMetaJSON() ([]byte, error) {
	// 创建元数据副本，移除ChunkList中的Data
	metaBlock := *b
	for i := range metaBlock.ChunkList {
		metaBlock.ChunkList[i].Data = nil
	}
	return json.Marshal(metaBlock)
}

// ToStorageFormat 转换为存储格式（MsgPack）
func (b *Block) ToStorageFormat() ([]byte, error) {
	return msgpack.Marshal(b)
}

// FromStorageFormat 从存储格式解析
func (b *Block) FromStorageFormat(data []byte) error {
	return msgpack.Unmarshal(data, b)
}

// GetChunk 获取特定块
func (b *Block) GetChunk(index int) (*BlockChunk, error) {
	if index < 0 || index >= len(b.ChunkList) {
		return nil, errors.New("chunk index out of range")
	}
	return &b.ChunkList[index], nil
}

// GetChunkByHash 根据哈希获取块
func (b *Block) GetChunkByHash(hash [32]byte) (*BlockChunk, error) {
	for _, chunk := range b.ChunkList {
		if chunk.Hash == hash {
			return &chunk, nil
		}
	}
	return nil, errors.New("chunk not found")
}

// WriteToStorage 写入到存储后端
func (b *Block) WriteToStorage(storage block.BlockStore) error {
	// 序列化完整块数据
	data, err := b.ToStorageFormat()
	if err != nil {
		return err
	}

	// 写入存储
	if err := storage.WriteBlock(b.ID, data); err != nil {
		return err
	}

	// 更新位置信息
	b.Location = storage.Location(b.ID)
	b.UpdatedAt = time.Now().UTC()

	return nil
}

// ReadFromStorage 从存储后端读取
func (b *Block) ReadFromStorage(storage block.BlockStore) error {
	// 读取块数据
	data, err := storage.ReadBlock(b.ID, 0, -1) // 读取整个块
	if err != nil {
		return err
	}

	// 解析块
	if err := b.FromStorageFormat(data); err != nil {
		return err
	}

	return nil
}

// SaveMeta 保存元数据到KV存储
func (b *Block) SaveMeta(kv kv.KVStore) error {
	metaData, err := b.ToMetaJSON()
	if err != nil {
		return err
	}

	return kv.Put(context.Background(), BlockMetaKey(b.ID), metaData)
}

// LoadMeta 从KV存储加载元数据
func (b *Block) LoadMeta(kv kv.KVStore, blockID string) error {
	exists, err := kv.Get(context.Background(), BlockMetaKey(blockID), b)
	if err != nil {
		return err
	}
	if !exists {
		return errors.New("block meta not found")
	}

	return nil
}

// BlockMetaKey 生成块元数据键
func BlockMetaKey(blockID string) string {
	return "block:" + blockID
}

// generateUUID 生成唯一ID
func generateUUID() string {
	// 实际实现中应使用真实的UUID生成
	return fmt.Sprintf("%x", time.Now().UnixNano())
}
