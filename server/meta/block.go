// Package meta /*
package meta

import (
	"time"

	"lukechampine.com/blake3"

	"github.com/mageg-x/boulder/internal/utils"
)

// BlockChunk 表示块中的一个块条目
type BlockChunk struct {
	Hash string `json:"hash" msgpack:"hash"` // 块哈希
	Size int32  `json:"size" msgpack:"size"` // 块大小
	Data []byte `json:"-" msgpack:"-"`       // 仅用于内存操作，不持久化
}

// BlockHeader BlockData ，存放在磁盘上只包含头信息，不含 Data
type BlockHeader struct {
	ID         string       `json:"id" msgpack:"id"`
	Etag       [32]byte     `json:"etag" msgpack:"etag"`
	TotalSize  int64        `json:"total_size" msgpack:"total_size"`
	RealSize   int64        `json:"real_size" msgpack:"real_size"`   // 实际占用大小
	Compressed bool         `json:"compressed" msgpack:"compressed"` // 是否压缩
	Encrypted  bool         `json:"encrypted" msgpack:"encrypted"`   // 是否加密
	ChunkList  []BlockChunk `json:"chunk_list" msgpack:"chunk_list"` // 块列表
}

// BlockData BlockData: 完整结构（包含 Data）
type BlockData struct {
	BlockHeader
	Data []byte `json:"data"  msgpack:"data"`
}

// Block 表示存储块元数据, 存在元数据中
type Block struct {
	BlockHeader
	StorageID string    `json:"storage_id" msgpack:"storage_id"` // 存储后端ID
	CreatedAt time.Time `json:"created_at" msgpack:"created_at"` // 创建时间
	UpdatedAt time.Time `json:"updated_at" msgpack:"updated_at"` // 更新时间
	Dirty     bool      `json:"-" msgpack:"-" default:"true"`    // 是否变更数据
}

// NewBlock 创建新块
func NewBlock(storageID string) *Block {
	return &Block{
		BlockHeader: BlockHeader{
			ID: GenBlockID(),
		},
		StorageID: storageID,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
}

func GenBlockID() string {
	return utils.GenUUID()
}
func GenBlockKey(storageID, blockID string) string {
	return "aws:block:" + storageID + ":" + blockID
}

// Clone 创建 Block 的深拷贝
// 该方法创建一个新的 Block 实例，并复制所有字段的值
// 对于引用类型字段（如 ChunkList），会创建新的切片并复制其中的元素
func (b *Block) Clone(cloneData bool) *Block {
	cp := &Block{}
	*cp = *b // 浅拷贝
	cp.ChunkList = make([]BlockChunk, len(b.ChunkList))

	// 复制 ChunkList 中的每个元素
	for i, chunk := range b.ChunkList {
		newChunk := chunk
		if chunk.Data != nil && cloneData {
			newChunk.Data = make([]byte, len(chunk.Data))
			copy(newChunk.Data, chunk.Data)
		}
		cp.ChunkList[i] = newChunk
	}

	return cp
}

// CalcChunkHash 计算数据的哈希
func (b *BlockData) CalcChunkHash() {
	b.Etag = blake3.Sum256(b.Data)
}
