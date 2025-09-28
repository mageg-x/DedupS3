package fs

import (
	"sort"
	"sync"
)

// Region 表示内存区域
type Region struct {
	Start, End int64
}

// Size 返回区域大小
func (r Region) Size() int64 {
	return r.End - r.Start
}

// Contains 检查是否包含指定偏移
func (r Region) Contains(offset int64) bool {
	return offset >= r.Start && offset < r.End
}

// FileRegion 表示一个文件在 mmap 中的区域
type FileRegion struct {
	Region
	Ver  int32
	Path string
}

// FreeListManager 空闲空间管理器
type FreeListManager struct {
	mu        sync.RWMutex
	freeList  []Region
	totalSize int64
}

func (r *FileRegion) Equals(other *FileRegion) bool {
	if r == nil || other == nil {
		return r == other // 都为 nil 才相等
	}
	return r.Path == other.Path &&
		r.Start == other.Start &&
		r.End == other.End &&
		r.Ver == other.Ver
}

// NewFreeListManager 创建空闲空间管理器
func NewFreeListManager(totalSize int64) *FreeListManager {
	return &FreeListManager{
		freeList:  []Region{{0, totalSize}},
		totalSize: totalSize,
	}
}

// BestFitAlloc 最佳适应分配算法（优化版）
func (flm *FreeListManager) BestFitAlloc(size int64) (int64, error) {
	if size <= 0 {
		return 0, ErrInvalidSize
	}

	flm.mu.Lock()
	defer flm.mu.Unlock()

	bestIdx := -1
	bestSize := flm.totalSize + 1

	// 寻找最佳匹配
	for i, r := range flm.freeList {
		blockSize := r.Size()
		if blockSize >= size && blockSize < bestSize {
			bestIdx = i
			bestSize = blockSize

			// 如果找到完全匹配，直接使用
			if blockSize == size {
				break
			}
		}
	}

	if bestIdx == -1 {
		return 0, ErrSpaceExhausted
	}

	// 分配空间
	r := flm.freeList[bestIdx]
	offset := r.Start

	// 更新空闲块
	if r.Size() == size {
		// 完全使用，移除该块
		flm.freeList = append(flm.freeList[:bestIdx], flm.freeList[bestIdx+1:]...)
	} else {
		// 部分使用，更新剩余空间
		flm.freeList[bestIdx] = Region{r.Start + size, r.End}
	}

	return offset, nil
}

// Free 释放空间（优化版）
func (flm *FreeListManager) Free(offset, size int64) {
	if size <= 0 {
		return
	}

	flm.mu.Lock()
	defer flm.mu.Unlock()

	newRegion := Region{offset, offset + size}

	// 插入排序位置
	insertPos := sort.Search(len(flm.freeList), func(i int) bool {
		return flm.freeList[i].Start > newRegion.Start
	})

	// 插入新区域
	flm.freeList = append(flm.freeList, Region{})
	copy(flm.freeList[insertPos+1:], flm.freeList[insertPos:])
	flm.freeList[insertPos] = newRegion

	// 合并相邻区域
	flm.mergeAdjacent(insertPos)
}

// mergeAdjacent 合并相邻区域（需要调用者持有锁）
func (flm *FreeListManager) mergeAdjacent(pos int) {
	// 向前合并
	for pos > 0 && flm.freeList[pos-1].End == flm.freeList[pos].Start {
		flm.freeList[pos-1].End = flm.freeList[pos].End
		flm.freeList = append(flm.freeList[:pos], flm.freeList[pos+1:]...)
		pos--
	}

	// 向后合并
	for pos < len(flm.freeList)-1 && flm.freeList[pos].End == flm.freeList[pos+1].Start {
		flm.freeList[pos].End = flm.freeList[pos+1].End
		flm.freeList = append(flm.freeList[:pos+1], flm.freeList[pos+2:]...)
	}
}

// FreeSpace 获取总空闲空间
func (flm *FreeListManager) FreeSpace() int64 {
	flm.mu.RLock()
	defer flm.mu.RUnlock()

	var total int64
	for _, r := range flm.freeList {
		total += r.Size()
	}
	return total
}

// LargestFreeBlock 获取最大空闲块大小
func (flm *FreeListManager) LargestFreeBlock() int64 {
	flm.mu.RLock()
	defer flm.mu.RUnlock()

	var largest int64
	for _, r := range flm.freeList {
		if size := r.Size(); size > largest {
			largest = size
		}
	}
	return largest
}
