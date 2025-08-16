package meta

import (
	"errors"
	"time"
)

// BucketQuota 表示存储桶配额
type BucketQuota struct {
	Type        string `json:"Type"` // Storage | ObjectCount
	SizeGB      int    `json:"SizeGB,omitempty"`
	ObjectCount int    `json:"ObjectCount,omitempty"`

	CreatedAt time.Time `json:"-"`
	UpdatedAt time.Time `json:"-"`
}

// SetStorageQuota 设置存储空间配额
func (q *BucketQuota) SetStorageQuota(sizeGB int) error {
	if q == nil {
		return errors.New("quota config not initialized")
	}

	if sizeGB <= 0 {
		return errors.New("quota size must be positive")
	}

	q.Type = "Storage"
	q.SizeGB = sizeGB
	q.ObjectCount = 0
	q.UpdatedAt = time.Now().UTC()
	return nil
}

// SetObjectCountQuota 设置对象数量配额
func (q *BucketQuota) SetObjectCountQuota(count int) error {
	if q == nil {
		return errors.New("quota config not initialized")
	}

	if count <= 0 {
		return errors.New("quota count must be positive")
	}

	q.Type = "ObjectCount"
	q.ObjectCount = count
	q.SizeGB = 0
	q.UpdatedAt = time.Now().UTC()
	return nil
}

// CheckQuota 检查是否超出配额
func (q *BucketQuota) CheckQuota(currentSizeGB float64, currentObjectCount int) error {
	if q == nil {
		return nil // 无配额限制
	}

	switch q.Type {
	case "Storage":
		if currentSizeGB > float64(q.SizeGB) {
			return errors.New("storage quota exceeded")
		}
	case "ObjectCount":
		if currentObjectCount > q.ObjectCount {
			return errors.New("object count quota exceeded")
		}
	}

	return nil
}

// IsStorageQuota 检查是否是存储空间配额
func (q *BucketQuota) IsStorageQuota() bool {
	return q != nil && q.Type == "Storage"
}

// IsObjectCountQuota 检查是否是对象数量配额
func (q *BucketQuota) IsObjectCountQuota() bool {
	return q != nil && q.Type == "ObjectCount"
}
