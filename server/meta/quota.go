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
