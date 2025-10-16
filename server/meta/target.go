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

// BucketTargets 表示存储桶目标配置
type BucketTargets struct {
	Targets []BucketTarget `json:"Targets"`

	CreatedAt time.Time `json:"-"`
	UpdatedAt time.Time `json:"-"`
}

// BucketTarget 表示单个存储桶目标
type BucketTarget struct {
	ID          string      `json:"ID"`
	Arn         string      `json:"Arn"`         // 目标ARN
	Endpoint    string      `json:"Endpoint"`    // 目标端点
	Bucket      string      `json:"Bucket"`      // 目标存储桶
	Region      string      `json:"Region"`      // 目标区域
	Credentials Credentials `json:"Credentials"` // 认证信息
	SyncState   string      `json:"SyncState"`   // Active | Inactive
}

// Credentials 表示认证信息
// 临时凭证结构
type Credentials struct {
	AccessKeyID  string    `json:"accessKeyId"`
	SecretKey    string    `json:"secretKey"`
	SessionToken string    `json:"sessionToken"`
	Expiration   time.Time `json:"expiration"`
}

// AddTarget 添加新的存储桶目标
func (b *BucketTargets) AddTarget(target BucketTarget) error {
	if b == nil {
		return errors.New("target config not initialized")
	}

	if target.ID == "" {
		return errors.New("target ID is required")
	}

	if target.Arn == "" {
		return errors.New("target ARN is required")
	}

	if target.Bucket == "" {
		return errors.New("target bucket is required")
	}

	if target.Region == "" {
		return errors.New("target region is required")
	}

	if target.Credentials.AccessKeyID == "" || target.Credentials.SecretKey == "" {
		return errors.New("credentials are required")
	}

	// 检查ID是否唯一
	for _, t := range b.Targets {
		if t.ID == target.ID {
			return errors.New("target ID must be unique")
		}
	}

	b.Targets = append(b.Targets, target)
	b.UpdatedAt = time.Now().UTC()
	return nil
}

// RemoveTarget 移除存储桶目标
func (b *BucketTargets) RemoveTarget(id string) error {
	if b == nil {
		return errors.New("target config not initialized")
	}

	for i, target := range b.Targets {
		if target.ID == id {
			b.Targets = append(b.Targets[:i], b.Targets[i+1:]...)
			b.UpdatedAt = time.Now().UTC()
			return nil
		}
	}

	return errors.New("target not found")
}

// GetTarget 获取存储桶目标
func (b *BucketTargets) GetTarget(id string) (BucketTarget, bool) {
	if b == nil {
		return BucketTarget{}, false
	}

	for _, target := range b.Targets {
		if target.ID == id {
			return target, true
		}
	}

	return BucketTarget{}, false
}

// ActivateTarget 激活存储桶目标
func (b *BucketTargets) ActivateTarget(id string) error {
	if b == nil {
		return errors.New("target config not initialized")
	}

	for i, target := range b.Targets {
		if target.ID == id {
			b.Targets[i].SyncState = "Active"
			b.UpdatedAt = time.Now().UTC()
			return nil
		}
	}

	return errors.New("target not found")
}

// DeactivateTarget 停用存储桶目标
func (b *BucketTargets) DeactivateTarget(id string) error {
	if b == nil {
		return errors.New("target config not initialized")
	}

	for i, target := range b.Targets {
		if target.ID == id {
			b.Targets[i].SyncState = "Inactive"
			b.UpdatedAt = time.Now().UTC()
			return nil
		}
	}

	return errors.New("target not found")
}
