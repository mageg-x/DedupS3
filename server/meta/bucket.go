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
	"encoding/xml"
	"errors"
	"time"
)

type BucketMetadata struct {
	Name         string                          `json:"name" xml:"Name"`
	CreationDate time.Time                       `json:"creationDate" xml:"CreationDate"`
	Owner        Owner                           `json:"owner" xml:"Owner"`
	ACL          *ACL                            `json:"acl" xml:"AccessControlPolicy"`
	Location     string                          `json:"location" xml:"LocationConstraint"`
	Policy       *BucketPolicy                   `json:"policyConfig" xml:"Policy"`
	Notification *EventNotificationConfiguration `json:"notification" xml:"NotificationConfiguration"`
	Lifecycle    *LifecycleConfiguration         `json:"lifecycle" xml:"LifecycleConfiguration"`
	ObjectLock   *ObjectLockConfiguration        `json:"objectLock" xml:"ObjectLockConfiguration"`
	Versioning   *VersioningConfiguration        `json:"versioning" xml:"VersioningConfiguration"`
	SSE          *BucketSSEConfiguration         `json:"sse" xml:"ServerSideEncryptionConfiguration"`
	Tagging      *Tagging                        `json:"tags" xml:"Tagging"`
	Quota        *BucketQuota                    `json:"quota" xml:"Quota"`
	Replication  *ReplicationConfiguration       `json:"replication" xml:"ReplicationConfiguration"`
	Targets      *BucketTargets                  `json:"targets" xml:"Targets"`
}

func (bm *BucketMetadata) GenBucketKey() string {
	return "aws:bucket:" + bm.Owner.ID + ":" + bm.Name
}

// 设置桶策略
func (bm *BucketMetadata) SetPolicy(policy BucketPolicy) {
	bm.Policy = &policy
}

// 获取桶策略（安全访问）
func (bm *BucketMetadata) GetPolicy() (*BucketPolicy, error) {
	if bm.Policy == nil {
		return nil, errors.New("bucket policy not configured")
	}
	return bm.Policy, nil
}

// 启用版本控制
func (bm *BucketMetadata) EnableVersioning() {
	if bm.Versioning == nil {
		bm.Versioning = &VersioningConfiguration{}
	}
	bm.Versioning.Status = "Enabled"
}

// 添加生命周期规则
func (bm *BucketMetadata) AddLifecycleRule(rule LifecycleRule) {
	if bm.Lifecycle == nil {
		bm.Lifecycle = &LifecycleConfiguration{}
	}
	bm.Lifecycle.Rules = append(bm.Lifecycle.Rules, rule)
}

// 设置标签
func (bm *BucketMetadata) SetTags(tags Tagging) {
	bm.Tagging = &tags
}

// 获取标签值
func (bm *BucketMetadata) GetTag(key string) (string, bool) {
	if bm.Tagging == nil {
		return "", false
	}
	for _, tag := range bm.Tagging.TagSet.Tags {
		if tag.Key == key {
			return tag.Value, true
		}
	}
	return "", false
}

// 验证桶元数据有效性
func (bm *BucketMetadata) Validate() error {
	if bm.Name == "" {
		return errors.New("bucket name is required")
	}
	if bm.Location == "" {
		return errors.New("location constraint is required")
	}
	if bm.Owner.ID == "" {
		return errors.New("owner ID is required")
	}
	return nil
}

// 转换为AWS XML响应格式
func (bm *BucketMetadata) ToAWSXML() ([]byte, error) {
	type alias BucketMetadata // 防止递归序列化
	aux := struct {
		XMLName xml.Name `xml:"BucketMetadata"`
		*alias
	}{
		alias: (*alias)(bm),
	}
	return xml.MarshalIndent(aux, "", "  ")
}

// 从AWS XML解析桶元数据
func ParseFromAWSXML(data []byte) (*BucketMetadata, error) {
	var bm BucketMetadata
	if err := xml.Unmarshal(data, &bm); err != nil {
		return nil, err
	}
	return &bm, nil
}

// IsEncrypted 检查桶是否启用了默认加密
func (bm *BucketMetadata) IsEncrypted() bool {
	return bm.SSE != nil && bm.SSE.IsEnabled()
}

// EnableDefaultSSE 启用默认服务器端加密
func (bm *BucketMetadata) EnableDefaultSSE(algorithm, kmsKeyID string, bucketKeyEnabled bool) error {
	if bm.SSE == nil {
		bm.SSE = &BucketSSEConfiguration{}
	}
	return bm.SSE.ApplyDefaultEncryption(algorithm, kmsKeyID, bucketKeyEnabled)
}

// GetEncryptionAlgorithm 获取加密算法
func (bm *BucketMetadata) GetEncryptionAlgorithm() string {
	if bm.SSE == nil {
		return ""
	}
	return bm.SSE.Algorithm()
}

// IsBucketKeyEnabled 检查桶密钥是否启用
func (bm *BucketMetadata) IsBucketKeyEnabled() bool {
	return bm.SSE != nil && bm.SSE.IsBucketKeyEnabled()
}
