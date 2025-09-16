package meta

import (
	"encoding/xml"
	"errors"
	"strings"
	"time"
)

// ReplicationConfiguration 表示跨区域复制配置
type ReplicationConfiguration struct {
	XMLName xml.Name `xml:"ReplicationConfiguration" json:"replicationConfiguration"`
	XMLNS   string   `xml:"xmlns,attr" json:"xmlns"` // 固定值为http://s3.amazonaws.com/doc/2006-03-01/
	Role    string   `xml:"Role" json:"role"`        // IAM角色ARN
	Rules   []Rule   `xml:"Rule" json:"rules"`

	CreatedAt time.Time `xml:"-" json:"createdAt"`
	UpdatedAt time.Time `xml:"-" json:"updatedAt"`
}

// Rule 表示复制规则
type Rule struct {
	ID                        string                     `xml:"ID,omitempty" json:"id"`
	Status                    string                     `xml:"Status" json:"status"` // Enabled | Disabled
	Priority                  int                        `xml:"Priority,omitempty" json:"priority"`
	DeleteMarkerReplication   *DeleteMarkerReplication   `xml:"DeleteMarkerReplication,omitempty" json:"deleteMarkerReplication"`
	Filter                    *ReplFilter                `xml:"Filter,omitempty" json:"filter"`
	SourceSelectionCriteria   *SourceSelectionCriteria   `xml:"SourceSelectionCriteria,omitempty" json:"sourceSelectionCriteria"`
	ExistingObjectReplication *ExistingObjectReplication `xml:"ExistingObjectReplication,omitempty" json:"existingObjectReplication"`
	Destination               Destination                `xml:"Destination" json:"destination"`
}

// ReplFilter 表示复制规则过滤条件
type ReplFilter struct {
	Prefix string             `xml:"Prefix,omitempty" json:"prefix"`
	And    *FilterAndOperator `xml:"And,omitempty" json:"and"`
	Tag    *ReplTag           `xml:"Tag,omitempty" json:"tag"`
}

// FilterAndOperator 表示AND条件
type FilterAndOperator struct {
	Prefix string    `xml:"Prefix,omitempty" json:"prefix"`
	Tags   []ReplTag `xml:"Tag,omitempty" json:"tags"`
}

// ReplTag 表示对象标签
type ReplTag struct {
	Key   string `xml:"Key" json:"key"`
	Value string `xml:"Value" json:"value"`
}

// DeleteMarkerReplication 表示删除标记复制设置
type DeleteMarkerReplication struct {
	Status string `xml:"Status" json:"status"` // Enabled | Disabled
}

// SourceSelectionCriteria 表示源对象选择标准
type SourceSelectionCriteria struct {
	SseKmsEncryptedObjects *SseKmsEncryptedObjects `xml:"SseKmsEncryptedObjects,omitempty" json:"sseKmsEncryptedObjects"`
}

// SseKmsEncryptedObjects 表示SSE-KMS加密对象
type SseKmsEncryptedObjects struct {
	Status string `xml:"Status" json:"status"` // Enabled | Disabled
}

// ExistingObjectReplication 表示现有对象复制
type ExistingObjectReplication struct {
	Status string `xml:"Status" json:"status"` // Enabled | Disabled
}

// Destination 表示复制目标
type Destination struct {
	Bucket                   string                    `xml:"Bucket" json:"bucket"` // 目标存储桶ARN
	StorageClass             string                    `xml:"StorageClass,omitempty" json:"storageClass"`
	EncryptionConfiguration  *EncryptionConfiguration  `xml:"EncryptionConfiguration,omitempty" json:"encryptionConfiguration"`
	AccessControlTranslation *AccessControlTranslation `xml:"AccessControlTranslation,omitempty" json:"accessControlTranslation"`
	Account                  string                    `xml:"Account,omitempty" json:"account"` // 目标账户ID
}

// EncryptionConfiguration 表示目标加密配置
type EncryptionConfiguration struct {
	ReplicaKmsKeyID string `xml:"ReplicaKmsKeyID" json:"replicaKmsKeyId"`
}

// AccessControlTranslation 表示访问控制翻译
type AccessControlTranslation struct {
	Owner string `xml:"Owner" json:"owner"` // Destination
}

// AddRule 添加新的复制规则
func (r *ReplicationConfiguration) AddRule(rule Rule) error {
	if r == nil {
		return errors.New("replication config not initialized")
	}

	if rule.ID == "" {
		return errors.New("rule ID is required")
	}

	if rule.Status != "Enabled" && rule.Status != "Disabled" {
		return errors.New("invalid rule status")
	}

	if rule.Destination.Bucket == "" {
		return errors.New("destination bucket is required")
	}

	// 检查规则ID是否唯一
	for _, existing := range r.Rules {
		if existing.ID == rule.ID {
			return errors.New("rule ID must be unique")
		}
	}

	r.Rules = append(r.Rules, rule)
	r.UpdatedAt = time.Now().UTC()
	return nil
}

// ShouldReplicate 检查对象是否应被复制
func (r *ReplicationConfiguration) ShouldReplicate(objectKey string, tags map[string]string, isSSEKMS bool, isDeleteMarker bool) (bool, Destination) {
	if r == nil || r.Role == "" {
		return false, Destination{}
	}

	// 按优先级排序规则
	rules := r.Rules
	// 实际实现中需要按优先级排序

	for _, rule := range rules {
		if rule.Status != "Enabled" {
			continue
		}

		// 检查过滤规则
		if rule.Filter != nil && !rule.Filter.Matches(objectKey, tags) {
			continue
		}

		// 检查源选择标准
		if rule.SourceSelectionCriteria != nil &&
			rule.SourceSelectionCriteria.SseKmsEncryptedObjects != nil {
			if rule.SourceSelectionCriteria.SseKmsEncryptedObjects.Status == "Enabled" && !isSSEKMS {
				continue
			}
			if rule.SourceSelectionCriteria.SseKmsEncryptedObjects.Status == "Disabled" && isSSEKMS {
				continue
			}
		}

		// 检查删除标记
		if isDeleteMarker && rule.DeleteMarkerReplication != nil &&
			rule.DeleteMarkerReplication.Status == "Disabled" {
			continue
		}

		// 检查现有对象复制
		if rule.ExistingObjectReplication != nil &&
			rule.ExistingObjectReplication.Status == "Disabled" {
			// 如果对象是已存在的（不是新上传的），则跳过
			// 实际实现中需要检查对象是否已存在
			// 这里简化处理
		}

		return true, rule.Destination
	}

	return false, Destination{}
}

// Matches 检查对象是否匹配过滤规则
func (f *ReplFilter) Matches(objectKey string, tags map[string]string) bool {
	if f == nil {
		return true // 没有过滤器表示匹配所有对象
	}

	// 检查前缀
	if f.Prefix != "" && !strings.HasPrefix(objectKey, f.Prefix) {
		return false
	}

	// 检查标签
	if f.Tag != nil {
		value, exists := tags[f.Tag.Key]
		if !exists || value != f.Tag.Value {
			return false
		}
	}

	// 检查AND条件
	if f.And != nil {
		if f.And.Prefix != "" && !strings.HasPrefix(objectKey, f.And.Prefix) {
			return false
		}

		for _, tag := range f.And.Tags {
			value, exists := tags[tag.Key]
			if !exists || value != tag.Value {
				return false
			}
		}
	}

	return true
}

// NewPrefixFilter 创建基于前缀的过滤器
func NewPrefixFilter(prefix string) *ReplFilter {
	return &ReplFilter{
		Prefix: prefix,
	}
}

// NewTagFilter 创建基于标签的过滤器
func NewTagFilter(key, value string) *ReplFilter {
	return &ReplFilter{
		Tag: &ReplTag{
			Key:   key,
			Value: value,
		},
	}
}

// NewAndFilter 创建复合AND过滤器
func NewAndFilter(prefix string, tags map[string]string) *ReplFilter {
	var tagList []ReplTag
	for k, v := range tags {
		tagList = append(tagList, ReplTag{Key: k, Value: v})
	}

	return &ReplFilter{
		And: &FilterAndOperator{
			Prefix: prefix,
			Tags:   tagList,
		},
	}
}
