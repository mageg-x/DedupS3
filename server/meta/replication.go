package meta

import (
	"encoding/xml"
	"errors"
	"strings"
	"time"
)

// ReplicationConfiguration 表示跨区域复制配置
type ReplicationConfiguration struct {
	XMLName xml.Name `xml:"ReplicationConfiguration"`
	Role    string   `xml:"Role"` // IAM角色ARN
	Rules   []Rule   `xml:"Rule"`

	CreatedAt time.Time `xml:"-"`
	UpdatedAt time.Time `xml:"-"`
}

// Rule 表示复制规则
type Rule struct {
	ID                        string                     `xml:"ID,omitempty"`
	Status                    string                     `xml:"Status"` // Enabled | Disabled
	Priority                  int                        `xml:"Priority,omitempty"`
	DeleteMarkerReplication   *DeleteMarkerReplication   `xml:"DeleteMarkerReplication,omitempty"`
	Filter                    *ReplFilter                `xml:"Filter,omitempty"`
	SourceSelectionCriteria   *SourceSelectionCriteria   `xml:"SourceSelectionCriteria,omitempty"`
	ExistingObjectReplication *ExistingObjectReplication `xml:"ExistingObjectReplication,omitempty"`
	Destination               Destination                `xml:"Destination"`
}

// ReplFilter 表示复制规则过滤条件
type ReplFilter struct {
	Prefix string             `xml:"Prefix,omitempty"`
	And    *FilterAndOperator `xml:"And,omitempty"`
	Tag    *ReplTag           `xml:"Tag,omitempty"`
}

// FilterAndOperator 表示AND条件
type FilterAndOperator struct {
	Prefix string    `xml:"Prefix,omitempty"`
	Tags   []ReplTag `xml:"Tag,omitempty"`
}

// ReplTag 表示对象标签
type ReplTag struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

// DeleteMarkerReplication 表示删除标记复制设置
type DeleteMarkerReplication struct {
	Status string `xml:"Status"` // Enabled | Disabled
}

// SourceSelectionCriteria 表示源对象选择标准
type SourceSelectionCriteria struct {
	SseKmsEncryptedObjects *SseKmsEncryptedObjects `xml:"SseKmsEncryptedObjects,omitempty"`
}

// SseKmsEncryptedObjects 表示SSE-KMS加密对象
type SseKmsEncryptedObjects struct {
	Status string `xml:"Status"` // Enabled | Disabled
}

// ExistingObjectReplication 表示现有对象复制
type ExistingObjectReplication struct {
	Status string `xml:"Status"` // Enabled | Disabled
}

// Destination 表示复制目标
type Destination struct {
	Bucket                   string                    `xml:"Bucket"` // 目标存储桶ARN
	StorageClass             string                    `xml:"StorageClass,omitempty"`
	EncryptionConfiguration  *EncryptionConfiguration  `xml:"EncryptionConfiguration,omitempty"`
	AccessControlTranslation *AccessControlTranslation `xml:"AccessControlTranslation,omitempty"`
	Account                  string                    `xml:"Account,omitempty"` // 目标账户ID
}

// EncryptionConfiguration 表示目标加密配置
type EncryptionConfiguration struct {
	ReplicaKmsKeyID string `xml:"ReplicaKmsKeyID"`
}

// AccessControlTranslation 表示访问控制翻译
type AccessControlTranslation struct {
	Owner string `xml:"Owner"` // Destination
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
