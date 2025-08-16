package meta

import (
	"encoding/xml"
	"errors"
	"time"
)

// Tagging 表示存储桶标签配置
type Tagging struct {
	XMLName xml.Name `xml:"Tagging"`
	TagSet  TagSet   `xml:"TagSet"`

	CreatedAt time.Time `xml:"-"`
	UpdatedAt time.Time `xml:"-"`
}

// TagSet 表示标签集合
type TagSet struct {
	Tags []Tag `xml:"Tag"`
}

// Tag 表示单个标签
type Tag struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

// AddTag 添加新标签
func (t *Tagging) AddTag(key, value string) error {
	if t == nil {
		return errors.New("tagging config not initialized")
	}

	if key == "" {
		return errors.New("tag key cannot be empty")
	}

	// 更新现有标签
	for i, tag := range t.TagSet.Tags {
		if tag.Key == key {
			t.TagSet.Tags[i].Value = value
			t.UpdatedAt = time.Now().UTC()
			return nil
		}
	}

	// 添加新标签
	t.TagSet.Tags = append(t.TagSet.Tags, Tag{Key: key, Value: value})
	t.UpdatedAt = time.Now().UTC()
	return nil
}

// RemoveTag 删除标签
func (t *Tagging) RemoveTag(key string) error {
	if t == nil {
		return errors.New("tagging config not initialized")
	}

	for i, tag := range t.TagSet.Tags {
		if tag.Key == key {
			t.TagSet.Tags = append(t.TagSet.Tags[:i], t.TagSet.Tags[i+1:]...)
			t.UpdatedAt = time.Now().UTC()
			return nil
		}
	}

	return errors.New("tag not found")
}

// GetTagValue 获取标签值
func (t *Tagging) GetTagValue(key string) (string, bool) {
	if t == nil {
		return "", false
	}

	for _, tag := range t.TagSet.Tags {
		if tag.Key == key {
			return tag.Value, true
		}
	}

	return "", false
}

// ToMap 转换为map
func (t *Tagging) ToMap() map[string]string {
	if t == nil {
		return nil
	}

	tags := make(map[string]string)
	for _, tag := range t.TagSet.Tags {
		tags[tag.Key] = tag.Value
	}
	return tags
}
