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

// Tagging 表示存储桶标签配置
type Tagging struct {
	XMLName xml.Name `xml:"Tagging" json:"tagging"`
	XMLNS   string   `xml:"xmlns,attr" json:"xmlns"` // 固定值为http://s3.amazonaws.com/doc/2006-03-01/
	TagSet  TagSet   `xml:"TagSet" json:"tagSet"`

	CreatedAt time.Time `xml:"-" json:"createdAt"`
	UpdatedAt time.Time `xml:"-" json:"updatedAt"`
}

// TagSet 表示标签集合
type TagSet struct {
	Tags []Tag `xml:"Tag" json:"tags"`
}

// Tag 表示单个标签
type Tag struct {
	Key   string `xml:"Key" json:"key"`
	Value string `xml:"Value" json:"value"`
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
