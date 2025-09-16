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
	"strings"
	"time"
)

// LifecycleConfiguration 表示生命周期配置
type LifecycleConfiguration struct {
	XMLName xml.Name        `xml:"LifecycleConfiguration" json:"lifecycleConfiguration"`
	XMLNS   string          `xml:"xmlns,attr" json:"xmlns"` // 固定值为http://s3.amazonaws.com/doc/2006-03-01/
	Rules   []LifecycleRule `xml:"Rule" json:"rules"`

	CreatedAt time.Time `xml:"-" json:"createdAt"`
	UpdatedAt time.Time `xml:"-" json:"updatedAt"`
}

// LifecycleRule 表示生命周期规则
type LifecycleRule struct {
	ID                             string                          `xml:"ID,omitempty" json:"id"`
	Status                         string                          `xml:"Status" json:"status"` // Enabled | Disabled
	Filter                         *LifeCycleFilter                `xml:"Filter" json:"filter"`
	Expiration                     *Expiration                     `xml:"Expiration,omitempty" json:"expiration"`
	Transition                     *Transition                     `xml:"Transition,omitempty" json:"transition"`
	NoncurrentVersionExpiration    *NoncurrentVersionExpiration    `xml:"NoncurrentVersionExpiration,omitempty" json:"noncurrentVersionExpiration"`
	NoncurrentVersionTransition    *NoncurrentVersionTransition    `xml:"NoncurrentVersionTransition,omitempty" json:"noncurrentVersionTransition"`
	AbortIncompleteMultipartUpload *AbortIncompleteMultipartUpload `xml:"AbortIncompleteMultipartUpload,omitempty" json:"abortIncompleteMultipartUpload"`
}

// LifeCycleFilter 表示对象过滤规则
type LifeCycleFilter struct {
	Prefix string        `xml:"Prefix,omitempty" json:"prefix"`
	And    *AndOperator  `xml:"And,omitempty" json:"and"`
	Tag    *LifeCycleTag `xml:"Tag,omitempty" json:"tag"`
}

// AndOperator 表示AND条件
type AndOperator struct {
	Prefix string         `xml:"Prefix,omitempty" json:"prefix"`
	Tags   []LifeCycleTag `xml:"Tag,omitempty" json:"tags"`
}

// LifeCycleTag 表示对象标签
type LifeCycleTag struct {
	Key   string `xml:"Key" json:"key"`
	Value string `xml:"Value" json:"value"`
}

// Expiration 表示对象过期规则
type Expiration struct {
	Date                      *time.Time `xml:"Date,omitempty" json:"date"`
	Days                      int        `xml:"Days,omitempty" json:"days"`
	ExpiredObjectDeleteMarker bool       `xml:"ExpiredObjectDeleteMarker,omitempty" json:"expiredObjectDeleteMarker"`
}

// Transition 表示存储类型转换规则
type Transition struct {
	Date         *time.Time `xml:"Date,omitempty" json:"date"`
	Days         int        `xml:"Days,omitempty" json:"days"`
	StorageClass string     `xml:"StorageClass" json:"storageClass"`
}

// NoncurrentVersionExpiration 表示非当前版本过期规则
type NoncurrentVersionExpiration struct {
	NoncurrentDays int `xml:"NoncurrentDays" json:"noncurrentDays"`
}

// NoncurrentVersionTransition 表示非当前版本转换规则
type NoncurrentVersionTransition struct {
	NoncurrentDays int    `xml:"NoncurrentDays" json:"noncurrentDays"`
	StorageClass   string `xml:"StorageClass" json:"storageClass"`
}

// AbortIncompleteMultipartUpload 表示分段上传中止规则
type AbortIncompleteMultipartUpload struct {
	DaysAfterInitiation int `xml:"DaysAfterInitiation" json:"daysAfterInitiation"`
}

// AddRule 添加新的生命周期规则
func (l *LifecycleConfiguration) AddRule(rule LifecycleRule) error {
	if l == nil {
		return errors.New("lifecycle config not initialized")
	}

	if rule.ID == "" {
		return errors.New("rule ID is required")
	}

	if rule.Status != "Enabled" && rule.Status != "Disabled" {
		return errors.New("invalid rule status")
	}

	if rule.Filter == nil {
		return errors.New("filter is required")
	}

	// 检查规则冲突
	for _, r := range l.Rules {
		if r.ID == rule.ID {
			return errors.New("rule ID must be unique")
		}
	}

	l.Rules = append(l.Rules, rule)
	l.UpdatedAt = time.Now().UTC()
	return nil
}

// CheckExpiration 检查对象是否应过期
func (l *LifecycleConfiguration) CheckExpiration(objectKey string, tags map[string]string, createTime time.Time, isDeleteMarker bool) (bool, time.Time) {
	if l == nil {
		return false, time.Time{}
	}

	now := time.Now().UTC()

	for _, rule := range l.Rules {
		if rule.Status != "Enabled" {
			continue
		}

		if !rule.Filter.Matches(objectKey, tags) {
			continue
		}

		if rule.Expiration != nil {
			// 处理删除标记过期
			if isDeleteMarker && rule.Expiration.ExpiredObjectDeleteMarker {
				return true, now
			}

			// 基于天数的过期
			if rule.Expiration.Days > 0 {
				expirationTime := createTime.AddDate(0, 0, rule.Expiration.Days)
				if now.After(expirationTime) {
					return true, expirationTime
				}
			}

			// 基于日期的过期
			if rule.Expiration.Date != nil && now.After(*rule.Expiration.Date) {
				return true, *rule.Expiration.Date
			}
		}
	}

	return false, time.Time{}
}

// CheckTransition 检查对象是否应转换存储类型
func (l *LifecycleConfiguration) CheckTransition(objectKey string, tags map[string]string, createTime time.Time) (bool, string, time.Time) {
	if l == nil {
		return false, "", time.Time{}
	}

	now := time.Now().UTC()

	for _, rule := range l.Rules {
		if rule.Status != "Enabled" {
			continue
		}

		if !rule.Filter.Matches(objectKey, tags) {
			continue
		}

		if rule.Transition != nil {
			// 基于天数的转换
			if rule.Transition.Days > 0 {
				transitionTime := createTime.AddDate(0, 0, rule.Transition.Days)
				if now.After(transitionTime) {
					return true, rule.Transition.StorageClass, transitionTime
				}
			}

			// 基于日期的转换
			if rule.Transition.Date != nil && now.After(*rule.Transition.Date) {
				return true, rule.Transition.StorageClass, *rule.Transition.Date
			}
		}
	}

	return false, "", time.Time{}
}

// Matches 检查对象是否匹配过滤规则
func (f *LifeCycleFilter) Matches(objectKey string, tags map[string]string) bool {
	if f == nil {
		return false
	}

	if f.Prefix != "" {
		if !strings.HasPrefix(objectKey, f.Prefix) {
			return false
		}
	}

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

	if f.Tag != nil {
		value, exists := tags[f.Tag.Key]
		if !exists || value != f.Tag.Value {
			return false
		}
	}

	return true
}
