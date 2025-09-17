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
	"fmt"
	"strings"
	"time"
)

// EventNotificationConfiguration 表示事件通知配置
type EventNotificationConfiguration struct {
	XMLName xml.Name `xml:"NotificationConfiguration" json:"notificationConfiguration"`
	XMLNS   string   `xml:"xmlns,attr" json:"xmlns"` // 固定值为http://s3.amazonaws.com/doc/2006-03-01/

	TopicConfigurations      []TopicConfiguration      `xml:"TopicConfiguration,omitempty" json:"topicConfigurations"`
	QueueConfigurations      []QueueConfiguration      `xml:"QueueConfiguration,omitempty" json:"queueConfigurations"`
	LambdaConfigurations     []LambdaConfiguration     `xml:"CloudFunctionConfiguration,omitempty" json:"lambdaConfigurations"`
	EventBridgeConfiguration *EventBridgeConfiguration `xml:"EventBridgeConfiguration,omitempty" json:"eventBridgeConfiguration"`

	CreatedAt time.Time `xml:"-" json:"createdAt"`
	UpdatedAt time.Time `xml:"-" json:"updatedAt"`
}

// TopicConfiguration 表示SNS主题通知配置
type TopicConfiguration struct {
	ID       string       `xml:"Id,omitempty"`
	Events   []string     `xml:"Event"` // s3:ObjectCreated:*, s3:ObjectRemoved:*, etc.
	Filter   *EventFilter `xml:"Filter,omitempty"`
	TopicARN string       `xml:"Topic"`
}

// QueueConfiguration 表示SQS队列通知配置
type QueueConfiguration struct {
	ID       string       `xml:"Id,omitempty"`
	Events   []string     `xml:"Event"`
	Filter   *EventFilter `xml:"Filter,omitempty"`
	QueueARN string       `xml:"Queue"`
}

// LambdaConfiguration 表示Lambda函数通知配置
type LambdaConfiguration struct {
	ID                string       `xml:"Id,omitempty"`
	Events            []string     `xml:"Event"`
	Filter            *EventFilter `xml:"Filter,omitempty"`
	LambdaFunctionARN string       `xml:"CloudFunction"`
}

// EventBridgeConfiguration 表示EventBridge通知配置
type EventBridgeConfiguration struct {
	// EventBridge配置没有额外参数
}

// EventFilter 表示事件过滤规则
type EventFilter struct {
	S3Key S3KeyFilter `xml:"S3Key"`
}

// S3KeyFilter 表示对象键过滤规则
type S3KeyFilter struct {
	Rules []FilterRule `xml:"FilterRule"`
}

// FilterRule 表示单个过滤规则
type FilterRule struct {
	Name  string `xml:"Name"` // prefix | suffix
	Value string `xml:"Value"`
}

// AddTopicConfiguration 添加主题通知配置
func (e *EventNotificationConfiguration) AddTopicConfiguration(config TopicConfiguration) error {
	if e == nil {
		return errors.New("event config not initialized")
	}

	if config.TopicARN == "" {
		return errors.New("topic ARN is required")
	}

	if len(config.Events) == 0 {
		return errors.New("at least one event type is required")
	}

	e.TopicConfigurations = append(e.TopicConfigurations, config)
	e.UpdatedAt = time.Now().UTC()
	return nil
}

// AddQueueConfiguration 添加队列通知配置
func (e *EventNotificationConfiguration) AddQueueConfiguration(config QueueConfiguration) error {
	if e == nil {
		return errors.New("event config not initialized")
	}

	if config.QueueARN == "" {
		return errors.New("queue ARN is required")
	}

	if len(config.Events) == 0 {
		return errors.New("at least one event type is required")
	}

	e.QueueConfigurations = append(e.QueueConfigurations, config)
	e.UpdatedAt = time.Now().UTC()
	return nil
}

// AddLambdaConfiguration 添加Lambda通知配置
func (e *EventNotificationConfiguration) AddLambdaConfiguration(config LambdaConfiguration) error {
	if e == nil {
		return errors.New("event config not initialized")
	}

	if config.LambdaFunctionARN == "" {
		return errors.New("lambda ARN is required")
	}

	if len(config.Events) == 0 {
		return errors.New("at least one event type is required")
	}

	e.LambdaConfigurations = append(e.LambdaConfigurations, config)
	e.UpdatedAt = time.Now().UTC()
	return nil
}

// ShouldNotify 检查事件是否应触发通知
func (e *EventNotificationConfiguration) ShouldNotify(eventName, objectKey string) bool {
	if e == nil {
		return false
	}

	// 检查所有配置类型
	if checkConfigurations(e.TopicConfigurations, eventName, objectKey) {
		return true
	}

	if checkConfigurations(e.QueueConfigurations, eventName, objectKey) {
		return true
	}

	if checkConfigurations(e.LambdaConfigurations, eventName, objectKey) {
		return true
	}

	// EventBridge配置会发送所有事件
	if e.EventBridgeConfiguration != nil {
		return true
	}

	return false
}

// checkConfigurations 检查一组配置是否匹配事件
func checkConfigurations[T TopicConfiguration | QueueConfiguration | LambdaConfiguration](
	configs []T, eventName, objectKey string) bool {
	for _, config := range configs {
		// 检查事件类型
		eventMatch := false
		for _, e := range getEvents(config) {
			if e == eventName || e == "s3:*" || strings.HasSuffix(e, ":*") &&
				strings.HasPrefix(eventName, strings.TrimSuffix(e, ":*")) {
				eventMatch = true
				break
			}
		}
		if !eventMatch {
			continue
		}

		// 检查过滤规则
		filter := getFilter(config)
		if filter == nil || filter.Matches(objectKey) {
			return true
		}
	}
	return false
}

// getEvents 从配置中获取事件列表
func getEvents(config any) []string {
	switch c := config.(type) {
	case TopicConfiguration:
		return c.Events
	case QueueConfiguration:
		return c.Events
	case LambdaConfiguration:
		return c.Events
	}
	return nil
}

// getFilter 从配置中获取过滤器
func getFilter(config any) *EventFilter {
	switch c := config.(type) {
	case TopicConfiguration:
		return c.Filter
	case QueueConfiguration:
		return c.Filter
	case LambdaConfiguration:
		return c.Filter
	}
	return nil
}

// Matches 检查对象键是否匹配过滤规则
func (f *EventFilter) Matches(objectKey string) bool {
	if f == nil {
		return true
	}

	prefix := ""
	suffix := ""

	for _, rule := range f.S3Key.Rules {
		switch rule.Name {
		case "prefix":
			prefix = rule.Value
		case "suffix":
			suffix = rule.Value
		}
	}

	if prefix != "" && !strings.HasPrefix(objectKey, prefix) {
		return false
	}

	if suffix != "" && !strings.HasSuffix(objectKey, suffix) {
		return false
	}

	return true
}

// Validate 验证事件通知配置是否有效
func (e *EventNotificationConfiguration) Validate() error {
	if e == nil {
		return errors.New("event notification configuration is nil")
	}

	// 验证XML命名空间
	if e.XMLNS != "http://s3.amazonaws.com/doc/2006-03-01/" {
		return errors.New("invalid XML namespace")
	}

	// 验证各种配置的总数量不超过100个
	totalConfigs := len(e.TopicConfigurations) + len(e.QueueConfigurations) + len(e.LambdaConfigurations)
	if e.EventBridgeConfiguration != nil {
		totalConfigs++
	}

	if totalConfigs > 100 {
		return errors.New("event notification configuration cannot have more than 100 configurations")
	}

	// 定义有效的事件类型
	validEvents := map[string]bool{
		"s3:ObjectCreated:*":                               true,
		"s3:ObjectCreated:Put":                             true,
		"s3:ObjectCreated:Post":                            true,
		"s3:ObjectCreated:Copy":                            true,
		"s3:ObjectCreated:CompleteMultipartUpload":         true,
		"s3:ObjectRemoved:*":                               true,
		"s3:ObjectRemoved:Delete":                          true,
		"s3:ObjectRemoved:DeleteMarkerCreated":             true,
		"s3:ObjectRestore:*":                               true,
		"s3:ObjectRestore:Post":                            true,
		"s3:ObjectRestore:Completed":                       true,
		"s3:ObjectRestore:Delete":                          true,
		"s3:ReducedRedundancyLostObject":                   true,
		"s3:Replication:*":                                 true,
		"s3:Replication:OperationFailedReplication":        true,
		"s3:Replication:OperationMissedThreshold":          true,
		"s3:Replication:OperationReplicatedAfterThreshold": true,
		"s3:Replication:OperationNotTracked":               true,
		"s3:IntelligentTiering":                            true,
		"s3:Lifecycle:*":                                   true,
		"s3:Lifecycle:Expiration":                          true,
		"s3:Lifecycle:Transition":                          true,
		"s3:Lifecycle:NoncurrentVersionExpiration":         true,
		"s3:Lifecycle:NoncurrentVersionTransition":         true,
		"s3:Lifecycle:AbortIncompleteMultipartUpload":      true,
		"s3:ObjectTagging:*":                               true,
		"s3:ObjectTagging:Put":                             true,
		"s3:ObjectTagging:Delete":                          true,
	}

	// 验证TopicConfigurations
	for i, config := range e.TopicConfigurations {
		if err := validateNotificationConfig(config.ID, config.Events, config.TopicARN, config.Filter, i, "Topic", validEvents); err != nil {
			return err
		}
	}

	// 验证QueueConfigurations
	for i, config := range e.QueueConfigurations {
		if err := validateNotificationConfig(config.ID, config.Events, config.QueueARN, config.Filter, i, "Queue", validEvents); err != nil {
			return err
		}
	}

	// 验证LambdaConfigurations
	for i, config := range e.LambdaConfigurations {
		if err := validateNotificationConfig(config.ID, config.Events, config.LambdaFunctionARN, config.Filter, i, "Lambda", validEvents); err != nil {
			return err
		}
	}

	// EventBridgeConfiguration不需要额外验证

	return nil
}

// validateNotificationConfig 验证单个通知配置
func validateNotificationConfig(id string, events []string, arn string, filter *EventFilter, index int, configType string, validEvents map[string]bool) error {
	// 验证ID唯一性 - 在实际应用中需要在外部跟踪所有ID

	// 验证ARN不为空
	if arn == "" {
		return fmt.Errorf("%s ARN cannot be empty in %s configuration %d", configType, configType, index)
	}

	// 验证ARN格式（简单验证）
	if !strings.HasPrefix(arn, "arn:") {
		return fmt.Errorf("invalid %s ARN format in %s configuration %d", configType, configType, index)
	}

	// 验证事件列表不为空
	if len(events) == 0 {
		return fmt.Errorf("at least one event type is required in %s configuration %d", configType, index)
	}

	// 验证每个事件类型
	for _, event := range events {
		if !validEvents[event] {
			return fmt.Errorf("invalid event type '%s' in %s configuration %d", event, configType, index)
		}
	}

	// 验证Filter（如果存在）
	if filter != nil {
		// 验证S3Key过滤器
		if len(filter.S3Key.Rules) == 0 {
			return fmt.Errorf("S3Key filter must contain at least one rule in %s configuration %d", configType, index)
		}

		// 验证每个过滤规则
		for i, rule := range filter.S3Key.Rules {
			if rule.Name != "prefix" && rule.Name != "suffix" {
				return fmt.Errorf("invalid filter rule name '%s' in %s configuration %d, rule %d", rule.Name, configType, index, i)
			}

			// 验证规则值不为空
			if rule.Value == "" {
				return fmt.Errorf("filter rule value cannot be empty in %s configuration %d, rule %d", configType, index, i)
			}

			// 验证规则值长度不超过1024个字符
			if len(rule.Value) > 1024 {
				return fmt.Errorf("filter rule value cannot exceed 1024 characters in %s configuration %d, rule %d", configType, index, i)
			}
		}
	}

	return nil
}
