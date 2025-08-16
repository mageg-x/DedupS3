package meta

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"regexp"
	"strings"
	"time"
)

// BucketPolicy 表示完整的存储桶策略
type BucketPolicy struct {
	ID         string      `json:"Id,omitempty"` // 策略ID
	Version    string      `json:"Version"`      // 策略版本
	Statements []Statement `json:"Statement"`    // 策略声明
	CreatedAt  time.Time   `json:"CreatedAt"`    // 创建时间
	UpdatedAt  time.Time   `json:"UpdatedAt"`    // 更新时间
}

// Statement 表示策略中的单个声明
type Statement struct {
	Sid          string         `json:"Sid,omitempty"`          // 声明ID
	Effect       string         `json:"Effect"`                 // 允许(Allow)或拒绝(Deny)
	Principal    Principal      `json:"Principal"`              // 授权主体
	NotPrincipal Principal      `json:"NotPrincipal,omitempty"` // 排除主体
	Action       StringOrArray  `json:"Action"`                 // 操作类型
	NotAction    StringOrArray  `json:"NotAction,omitempty"`    // 排除操作
	Resource     StringOrArray  `json:"Resource"`               // 资源路径
	NotResource  StringOrArray  `json:"NotResource,omitempty"`  // 排除资源
	Condition    ConditionBlock `json:"Condition,omitempty"`    // 条件块
}

// Principal 表示策略主体
type Principal struct {
	AWS           StringOrArray `json:"AWS,omitempty"`           // AWS账户
	Federated     StringOrArray `json:"Federated,omitempty"`     // 联合身份
	Service       StringOrArray `json:"Service,omitempty"`       // AWS服务
	CanonicalUser StringOrArray `json:"CanonicalUser,omitempty"` // 规范用户ID
}

// ConditionBlock 表示条件块
type ConditionBlock map[string]map[string]StringOrArray

// StringOrArray 可以是字符串或字符串数组
type StringOrArray []string

// UnmarshalJSON 自定义JSON解析
func (s *StringOrArray) UnmarshalJSON(data []byte) error {
	if data[0] == '[' {
		var arr []string
		if err := json.Unmarshal(data, &arr); err != nil {
			return err
		}
		*s = arr
		return nil
	}

	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	*s = []string{str}
	return nil
}

// IsAllowed 检查给定主体对资源执行操作是否被允许
func (p *BucketPolicy) IsAllowed(principalARN, action, resource string, context map[string]string) (bool, error) {
	if p == nil {
		return false, errors.New("policy not configured")
	}

	// 默认拒绝
	result := false

	for _, stmt := range p.Statements {
		// 检查是否匹配声明
		matches, err := stmt.Matches(principalARN, action, resource, context)
		if err != nil {
			return false, err
		}

		if !matches {
			continue
		}

		// 处理显式拒绝
		if stmt.Effect == "Deny" {
			return false, nil
		}

		// 处理允许
		if stmt.Effect == "Allow" {
			result = true
		}
	}

	return result, nil
}

// Matches 检查声明是否匹配给定参数
func (s *Statement) Matches(principalARN, action, resource string, context map[string]string) (bool, error) {
	// 检查主体
	if !s.principalMatches(principalARN) {
		return false, nil
	}

	// 检查操作
	if !s.actionMatches(action) {
		return false, nil
	}

	// 检查资源
	if !s.resourceMatches(resource) {
		return false, nil
	}

	// 检查条件
	if !s.conditionMatches(context) {
		return false, nil
	}

	return true, nil
}

// principalMatches 检查主体匹配
func (s *Statement) principalMatches(principalARN string) bool {
	// 处理NotPrincipal
	if len(s.NotPrincipal.AWS) > 0 {
		for _, np := range s.NotPrincipal.AWS {
			if matchARN(np, principalARN) {
				return false
			}
		}
	}

	// 如果Principal为空，则匹配所有主体
	if len(s.Principal.AWS) == 0 &&
		len(s.Principal.Federated) == 0 &&
		len(s.Principal.Service) == 0 &&
		len(s.Principal.CanonicalUser) == 0 {
		return true
	}

	// 检查AWS主体
	for _, p := range s.Principal.AWS {
		if matchARN(p, principalARN) {
			return true
		}
	}

	// 其他主体类型检查（简化实现）
	// 实际中需要检查Federated、Service和CanonicalUser

	return false
}

// actionMatches 检查操作匹配
func (s *Statement) actionMatches(action string) bool {
	// 处理NotAction
	for _, na := range s.NotAction {
		if matchAction(na, action) {
			return false
		}
	}

	// 如果Action为空，则匹配所有操作
	if len(s.Action) == 0 {
		return true
	}

	// 检查允许的操作
	for _, a := range s.Action {
		if matchAction(a, action) {
			return true
		}
	}

	return false
}

// resourceMatches 检查资源匹配
func (s *Statement) resourceMatches(resource string) bool {
	// 处理NotResource
	for _, nr := range s.NotResource {
		if matchResource(nr, resource) {
			return false
		}
	}

	// 如果Resource为空，则匹配所有资源
	if len(s.Resource) == 0 {
		return true
	}

	// 检查允许的资源
	for _, r := range s.Resource {
		if matchResource(r, resource) {
			return true
		}
	}

	return false
}

// conditionMatches 检查条件匹配
func (s *Statement) conditionMatches(context map[string]string) bool {
	if s.Condition == nil {
		return true
	}

	for conditionKey, conditionValues := range s.Condition {
		for operator, values := range conditionValues {
			contextValue, exists := context[conditionKey]
			if !exists {
				return false
			}

			switch operator {
			case "StringEquals":
				found := false
				for _, v := range values {
					if v == contextValue {
						found = true
						break
					}
				}
				if !found {
					return false
				}

			case "StringNotEquals":
				for _, v := range values {
					if v == contextValue {
						return false
					}
				}

			case "StringLike":
				found := false
				for _, pattern := range values {
					if matchPattern(pattern, contextValue) {
						found = true
						break
					}
				}
				if !found {
					return false
				}

			case "StringNotLike":
				for _, pattern := range values {
					if matchPattern(pattern, contextValue) {
						return false
					}
				}

			case "IpAddress":
				found := false
				for _, cidr := range values {
					if matchCIDR(cidr, contextValue) {
						found = true
						break
					}
				}
				if !found {
					return false
				}

				// 其他条件运算符...
			}
		}
	}

	return true
}

// matchARN 检查ARN是否匹配
func matchARN(pattern, arn string) bool {
	if pattern == "*" {
		return true
	}

	// 将模式转换为正则表达式
	pattern = regexp.QuoteMeta(pattern)
	pattern = strings.ReplaceAll(pattern, "\\*", ".*")
	pattern = "^" + pattern + "$"

	matched, _ := regexp.MatchString(pattern, arn)
	return matched
}

// matchAction 检查操作是否匹配
func matchAction(pattern, action string) bool {
	// 操作格式: s3:GetObject
	parts := strings.SplitN(pattern, ":", 2)
	if len(parts) != 2 {
		return false
	}

	if parts[0] != "s3" {
		return false
	}

	if parts[1] == "*" {
		return true
	}

	return pattern == action
}

// matchResource 检查资源是否匹配
func matchResource(pattern, resource string) bool {
	if pattern == "*" {
		return true
	}

	// 资源格式: arn:aws:s3:::my-bucket/*
	pattern = regexp.QuoteMeta(pattern)
	pattern = strings.ReplaceAll(pattern, "\\*", ".*")
	pattern = "^" + pattern + "$"

	matched, _ := regexp.MatchString(pattern, resource)
	return matched
}

// matchPattern 使用通配符匹配字符串
func matchPattern(pattern, value string) bool {
	if pattern == "*" {
		return true
	}

	// 将通配符模式转换为正则表达式
	pattern = regexp.QuoteMeta(pattern)
	pattern = strings.ReplaceAll(pattern, "\\*", ".*")
	pattern = strings.ReplaceAll(pattern, "\\?", ".")
	pattern = "^" + pattern + "$"

	matched, _ := regexp.MatchString(pattern, value)
	return matched
}

// matchCIDR 检查IP是否在CIDR范围内
func matchCIDR(cidr, ip string) bool {
	// 解析CIDR字符串
	_, ipnet, err := net.ParseCIDR(cidr)
	if err != nil {
		return false
	}

	// 解析IP地址
	ipAddr := net.ParseIP(ip)
	if ipAddr == nil {
		return false
	}

	// 检查IP是否在CIDR范围内
	return ipnet.Contains(ipAddr)
}

// AddStatement 添加新的策略声明
func (p *BucketPolicy) AddStatement(stmt Statement) {
	if p == nil {
		return
	}
	p.Statements = append(p.Statements, stmt)
	p.UpdatedAt = time.Now().UTC()
}

// RemoveStatement 移除策略声明
func (p *BucketPolicy) RemoveStatement(sid string) {
	if p == nil {
		return
	}

	for i, stmt := range p.Statements {
		if stmt.Sid == sid {
			p.Statements = append(p.Statements[:i], p.Statements[i+1:]...)
			p.UpdatedAt = time.Now().UTC()
			return
		}
	}
}

// Validate 验证策略是否有效
func (p *BucketPolicy) Validate() error {
	if p == nil {
		return errors.New("policy is nil")
	}

	if p.Version != "2008-10-17" && p.Version != "2012-10-17" {
		return errors.New("invalid policy version")
	}

	for _, stmt := range p.Statements {
		if stmt.Effect != "Allow" && stmt.Effect != "Deny" {
			return fmt.Errorf("invalid effect in statement %s", stmt.Sid)
		}

		// 检查Principal和NotPrincipal不能同时存在
		if len(stmt.Principal.AWS) > 0 && len(stmt.NotPrincipal.AWS) > 0 {
			return fmt.Errorf("principal and NotPrincipal cannot both be specified in statement %s", stmt.Sid)
		}

		// 检查Action和NotAction不能同时存在
		if len(stmt.Action) > 0 && len(stmt.NotAction) > 0 {
			return fmt.Errorf("action and NotAction cannot both be specified in statement %s", stmt.Sid)
		}

		// 检查Resource和NotResource不能同时存在
		if len(stmt.Resource) > 0 && len(stmt.NotResource) > 0 {
			return fmt.Errorf("resource and NotResource cannot both be specified in statement %s", stmt.Sid)
		}
	}

	return nil
}
