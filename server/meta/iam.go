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
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	Rand "math/rand"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/twmb/murmur3"

	xhttp "github.com/mageg-x/dedups3/internal/http"
	"github.com/mageg-x/dedups3/internal/logger"
)

const (
	// AccountOn indicates that credentials are enabled
	AccountOn = "on"
	// AccountOff indicates that credentials are disabled
	AccountOff = "off"

	MaxUserNum      = 100 // 每个账号最多创建 100个子用户
	MaxGroupNum     = 100 // 每个账号最多创建 100个group
	MaxRoleNum      = 100 // 每个账号最多创建 100个role
	MaxPolicyNum    = 100 // 每个账号最多创建 100个policy
	MaxAccessKeyNum = 100 // 每个用户最多创建 100个 access key
)

var (
	TimeSentinel = time.Unix(0, 0).UTC()
)

type StringSet map[string]struct{}

func (s StringSet) MarshalJSON() ([]byte, error) {
	if s == nil {
		return []byte("[]"), nil
	}
	keys := make([]string, 0, len(s))
	for k := range s {
		keys = append(keys, k)
	}
	sort.Strings(keys) // 可选：保证输出顺序一致
	return json.Marshal(keys)
}

func (s *StringSet) UnmarshalJSON(data []byte) error {
	if *s == nil {
		*s = make(StringSet)
	}
	for k := range *s {
		delete(*s, k)
	}

	var list []string
	if err := json.Unmarshal(data, &list); err != nil {
		return err
	}
	for _, v := range list {
		(*s)[v] = struct{}{}
	}
	return nil
}

// IamAccount 表示完整的 IAM 系统
type IamAccount struct {
	AccountID string       `json:"accountId"` // AWS 账户ID
	Name      string       `json:"name"`      // AWS 账户名
	Users     StringSet    `json:"users"`     // IAM 用户 (key: 用户名)
	Groups    StringSet    `json:"groups"`    // IAM 用户组 (key: 组名)
	Roles     StringSet    `json:"roles"`     // IAM 角色 (key: 角色名)
	Policies  StringSet    `json:"policies"`  // IAM 策略 (key: 策略名)
	Quota     *QuotaConfig `json:"quota"`     // 配额限制
}

// IamUser 表示 IAM 用户
type IamUser struct {
	ID                  string            `json:"id"`                  // 用户唯一ID
	ARN                 string            `json:"arn"`                 // 用户ARN
	Username            string            `json:"username"`            // 用户名
	Password            string            `json:"password"`            // 登录密码
	AccessKeys          StringSet         `json:"accessKeys"`          // 访问密钥
	Groups              StringSet         `json:"groups"`              // 所属用户组
	Roles               StringSet         `json:"roles"`               // 用户可以担任的角色
	AttachedPolicies    StringSet         `json:"attachedPolicies"`    // 附加策略
	PermissionsBoundary string            `json:"permissionsBoundary"` // 权限边界
	Tags                map[string]string `json:"tags"`                // 用户标签
	IsRoot              bool              `json:"isRoot"`              // 是否是根用户
	Enabled             bool              `json:"enabled"`             // 是否启用
	CreatedAt           time.Time         `json:"createdAt"`           // 创建时间
}

// AccessKey 表示访问密钥
type AccessKey struct {
	AccessKeyID string    `json:"accessKeyId"`
	SecretKey   string    `json:"secretKey"`
	Status      bool      `json:"status"` // Active | Inactive
	CreatedAt   time.Time `json:"createdAt"`
	ExpiredAt   time.Time `json:"expiredAt"`
	AccountID   string    `json:"accountId"`
	Username    string    `json:"username"` // 创建者
}

// IamGroup 表示 IAM 用户组
type IamGroup struct {
	ARN              string    `json:"arn"`
	Name             string    `json:"name"`
	Description      string    `json:"description"`
	Users            StringSet `json:"users"`            // 组成员
	AttachedPolicies StringSet `json:"attachedPolicies"` // 附加策略名称列表
	CreateAt         time.Time `json:"createAt"`
}

// IamRole 表示 IAM 角色
type IamRole struct {
	ARN              string    `json:"arn"`
	Name             string    `json:"name"`
	Description      string    `json:"description"`
	AssumeRolePolicy string    `json:"assumeRolePolicy,omitempty"` // 信任策略JSON字符串
	AttachedPolicies StringSet `json:"attachedPolicies"`           // 附加策略名称列表
	CreateAt         time.Time `json:"createAt"`
}

// IamPolicy 表示 IAM 策略
type IamPolicy struct {
	ARN         string    `json:"arn"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	Document    string    `json:"document"` // JSON 策略文档字符串
	CreateAt    time.Time `json:"createAt"`
}

// PolicyDocument 表示解析后的策略文档结构
type PolicyDocument struct {
	Version   string      `json:"Version"`
	Statement []Statement `json:"Statement"`
}

// CanonicalUser 表示规范用户（用于 S3 资源所有者）
type CanonicalUser struct {
	ID          string `json:"id"`          // 64字符十六进制ID
	DisplayName string `json:"displayName"` // 显示名称
}

type QuotaConfig struct {
	MaxSpaceSize   int  `json:"maxSpaceSize"`
	MaxObjectCount int  `json:"maxObjectCount"`
	Enable         bool `json:"enable"`
}

// ==================== ========== ARN 格式化函数 ==============================
// arn:aws:iam::{accountID}:user/{username}
func FormatUserARN(accountID, username string) string {
	return "arn:aws:iam::" + accountID + ":user/" + username
}

func FormatGroupARN(accountID, groupName string) string {
	return "arn:aws:iam::" + accountID + ":group/" + groupName
}

func FormatRoleARN(accountID, roleName string) string {
	return "arn:aws:iam::" + accountID + ":role/" + roleName
}

func FormatPolicyARN(accountID, policyName string) string {
	return "arn:aws:iam::" + accountID + ":policy/" + policyName
}

func (a *AccessKey) IsExpired() bool {
	if a.ExpiredAt.IsZero() || a.ExpiredAt.Equal(TimeSentinel) {
		return false
	}
	return a.ExpiredAt.Before(time.Now().UTC())
}

// CreateAccount 创建新的 IAM 系统
func CreateAccount(name string) *IamAccount {
	return &IamAccount{
		AccountID: GenerateAccountID(name),
		Name:      name,
		Users:     make(StringSet, 0),
		Groups:    make(StringSet, 0),
		Roles:     make(StringSet, 0),
		Policies:  make(StringSet, 0),
		Quota: &QuotaConfig{
			MaxSpaceSize:   100 * 1024 * 1024, // 100GB,单位为 KB
			MaxObjectCount: 100000,
			Enable:         true,
		},
	}
}

// CreateUser 创建新用户
func (a *IamAccount) CreateUser(username, password string, groups, roles, policies []string, enable bool) (*IamUser, error) {
	if _, exists := a.Users[username]; exists {
		logger.GetLogger("dedups3").Errorf("user %s already exists", username)
		return nil, xhttp.ToError(xhttp.ErrUserAlreadyExists)
	}
	if err := ValidateUsername(username); err != nil {
		logger.GetLogger("dedups3").Errorf("username %s is invalid format", username)
		return nil, xhttp.ToError(xhttp.ErrInvalidName)
	}

	if err := ValidatePassword(password, username); err != nil {
		logger.GetLogger("dedups3").Errorf("password for user %s is invalid: %v", username, err)
		return nil, xhttp.ToError(xhttp.ErrInvalidRequest)
	}

	user := &IamUser{
		ID:               generateCanonicalUserID(),
		ARN:              FormatUserARN(a.AccountID, username),
		Username:         username,
		Password:         password,
		CreatedAt:        time.Now().UTC(),
		AccessKeys:       make(StringSet),
		Groups:           make(StringSet),
		Roles:            make(StringSet),
		AttachedPolicies: make(StringSet),
		Tags:             make(map[string]string),
		Enabled:          enable,
	}
	for _, group := range groups {
		if _, exists := a.Groups[group]; exists {
			user.Groups[group] = struct{}{}
		}
	}
	for _, role := range roles {
		if _, exists := a.Roles[role]; exists {
			user.Roles[role] = struct{}{}
		}
	}
	for _, policy := range policies {
		if _, exists := a.Policies[policy]; exists {
			user.AttachedPolicies[policy] = struct{}{}
		}
	}
	return user, nil
}

// ============================== 辅助函数 ==============================

// 验证JSON格式
func IsValidJSON(str string) bool {
	var js json.RawMessage
	return json.Unmarshal([]byte(str), &js) == nil
}

// ValidatePolicyDocument 验证策略文档是否有效
func ValidatePolicyDocument(document string) error {
	var doc PolicyDocument
	if err := json.Unmarshal([]byte(document), &doc); err != nil {
		return fmt.Errorf("invalid JSON format: %w", err)
	}

	if doc.Version != "2008-10-17" && doc.Version != "2012-10-17" {
		return errors.New("unsupported policy version, must be '2008-10-17' or '2012-10-17'")
	}

	for i, stmt := range doc.Statement {
		if stmt.Effect != "Allow" && stmt.Effect != "Deny" {
			return fmt.Errorf("statement %d has invalid effect '%s', must be 'Allow' or 'Deny'", i, stmt.Effect)
		}

		if len(stmt.Action) == 0 {
			return fmt.Errorf("statement %d must specify at least one action", i)
		}
	}

	return nil
}

// 生成账户ID (12位数字，符合AWS规范)
func GenerateAccountID(name string) string {
	hash := murmur3.Sum64([]byte(name + "@raobanglin"))
	key := strconv.FormatUint(uint64(hash), 10)
	if len(key) > 12 {
		key = key[:12]
	} else if len(key) < 12 {
		key = key + strings.Repeat("0", 12-len(key))
	}
	return key
}

// 生成规范用户ID (64字符十六进制)
func generateCanonicalUserID() string {
	bytes := make([]byte, 32)
	rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

// ValidateAccountID 验证账户ID是否符合AWS规范 (12位数字)
func ValidateAccountID(accountID string) error {
	if len(accountID) != 12 {
		return errors.New("account ID must be 12 digits")
	}

	for _, c := range accountID {
		if c < '0' || c > '9' {
			return errors.New("account ID must contain only digits")
		}
	}

	return nil
}

// IsValidIAMName 检查是否为合法的 IAM 资源名称（Policy、Group、User、Role 等）
func IsValidIAMName(name string) bool {
	// 检查是否只包含 ASCII 字符
	for i := 0; i < len(name); i++ {
		if name[i] > 127 {
			return false // 包含非 ASCII，如中文、日文等
		}
	}

	// 检查长度
	if n := utf8.RuneCountInString(name); n < 1 || n > 128 {
		return false
	}

	// 使用正则检查字符集
	matched, err := regexp.MatchString(`^[a-zA-Z0-9+=,.@_-]+$`, name)
	if err != nil {
		return false // 不应该发生
	}

	return matched
}

// ValidateUsername 验证用户名是否符合AWS IAM规范
func ValidateUsername(username string) error {
	if len(username) == 0 || len(username) > 64 || username == "root" {
		return errors.New("username length must be between 1 and 64 characters")
	}

	// 检查首字符和尾字符
	firstChar := username[0]
	lastChar := username[len(username)-1]
	if firstChar == '.' || firstChar == '-' || lastChar == '.' || lastChar == '-' {
		return errors.New("username cannot start or end with '.' or '-'")
	}

	// 检查字符是否合法以及是否有连续的点或连字符
	for i := 0; i < len(username)-1; i++ {
		current := username[i]
		next := username[i+1]

		// 检查字符是否合法
		if !((current >= 'a' && current <= 'z') ||
			(current >= 'A' && current <= 'Z') ||
			(current >= '0' && current <= '9') ||
			current == '_' || current == '.' || current == '-') {
			return errors.New("username can only contain letters, numbers, '_', '.' and '-' ")
		}

		// 检查是否有连续的点或连字符
		if (current == '.' && next == '.') || (current == '-' && next == '-') {
			return errors.New("username cannot contain consecutive '.' or '-'")
		}
	}

	// 检查最后一个字符是否合法
	lastChar = username[len(username)-1]
	if !((lastChar >= 'a' && lastChar <= 'z') ||
		(lastChar >= 'A' && lastChar <= 'Z') ||
		(lastChar >= '0' && lastChar <= '9') ||
		lastChar == '_' || lastChar == '.' || lastChar == '-') {
		return errors.New("username can only contain letters, numbers, '_', '.' and '-' ")
	}

	return nil
}

// ValidatePassword 验证密码是否符合AWS IAM规范
func ValidatePassword(password, username string) error {
	// 检查长度
	if len(password) < 8 {
		return errors.New("password must be at least 8 characters long")
	}

	// 检查是否包含大写字母
	hasUpperCase := false
	// 检查是否包含小写字母
	hasLowerCase := false
	// 检查是否包含数字
	hasDigit := false
	// 检查是否包含特殊字符
	hasSpecialChar := false
	// 特殊字符集
	specialChars := "!@#$%^&*()_+-=[]{}|;:,.<>?/"

	for _, c := range password {
		if c >= 'A' && c <= 'Z' {
			hasUpperCase = true
		} else if c >= 'a' && c <= 'z' {
			hasLowerCase = true
		} else if c >= '0' && c <= '9' {
			hasDigit = true
		} else if strings.ContainsRune(specialChars, c) {
			hasSpecialChar = true
		}
	}

	if !hasUpperCase {
		return errors.New("password must contain at least one uppercase letter")
	}

	if !hasLowerCase {
		return errors.New("password must contain at least one lowercase letter")
	}

	if !hasDigit {
		return errors.New("password must contain at least one digit")
	}

	if !hasSpecialChar {
		return errors.New("password must contain at least one special character (!@#$%^&*()_+-=[]{}|;:,.<>?/)")
	}

	// 检查是否包含用户名
	if strings.Contains(strings.ToLower(password), strings.ToLower(username)) {
		return errors.New("password cannot contain username")
	}

	return nil
}

// 生成访问密钥ID (20字符)
func GenerateAccessKeyID() string {
	const charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, 20)
	for i := range b {
		b[i] = charset[Rand.Intn(len(charset))]
	}
	return string(b)
}

// 生成秘密访问密钥 (40字符)
func GenerateSecretAccessKey() string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, 40)
	for i := range b {
		b[i] = charset[Rand.Intn(len(charset))]
	}
	return string(b)
}

func ValidateAccessKeyID(ak string) bool {
	if len(ak) < 5 {
		return false
	}
	for i := 0; i < len(ak); i++ {
		c := ak[i]
		if !((c >= 'A' && c <= 'Z') ||
			(c >= 'a' && c <= 'z') ||
			(c >= '0' && c <= '9') ||
			c == '+' || c == '.' || c == '-' || c == '_' || c == '=' || c == '&' || c == '@' || c == '#' || c == '!') {
			return false
		}
	}
	return true
}

func ValidateSecretAccessKey(sk string) bool {
	if len(sk) < 5 {
		return false
	}
	for i := 0; i < len(sk); i++ {
		c := sk[i]
		if !((c >= 'A' && c <= 'Z') ||
			(c >= 'a' && c <= 'z') ||
			(c >= '0' && c <= '9') ||
			c == '+' || c == '.' || c == '-' || c == '_' || c == '=' || c == '&' || c == '@' || c == '#' || c == '!') {
			return false
		}
	}
	return true
}

// EvaluatePolicy 评估单个策略
func EvaluatePolicy(policyDoc, action, resource string) (allowed, explicitDeny bool, err error) {
	var doc PolicyDocument
	if err := json.Unmarshal([]byte(policyDoc), &doc); err != nil {
		return false, false, fmt.Errorf("invalid policy document: %w", err)
	}

	for _, stmt := range doc.Statement {
		// 检查操作匹配
		actionMatch := false
		for _, a := range stmt.Action {
			if matchPattern(a, action) {
				actionMatch = true
				break
			}
		}
		if !actionMatch {
			continue
		}

		// 检查资源匹配（如果策略指定了资源）
		if len(stmt.Resource) > 0 {
			resourceMatch := false
			for _, r := range stmt.Resource {
				if matchPattern(r, resource) {
					resourceMatch = true
					break
				}
			}
			if !resourceMatch {
				continue
			}
		}

		// 匹配的语句
		switch stmt.Effect {
		case "Allow":
			allowed = true
		case "Deny":
			explicitDeny = true
			return false, true, nil // 遇到显式拒绝立即返回
		}
	}

	return allowed, explicitDeny, nil
}
