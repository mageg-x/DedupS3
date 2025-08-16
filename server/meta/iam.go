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
	"strings"
	"time"
)

// IAMAccount 表示完整的 IAM 系统
type IAMAccount struct {
	AccountID string                `json:"accountId"` // AWS 账户ID
	Users     map[string]*IAMUser   `json:"users"`     // IAM 用户 (key: 用户名)
	Groups    map[string]*IAMGroup  `json:"groups"`    // IAM 用户组 (key: 组名)
	Roles     map[string]*IAMRole   `json:"roles"`     // IAM 角色 (key: 角色名)
	Policies  map[string]*IAMPolicy `json:"policies"`  // IAM 策略 (key: 策略名)
}

// CreateAccount 创建新的 IAM 系统
func CreateAccount(accountID string) *IAMAccount {
	return &IAMAccount{
		AccountID: accountID,
		Users:     make(map[string]*IAMUser),
		Groups:    make(map[string]*IAMGroup),
		Roles:     make(map[string]*IAMRole),
		Policies:  make(map[string]*IAMPolicy),
	}
}

// IAMUser 表示 IAM 用户
type IAMUser struct {
	ID                  string            `json:"id"`                  // 用户唯一ID
	ARN                 string            `json:"arn"`                 // 用户ARN
	Username            string            `json:"username"`            // 用户名
	Path                string            `json:"path"`                // 用户路径
	Password            string            `json:"password"`            // 登录密码
	AccessKeys          []AccessKey       `json:"accessKeys"`          // 访问密钥
	MFADevices          []MFADevice       `json:"mfaDevices"`          // MFA设备
	Groups              []string          `json:"groups"`              // 所属用户组
	Roles               []string          `json:"roles"`               // 用户可以担任的角色
	AttachedPolicies    []string          `json:"attachedPolicies"`    // 附加策略
	PermissionsBoundary string            `json:"permissionsBoundary"` // 权限边界
	Tags                map[string]string `json:"tags"`                // 用户标签
	IsRoot              bool              `json:"isRoot"`              // 是否是根用户
	CreatedAt           time.Time         `json:"createdAt"`           // 创建时间
}

// AccessKey 表示访问密钥
type AccessKey struct {
	AccessKeyID     string    `json:"accessKeyId"`
	SecretAccessKey string    `json:"secretAccessKey"`
	Status          string    `json:"status"` // Active | Inactive
	CreatedAt       time.Time `json:"createdAt"`
}

// MFADevice 表示 MFA 设备
type MFADevice struct {
	DeviceName string `json:"deviceName"`
	Type       string `json:"type"` // Virtual | Hardware
	Enabled    bool   `json:"enabled"`
}

// IAMGroup 表示 IAM 用户组
type IAMGroup struct {
	ARN              string   `json:"arn"`
	Name             string   `json:"name"`
	Path             string   `json:"path"`
	Users            []string `json:"users"`            // 组成员
	AttachedPolicies []string `json:"attachedPolicies"` // 附加策略
}

// IAMRole 表示 IAM 角色
type IAMRole struct {
	ARN              string   `json:"arn"`
	Name             string   `json:"name"`
	Path             string   `json:"path"`
	AssumeRolePolicy string   `json:"assumeRolePolicy"` // 信任策略
	AttachedPolicies []string `json:"attachedPolicies"` // 附加策略
}

// IAMPolicy 表示 IAM 策略
type IAMPolicy struct {
	ARN         string `json:"arn"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Document    string `json:"document"` // JSON 策略文档
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

// ============================== ARN 格式化函数 ==============================

func formatUserARN(accountID, path, username string) string {
	return "arn:aws:iam::" + accountID + ":user" + normalizePath(path) + username
}

func formatGroupARN(accountID, path, groupName string) string {
	return "arn:aws:iam::" + accountID + ":group" + normalizePath(path) + groupName
}

func formatRoleARN(accountID, path, roleName string) string {
	return "arn:aws:iam::" + accountID + ":role" + normalizePath(path) + roleName
}

func formatPolicyARN(accountID, policyName string) string {
	return "arn:aws:iam::" + accountID + ":policy/" + policyName
}

func formatRootARN(accountID string) string {
	return "arn:aws:iam::" + accountID + ":root"
}

// 规范化路径格式
func normalizePath(path string) string {
	if path == "" {
		return "/"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	if !strings.HasSuffix(path, "/") {
		path += "/"
	}
	return path
}

// ============================== 根用户操作 ==============================

// CreateRootUser 创建根用户（账户所有者）
func (a *IAMAccount) CreateRootUser() (*IAMUser, error) {
	// 检查是否已存在根用户
	if _, exists := a.Users["root"]; exists {
		return nil, errors.New("root user already exists")
	}

	// 创建根用户
	rootUser := &IAMUser{
		ID:        generateCanonicalUserID(),
		ARN:       formatRootARN(a.AccountID),
		Username:  "root",
		IsRoot:    true,
		CreatedAt: time.Now().UTC(),
	}

	a.Users["root"] = rootUser
	return rootUser, nil
}

// ============================== 用户操作 ==============================

// CreateUser 创建新用户
func (a *IAMAccount) CreateUser(username, path string) (*IAMUser, error) {
	if _, exists := a.Users[username]; exists {
		return nil, errors.New("user already exists")
	}

	user := &IAMUser{
		ID:         generateCanonicalUserID(),
		ARN:        formatUserARN(a.AccountID, path, username),
		Username:   username,
		Path:       normalizePath(path),
		CreatedAt:  time.Now().UTC(),
		AccessKeys: make([]AccessKey, 0),
		Groups:     make([]string, 0),
		Roles:      make([]string, 0), // 初始化空角色列表
		Tags:       make(map[string]string),
	}

	a.Users[username] = user
	return user, nil
}

// AddRoleToUser 让用户可以担任某个角色
func (a *IAMAccount) AddRoleToUser(username, roleName string) error {
	user, uExists := a.Users[username]
	_, rExists := a.Roles[roleName]

	if !uExists || !rExists {
		return errors.New("user or role not found")
	}

	// 检查用户是否已担任该角色
	for _, r := range user.Roles {
		if r == roleName {
			return errors.New("user already has this role")
		}
	}

	user.Roles = append(user.Roles, roleName)
	return nil
}

// CreateAccessKey 为用户创建访问密钥
func (a *IAMAccount) CreateAccessKey(username string) (*AccessKey, error) {
	user, exists := a.Users[username]
	if !exists {
		return nil, errors.New("user not found")
	}

	accessKey := AccessKey{
		AccessKeyID:     generateAccessKeyID(),
		SecretAccessKey: generateSecretAccessKey(),
		Status:          "Active",
		CreatedAt:       time.Now().UTC(),
	}

	user.AccessKeys = append(user.AccessKeys, accessKey)
	return &accessKey, nil
}

// ============================== 组操作 ==============================

// CreateGroup 创建新用户组
func (a *IAMAccount) CreateGroup(caller *IAMUser, name, path string) (*IAMGroup, error) {
	if !a.canManageIAM(caller) {
		return nil, errors.New("unauthorized to create groups")
	}

	if _, exists := a.Groups[name]; exists {
		return nil, errors.New("group already exists")
	}

	group := &IAMGroup{
		ARN:  formatGroupARN(a.AccountID, path, name),
		Name: name,
		Path: normalizePath(path),
	}

	a.Groups[name] = group
	return group, nil
}

// AddUserToGroup 添加用户到组
func (a *IAMAccount) AddUserToGroup(username, groupName string) error {
	user, uExists := a.Users[username]
	group, gExists := a.Groups[groupName]

	if !uExists || !gExists {
		return errors.New("user or group not found")
	}

	// 检查用户是否已在组中
	for _, g := range user.Groups {
		if g == groupName {
			return errors.New("user already in group")
		}
	}

	user.Groups = append(user.Groups, groupName)
	group.Users = append(group.Users, username)
	return nil
}

// ============================== 策略操作 ==============================

// CreatePolicy 创建新策略
func (a *IAMAccount) CreatePolicy(caller *IAMUser, name, description, document string) (*IAMPolicy, error) {
	// 检查调用者是否有创建策略的权限
	if !a.canManageIAM(caller) {
		return nil, errors.New("unauthorized to create policies")
	}

	if _, exists := a.Policies[name]; exists {
		return nil, errors.New("policy already exists")
	}

	// 验证策略文档
	if err := ValidatePolicyDocument(document); err != nil {
		return nil, fmt.Errorf("invalid policy document: %w", err)
	}

	policy := &IAMPolicy{
		ARN:         formatPolicyARN(a.AccountID, name),
		Name:        name,
		Description: description,
		Document:    document,
	}

	a.Policies[name] = policy
	return policy, nil
}

// AttachPolicyToUser 附加策略到用户
func (a *IAMAccount) AttachPolicyToUser(caller *IAMUser, username, policyName string) error {
	// 检查特定 IAM 权限
	allowed, _ := a.CheckPermission(caller.Username, "iam:AttachUserPolicy", "*") // 检查调用者是否有附加策略的权限
	if !allowed {
		return errors.New("unauthorized to attach policies")
	}

	user, exists := a.Users[username]
	if !exists {
		return errors.New("user not found")
	}

	// 检查策略是否存在
	if _, pExists := a.Policies[policyName]; !pExists {
		return errors.New("policy not found")
	}

	// 检查是否已附加
	for _, p := range user.AttachedPolicies {
		if p == policyName {
			return errors.New("policy already attached")
		}
	}

	user.AttachedPolicies = append(user.AttachedPolicies, policyName)
	return nil
}

// canManageIAM 检查用户是否有 IAM 管理权限
func (a *IAMAccount) canManageIAM(user *IAMUser) bool {
	// 根用户总是有权限
	if user.IsRoot {
		return true
	}

	// 检查用户是否有 IAM 管理策略
	allowed, _ := a.CheckPermission(user.Username, "iam:*", "*")
	return allowed
}

// ============================== 角色操作 ==============================

// CreateRole 创建新角色
func (a *IAMAccount) CreateRole(caller *IAMUser, name, path, assumeRolePolicy string) (*IAMRole, error) {
	if !a.canManageIAM(caller) {
		return nil, errors.New("unauthorized to create roles")
	}

	if _, exists := a.Roles[name]; exists {
		return nil, errors.New("role already exists")
	}

	// 验证信任策略是否是有效的 JSON
	if !isValidJSON(assumeRolePolicy) {
		return nil, errors.New("assumeRolePolicy must be valid JSON")
	}

	role := &IAMRole{
		ARN:              formatRoleARN(a.AccountID, path, name),
		Name:             name,
		Path:             normalizePath(path),
		AssumeRolePolicy: assumeRolePolicy,
		AttachedPolicies: make([]string, 0),
	}

	a.Roles[name] = role
	return role, nil
}

// AttachPolicyToRole 附加策略到角色
func (a *IAMAccount) AttachPolicyToRole(roleName, policyName string) error {
	role, exists := a.Roles[roleName]
	if !exists {
		return errors.New("role not found")
	}

	// 检查策略是否存在
	if _, pExists := a.Policies[policyName]; !pExists {
		return errors.New("policy not found")
	}

	// 检查是否已附加
	for _, p := range role.AttachedPolicies {
		if p == policyName {
			return errors.New("policy already attached to role")
		}
	}

	role.AttachedPolicies = append(role.AttachedPolicies, policyName)
	return nil
}

// UpdateAssumeRolePolicy 更新角色的信任策略
func (a *IAMAccount) UpdateAssumeRolePolicy(roleName, assumeRolePolicy string) error {
	role, exists := a.Roles[roleName]
	if !exists {
		return errors.New("role not found")
	}

	// 验证新的信任策略
	if !isValidJSON(assumeRolePolicy) {
		return errors.New("assumeRolePolicy must be valid JSON")
	}

	role.AssumeRolePolicy = assumeRolePolicy
	return nil
}

// ListAttachedRolePolicies 列出角色附加的策略
func (a *IAMAccount) ListAttachedRolePolicies(roleName string) ([]string, error) {
	role, exists := a.Roles[roleName]
	if !exists {
		return nil, errors.New("role not found")
	}
	return role.AttachedPolicies, nil
}

// DetachPolicyFromRole 从角色分离策略
func (a *IAMAccount) DetachPolicyFromRole(roleName, policyName string) error {
	role, exists := a.Roles[roleName]
	if !exists {
		return errors.New("role not found")
	}

	newPolicies := make([]string, 0, len(role.AttachedPolicies))
	found := false

	for _, p := range role.AttachedPolicies {
		if p == policyName {
			found = true
		} else {
			newPolicies = append(newPolicies, p)
		}
	}

	if !found {
		return errors.New("policy not attached to role")
	}

	role.AttachedPolicies = newPolicies
	return nil
}

// GetRole 获取角色信息
func (a *IAMAccount) GetRole(roleName string) (*IAMRole, error) {
	role, exists := a.Roles[roleName]
	if !exists {
		return nil, errors.New("role not found")
	}
	return role, nil
}

// ============================== 权限检查方法 ==============================

// CheckPermission 检查用户是否有权限执行指定操作
func (a *IAMAccount) CheckPermission(username, action, resource string) (bool, error) {
	user, exists := a.Users[username]
	if !exists {
		return false, errors.New("user not found")
	}

	// 1. 根用户拥有所有权限
	if user.IsRoot {
		return true, nil
	}

	// 2. 收集用户所有策略（直接附加+通过组附加）
	allPolicies := a.getAllUserPolicies(user)

	// 3. 检查权限边界（如果设置了）
	if user.PermissionsBoundary != "" {
		if boundaryPolicy, exists := a.Policies[user.PermissionsBoundary]; exists {
			allowedByBoundary, explicitDeny, err := evaluatePolicy(boundaryPolicy.Document, action, resource)
			if err != nil {
				return false, fmt.Errorf("error evaluating permissions boundary: %w", err)
			}
			if explicitDeny {
				return false, nil // 权限边界明确拒绝
			}
			if !allowedByBoundary {
				return false, nil // 权限边界未允许此操作
			}
		}
	}

	// 4. 检查所有策略
	explicitDenyFound := false
	allowFound := false

	for _, policy := range allPolicies {
		allowed, explicitDeny, err := evaluatePolicy(policy.Document, action, resource)
		if err != nil {
			return false, fmt.Errorf("error evaluating policy %s: %w", policy.Name, err)
		}

		if explicitDeny {
			explicitDenyFound = true
		}
		if allowed {
			allowFound = true
		}
	}

	// 显式拒绝优先于任何允许
	if explicitDenyFound {
		return false, nil
	}

	return allowFound, nil
}

// 获取用户所有策略（直接附加+通过组附加）
func (a *IAMAccount) getAllUserPolicies(user *IAMUser) []*IAMPolicy {
	var policies []*IAMPolicy

	// 直接附加的策略
	for _, policyName := range user.AttachedPolicies {
		if policy, exists := a.Policies[policyName]; exists {
			policies = append(policies, policy)
		}
	}

	// 通过组附加的策略
	for _, groupName := range user.Groups {
		if group, exists := a.Groups[groupName]; exists {
			for _, policyName := range group.AttachedPolicies {
				if policy, exists := a.Policies[policyName]; exists {
					policies = append(policies, policy)
				}
			}
		}
	}

	// 通过角色附加的策略（新增部分）
	for _, roleName := range user.Roles {
		if role, exists := a.Roles[roleName]; exists {
			for _, policyName := range role.AttachedPolicies {
				if policy, exists := a.Policies[policyName]; exists {
					policies = append(policies, policy)
				}
			}
		}
	}
	return policies
}

// 评估单个策略
func evaluatePolicy(policyDoc, action, resource string) (allowed, explicitDeny bool, err error) {
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

// ============================== 辅助权限方法 ==============================

// CanUserPerformAction 检查用户是否能执行操作（简化版）
func (a *IAMAccount) CanUserPerformAction(username, action string) bool {
	// 对于不关心具体资源的情况，使用通配符资源
	allowed, _ := a.CheckPermission(username, action, "*")
	return allowed
}

// ListUserPermissions 列出用户所有允许的操作
func (a *IAMAccount) ListUserPermissions(username string) ([]string, error) {
	user, exists := a.Users[username]
	if !exists {
		return nil, errors.New("user not found")
	}

	permissions := make(map[string]struct{})
	allPolicies := a.getAllUserPolicies(user)

	for _, policy := range allPolicies {
		var doc PolicyDocument
		if err := json.Unmarshal([]byte(policy.Document), &doc); err != nil {
			continue // 跳过无效策略
		}

		for _, stmt := range doc.Statement {
			if stmt.Effect == "Allow" {
				for _, action := range stmt.Action {
					// 解析通配符操作
					if strings.Contains(action, "*") {
						// 简化处理：添加通配符模式本身
						permissions[action] = struct{}{}
					} else {
						permissions[action] = struct{}{}
					}
				}
			}
		}
	}

	// 转换为切片
	result := make([]string, 0, len(permissions))
	for perm := range permissions {
		result = append(result, perm)
	}
	return result, nil
}

// ============================== 认证操作 ==============================

// Authenticate 认证用户
func (a *IAMAccount) Authenticate(accessKeyID, secretAccessKey string) (*IAMUser, error) {
	for _, user := range a.Users {
		for _, key := range user.AccessKeys {
			if key.AccessKeyID == accessKeyID &&
				key.SecretAccessKey == secretAccessKey &&
				key.Status == "Active" {
				return user, nil
			}
		}
	}

	return nil, errors.New("authentication failed")
}

// GetUserByAccessKeyID 通过访问密钥ID查找用户
func (a *IAMAccount) GetUserByAccessKeyID(accessKeyID string) (*IAMUser, error) {
	for _, user := range a.Users {
		for _, key := range user.AccessKeys {
			if key.AccessKeyID == accessKeyID {
				return user, nil
			}
		}
	}
	return nil, errors.New("access key not found")
}

// ============================== 辅助函数 ==============================

// 验证JSON格式
func isValidJSON(str string) bool {
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
func generateAccountID() string {
	const charset = "0123456789"
	b := make([]byte, 12)
	for i := range b {
		b[i] = charset[Rand.Intn(len(charset))]
	}
	return string(b)
}

// 生成规范用户ID (64字符十六进制)
func generateCanonicalUserID() string {
	bytes := make([]byte, 32)
	rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

// 生成访问密钥ID (20字符)
func generateAccessKeyID() string {
	const charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, 20)
	for i := range b {
		b[i] = charset[Rand.Intn(len(charset))]
	}
	return string(b)
}

// 生成秘密访问密钥 (40字符)
func generateSecretAccessKey() string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789+/"
	b := make([]byte, 40)
	for i := range b {
		b[i] = charset[Rand.Intn(len(charset))]
	}
	return string(b)
}
