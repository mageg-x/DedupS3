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
)

// AccessControlPolicy 表示S3访问控制策略，符合AWS API规范
type AccessControlPolicy struct {
	XMLName           xml.Name          `xml:"http://s3.amazonaws.com/doc/2006-03-01/ AccessControlPolicy"`
	Owner             CanonicalUser     `json:"owner" xml:"Owner"`                         // 资源所有者
	AccessControlList AccessControlList `json:"accessControlList" xml:"AccessControlList"` //授权列表
}

type AccessControlList struct {
	Grants []Grant `json:"grants" xml:"Grant"`
}

// Grant 表示单个授权项
type Grant struct {
	Grantee    Grantee `json:"grantee" xml:"Grantee"`       // 被授权者
	Permission string  `json:"permission" xml:"Permission"` // 权限
}

// Grantee 表示被授权者
type Grantee struct {
	XMLName     xml.Name `xml:"Grantee"`
	Type        string   `json:"type" xml:"xsi:type,attr"`                // 类型: CanonicalUser | AmazonCustomerByEmail | Group
	ID          string   `json:"id" xml:"ID,omitempty"`                   // 规范用户ID
	DisplayName string   `json:"displayName" xml:"DisplayName,omitempty"` // 显示名称
	Email       string   `json:"email" xml:"EmailAddress,omitempty"`      // 邮箱地址
	URI         string   `json:"uri" xml:"URI,omitempty"`                 // 组URI
}

// 预定义组常量
const (
	AllUsersGroup    = "http://acs.amazonaws.com/groups/global/AllUsers"
	AuthUsersGroup   = "http://acs.amazonaws.com/groups/global/AuthenticatedUsers"
	LogDeliveryGroup = "http://acs.amazonaws.com/groups/s3/LogDelivery"
)

// 权限常量
const (
	PermissionRead        = "READ"
	PermissionWrite       = "WRITE"
	PermissionReadACP     = "READ_ACP"
	PermissionWriteACP    = "WRITE_ACP"
	PermissionFullControl = "FULL_CONTROL"
)

// NewAccessControlPolicy 创建新的访问控制策略
func NewAccessControlPolicy(owner CanonicalUser) *AccessControlPolicy {
	return &AccessControlPolicy{
		Owner: owner,
		AccessControlList: AccessControlList{
			Grants: []Grant{
				{
					Grantee: Grantee{
						Type:        "CanonicalUser",
						ID:          owner.ID,
						DisplayName: owner.DisplayName,
					},
					Permission: PermissionFullControl,
				},
			},
		},
	}
}

// AddGrant 添加授权
func (acp *AccessControlPolicy) AddGrant(granteeType, id, displayName, email, uri, permission string) error {
	// 验证权限类型
	validPermissions := map[string]bool{
		PermissionRead:        true,
		PermissionWrite:       true,
		PermissionReadACP:     true,
		PermissionWriteACP:    true,
		PermissionFullControl: true,
	}
	if !validPermissions[permission] {
		return errors.New("invalid permission type")
	}

	grantee := Grantee{Type: granteeType}

	switch granteeType {
	case "CanonicalUser":
		if id == "" {
			return errors.New("ID is required for CanonicalUser")
		}
		grantee.ID = id
		grantee.DisplayName = displayName
	case "AmazonCustomerByEmail":
		if email == "" {
			return errors.New("email is required for AmazonCustomerByEmail")
		}
		grantee.Email = email
	case "Group":
		if uri == "" {
			return errors.New("URI is required for Group")
		}
		grantee.URI = uri
	default:
		return errors.New("invalid grantee type")
	}

	acp.AccessControlList.Grants = append(acp.AccessControlList.Grants, Grant{
		Grantee:    grantee,
		Permission: permission,
	})
	return nil
}

// GrantPublicRead 授予公共读取权限
func (acp *AccessControlPolicy) GrantPublicRead() error {
	return acp.AddGrant("Group", "", "", "", AllUsersGroup, PermissionRead)
}

// GrantPublicReadWrite 授予公共读写权限
func (acp *AccessControlPolicy) GrantPublicReadWrite() error {
	if err := acp.AddGrant("Group", "", "", "", AllUsersGroup, PermissionRead); err != nil {
		return err
	}
	return acp.AddGrant("Group", "", "", "", AllUsersGroup, PermissionWrite)
}

// GrantLogDeliveryWrite 授予日志交付组写入权限
func (acp *AccessControlPolicy) GrantLogDeliveryWrite() error {
	return acp.AddGrant("Group", "", "", "", LogDeliveryGroup, PermissionWrite)
}

// RemoveGrant 移除授权
func (acp *AccessControlPolicy) RemoveGrant(granteeType, id, email, uri string) {
	for i, grant := range acp.AccessControlList.Grants {
		switch granteeType {
		case "CanonicalUser":
			if grant.Grantee.Type == "CanonicalUser" && grant.Grantee.ID == id {
				acp.AccessControlList.Grants = append(acp.AccessControlList.Grants[:i], acp.AccessControlList.Grants[i+1:]...)
				return
			}
		case "AmazonCustomerByEmail":
			if grant.Grantee.Type == "AmazonCustomerByEmail" && grant.Grantee.Email == email {
				acp.AccessControlList.Grants = append(acp.AccessControlList.Grants[:i], acp.AccessControlList.Grants[i+1:]...)
				return
			}
		case "Group":
			if grant.Grantee.Type == "Group" && grant.Grantee.URI == uri {
				acp.AccessControlList.Grants = append(acp.AccessControlList.Grants[:i], acp.AccessControlList.Grants[i+1:]...)
				return
			}
		}
	}
}

// HasPermission 检查被授权者是否有特定权限
func (acp *AccessControlPolicy) HasPermission(grantee Grantee, permission string) bool {
	for _, grant := range acp.AccessControlList.Grants {
		if grantMatches(grant.Grantee, grantee) {
			if grant.Permission == PermissionFullControl {
				return true
			}
			if grant.Permission == permission {
				return true
			}
		}
	}
	return false
}

// grantMatches 检查两个被授权者是否匹配
func grantMatches(g1, g2 Grantee) bool {
	if g1.Type != g2.Type {
		return false
	}

	switch g1.Type {
	case "CanonicalUser":
		return g1.ID == g2.ID
	case "AmazonCustomerByEmail":
		return g1.Email == g2.Email
	case "Group":
		return g1.URI == g2.URI
	default:
		return false
	}
}

// ToXML 将 AccessControlPolicy 转换为 XML
func (acp *AccessControlPolicy) ToXML() ([]byte, error) {
	return xml.Marshal(acp)
}

// ParseXML 从 XML 解析 AccessControlPolicy
func (acp *AccessControlPolicy) ParseXML(data []byte) error {
	return xml.Unmarshal(data, acp)
}

// IsPublic 检查访问控制策略是否包含公共访问
func (acp *AccessControlPolicy) IsPublic() bool {
	for _, grant := range acp.AccessControlList.Grants {
		if grant.Grantee.Type == "Group" && grant.Grantee.URI == AllUsersGroup {
			return true
		}
	}
	return false
}
