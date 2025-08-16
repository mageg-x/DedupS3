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

// ACL 表示访问控制列表
type ACL struct {
	Owner  CanonicalUser `json:"owner" xml:"Owner"`                    // 资源所有者
	Grants []Grant       `json:"grants" xml:"AccessControlList>Grant"` // 授权列表
}

// Grant 表示单个授权项
type Grant struct {
	Grantee    Grantee `json:"grantee" xml:"Grantee"`       // 被授权者
	Permission string  `json:"permission" xml:"Permission"` // 权限
}

// Grantee 表示被授权者
type Grantee struct {
	Type        string `json:"type" xml:"xsi:type,attr"`                // 类型: CanonicalUser | AmazonCustomerByEmail | Group
	ID          string `json:"id" xml:"ID,omitempty"`                   // 规范用户ID
	DisplayName string `json:"displayName" xml:"DisplayName,omitempty"` // 显示名称
	Email       string `json:"email" xml:"EmailAddress,omitempty"`      // 邮箱地址
	URI         string `json:"uri" xml:"URI,omitempty"`                 // 组URI
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

// NewACL 创建新的 ACL
func NewACL(owner CanonicalUser) *ACL {
	return &ACL{
		Owner: owner,
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
	}
}

// AddGrant 添加授权
func (a *ACL) AddGrant(granteeType, id, displayName, email, uri, permission string) error {
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

	a.Grants = append(a.Grants, Grant{
		Grantee:    grantee,
		Permission: permission,
	})
	return nil
}

// GrantPublicRead 授予公共读取权限
func (a *ACL) GrantPublicRead() error {
	return a.AddGrant("Group", "", "", "", AllUsersGroup, PermissionRead)
}

// GrantPublicReadWrite 授予公共读写权限
func (a *ACL) GrantPublicReadWrite() error {
	if err := a.AddGrant("Group", "", "", "", AllUsersGroup, PermissionRead); err != nil {
		return err
	}
	return a.AddGrant("Group", "", "", "", AllUsersGroup, PermissionWrite)
}

// GrantLogDeliveryWrite 授予日志交付组写入权限
func (a *ACL) GrantLogDeliveryWrite() error {
	return a.AddGrant("Group", "", "", "", LogDeliveryGroup, PermissionWrite)
}

// RemoveGrant 移除授权
func (a *ACL) RemoveGrant(granteeType, id, email, uri string) {
	for i, grant := range a.Grants {
		switch granteeType {
		case "CanonicalUser":
			if grant.Grantee.Type == "CanonicalUser" && grant.Grantee.ID == id {
				a.Grants = append(a.Grants[:i], a.Grants[i+1:]...)
				return
			}
		case "AmazonCustomerByEmail":
			if grant.Grantee.Type == "AmazonCustomerByEmail" && grant.Grantee.Email == email {
				a.Grants = append(a.Grants[:i], a.Grants[i+1:]...)
				return
			}
		case "Group":
			if grant.Grantee.Type == "Group" && grant.Grantee.URI == uri {
				a.Grants = append(a.Grants[:i], a.Grants[i+1:]...)
				return
			}
		}
	}
}

// HasPermission 检查被授权者是否有特定权限
func (a *ACL) HasPermission(grantee Grantee, permission string) bool {
	for _, grant := range a.Grants {
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

// ToXML 将 ACL 转换为 XML
func (a *ACL) ToXML() ([]byte, error) {
	type ACLXML struct {
		XMLName xml.Name `xml:"AccessControlPolicy"`
		Owner   struct {
			ID          string `xml:"ID"`
			DisplayName string `xml:"DisplayName"`
		} `xml:"Owner"`
		AccessControlList struct {
			Grants []Grant `xml:"Grant"`
		} `xml:"AccessControlList"`
	}

	aclXML := ACLXML{
		Owner: struct {
			ID          string `xml:"ID"`
			DisplayName string `xml:"DisplayName"`
		}{
			ID:          a.Owner.ID,
			DisplayName: a.Owner.DisplayName,
		},
		AccessControlList: struct {
			Grants []Grant `xml:"Grant"`
		}{
			Grants: a.Grants,
		},
	}

	return xml.Marshal(aclXML)
}

// ParseXML 从 XML 解析 ACL
func (a *ACL) ParseXML(data []byte) error {
	type ACLXML struct {
		Owner struct {
			ID          string `xml:"ID"`
			DisplayName string `xml:"DisplayName"`
		} `xml:"Owner"`
		AccessControlList struct {
			Grants []Grant `xml:"Grant"`
		} `xml:"AccessControlList"`
	}

	var aclXML ACLXML
	if err := xml.Unmarshal(data, &aclXML); err != nil {
		return err
	}

	a.Owner = CanonicalUser{
		ID:          aclXML.Owner.ID,
		DisplayName: aclXML.Owner.DisplayName,
	}
	a.Grants = aclXML.AccessControlList.Grants
	return nil
}

// IsPublic 检查 ACL 是否包含公共访问
func (a *ACL) IsPublic() bool {
	for _, grant := range a.Grants {
		if grant.Grantee.Type == "Group" && grant.Grantee.URI == AllUsersGroup {
			return true
		}
	}
	return false
}
