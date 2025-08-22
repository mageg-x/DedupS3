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

// VersioningConfiguration 表示版本控制配置
type VersioningConfiguration struct {
	XMLName   xml.Name `xml:"VersioningConfiguration"`
	Status    string   `xml:"Status"`              // Enabled | Suspended
	MfaDelete string   `xml:"MfaDelete,omitempty"` // Enabled | Disabled

	CreatedAt time.Time `xml:"-"`
	UpdatedAt time.Time `xml:"-"`
}

// Enable 启用版本控制
func (v *VersioningConfiguration) Enable() {
	if v == nil {
		return
	}
	v.Status = "Enabled"
	v.UpdatedAt = time.Now().UTC()
}

// Suspend 暂停版本控制
func (v *VersioningConfiguration) Suspend() {
	if v == nil {
		return
	}
	v.Status = "Suspended"
	v.UpdatedAt = time.Now().UTC()
}

// EnableMFADelete 启用MFA删除
func (v *VersioningConfiguration) EnableMFADelete() error {
	if v == nil {
		return errors.New("versioning config not initialized")
	}

	if v.Status != "Enabled" {
		return errors.New("versioning must be enabled to configure MFA delete")
	}

	v.MfaDelete = "Enabled"
	v.UpdatedAt = time.Now().UTC()
	return nil
}

// DisableMFADelete 禁用MFA删除
func (v *VersioningConfiguration) DisableMFADelete() {
	if v == nil {
		return
	}
	v.MfaDelete = "Disabled"
	v.UpdatedAt = time.Now().UTC()
}

// IsEnabled 检查版本控制是否启用
func (v *VersioningConfiguration) IsEnabled() bool {
	return v != nil && v.Status == "Enabled"
}

// IsMFADeleteEnabled 检查MFA删除是否启用
func (v *VersioningConfiguration) IsMFADeleteEnabled() bool {
	return v != nil && v.MfaDelete == "Enabled"
}
