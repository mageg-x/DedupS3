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

// ObjectLockConfiguration 表示对象锁定配置
type ObjectLockConfiguration struct {
	XMLName           xml.Name        `xml:"ObjectLockConfiguration" json:"objectLockConfiguration"`
	XMLNS             string          `xml:"xmlns,attr" json:"xmlns"`                    // 固定值为http://s3.amazonaws.com/doc/2006-03-01/
	ObjectLockEnabled string          `xml:"ObjectLockEnabled" json:"objectLockEnabled"` // Enabled
	Rule              *ObjectLockRule `xml:"Rule,omitempty" json:"rule"`

	CreatedAt time.Time `xml:"-" json:"createdAt"`
	UpdatedAt time.Time `xml:"-" json:"updatedAt"`
}

// ObjectLockRule 表示对象锁定规则
type ObjectLockRule struct {
	DefaultRetention *DefaultRetention `xml:"DefaultRetention" json:"defaultRetention"`
}

// DefaultRetention 表示默认保留设置
type DefaultRetention struct {
	Mode  string `xml:"Mode" json:"mode"` // GOVERNANCE | COMPLIANCE
	Days  int    `xml:"Days,omitempty" json:"days"`
	Years int    `xml:"Years,omitempty" json:"years"`
}

// Retention 表示对象保留设置
type Retention struct {
	Mode            string    `xml:"Mode"` // GOVERNANCE | COMPLIANCE
	RetainUntilDate time.Time `xml:"RetainUntilDate"`
}

// LegalHold 表示法律保留设置
type LegalHold struct {
	Status string `xml:"Status"` // ON | OFF
}

// Enable 启用对象锁定
func (o *ObjectLockConfiguration) Enable(mode string, days, years int) error {
	if o == nil {
		return errors.New("object lock config not initialized")
	}

	if mode != "GOVERNANCE" && mode != "COMPLIANCE" {
		return errors.New("invalid lock mode")
	}

	if days == 0 && years == 0 {
		return errors.New("retention period must be specified")
	}

	if days > 0 && years > 0 {
		return errors.New("cannot specify both days and years")
	}

	o.ObjectLockEnabled = "Enabled"
	o.Rule = &ObjectLockRule{
		DefaultRetention: &DefaultRetention{
			Mode:  mode,
			Days:  days,
			Years: years,
		},
	}
	o.UpdatedAt = time.Now().UTC()
	return nil
}

// Disable 禁用对象锁定
func (o *ObjectLockConfiguration) Disable() {
	if o == nil {
		return
	}
	o.ObjectLockEnabled = ""
	o.Rule = nil
	o.UpdatedAt = time.Now().UTC()
}

// IsEnabled 检查对象锁定是否启用
func (o *ObjectLockConfiguration) IsEnabled() bool {
	return o != nil && o.ObjectLockEnabled == "Enabled"
}

// IsDeletionAllowed 检查是否允许删除对象
func (r *Retention) IsDeletionAllowed(bypassGovernance bool) bool {
	if r == nil {
		return true
	}

	if time.Now().UTC().Before(r.RetainUntilDate) {
		if r.Mode == "COMPLIANCE" {
			return false
		}
		if r.Mode == "GOVERNANCE" && !bypassGovernance {
			return false
		}
	}

	return true
}

// IsLegalHoldActive 检查法律保留是否激活
func (l *LegalHold) IsLegalHoldActive() bool {
	return l != nil && l.Status == "ON"
}

// Validate 验证对象锁定配置是否有效
func (o *ObjectLockConfiguration) Validate() error {
	if o == nil {
		return nil // 无配置，视为有效
	}

	// 验证XML命名空间
	if o.XMLNS != "" && o.XMLNS != "http://s3.amazonaws.com/doc/2006-03-01/" {
		return errors.New("invalid xmlns for object lock configuration")
	}

	// 如果启用了对象锁定，需要验证规则
	if o.ObjectLockEnabled == "Enabled" {
		if o.Rule == nil {
			return errors.New("rule must be specified when object lock is enabled")
		}

		// 验证规则中的默认保留设置
		if o.Rule.DefaultRetention == nil {
			return errors.New("default retention must be specified in rule")
		}

		// 验证保留模式
		if o.Rule.DefaultRetention.Mode != "GOVERNANCE" && o.Rule.DefaultRetention.Mode != "COMPLIANCE" {
			return errors.New("invalid retention mode, must be 'GOVERNANCE' or 'COMPLIANCE'")
		}

		// 验证保留期
		if o.Rule.DefaultRetention.Days == 0 && o.Rule.DefaultRetention.Years == 0 {
			return errors.New("retention period must be specified")
		}

		if o.Rule.DefaultRetention.Days > 0 && o.Rule.DefaultRetention.Years > 0 {
			return errors.New("cannot specify both days and years for retention")
		}

		if o.Rule.DefaultRetention.Days < 0 || o.Rule.DefaultRetention.Years < 0 {
			return errors.New("retention period must be positive")
		}
	} else if o.ObjectLockEnabled != "" {
		// ObjectLockEnabled只能是"Enabled"或空字符串
		return errors.New("invalid value for ObjectLockEnabled, must be 'Enabled' or empty")
	}

	return nil
}
