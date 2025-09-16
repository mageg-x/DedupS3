package meta

import (
	"encoding/xml"
	"errors"
	"time"
)

// ObjectLockConfiguration 表示对象锁定配置
type ObjectLockConfiguration struct {
	XMLName           xml.Name        `xml:"ObjectLockConfiguration"`
	XMLNS             string          `xml:"xmlns,attr"`        // 固定值为http://s3.amazonaws.com/doc/2006-03-01/
	ObjectLockEnabled string          `xml:"ObjectLockEnabled"` // Enabled
	Rule              *ObjectLockRule `xml:"Rule,omitempty"`

	CreatedAt time.Time `xml:"-"`
	UpdatedAt time.Time `xml:"-"`
}

// ObjectLockRule 表示对象锁定规则
type ObjectLockRule struct {
	DefaultRetention *DefaultRetention `xml:"DefaultRetention"`
}

// DefaultRetention 表示默认保留设置
type DefaultRetention struct {
	Mode  string `xml:"Mode"` // GOVERNANCE | COMPLIANCE
	Days  int    `xml:"Days,omitempty"`
	Years int    `xml:"Years,omitempty"`
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
