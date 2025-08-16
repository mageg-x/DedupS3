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
