package meta

import (
	"encoding/xml"
	"errors"
	"time"
)

// BucketSSEConfiguration 表示存储桶加密配置 (AWS S3 规范)
type BucketSSEConfiguration struct {
	XMLName xml.Name  `xml:"ServerSideEncryptionConfiguration" json:"serverSideEncryptionConfiguration"`
	XMLNS   string    `xml:"xmlns,attr" json:"xmlns"` // 固定值为http://s3.amazonaws.com/doc/2006-03-01/
	Rule    *SSERule  `xml:"Rule" json:"rule"`        // AWS 规范中 Rule 是单数形式，但允许多个规则
	Rules   []SSERule `xml:"-" json:"rules"`          // 内部使用，方便处理多个规则

	CreatedAt time.Time `xml:"-" json:"createdAt"`
	UpdatedAt time.Time `xml:"-" json:"updatedAt"`
}

// SSERule 表示加密规则 (AWS S3 规范)
type SSERule struct {
	ApplyServerSideEncryptionByDefault *SSEDefault `xml:"ApplyServerSideEncryptionByDefault" json:"applyServerSideEncryptionByDefault"`
	BucketKeyEnabled                   bool        `xml:"BucketKeyEnabled,omitempty" json:"bucketKeyEnabled"`
}

// SSEDefault 表示默认加密设置 (AWS S3 规范)
type SSEDefault struct {
	SSEAlgorithm   string `xml:"SSEAlgorithm" json:"sseAlgorithm"` // AES256 | aws:kms | aws:kms:dsse
	KMSMasterKeyID string `xml:"KMSMasterKeyID,omitempty" json:"kmsMasterKeyId"`
}

// ApplyDefaultEncryption 应用默认加密设置 (符合AWS规范)
func (s *BucketSSEConfiguration) ApplyDefaultEncryption(algorithm, kmsKeyID string, bucketKeyEnabled bool) error {
	if s == nil {
		return errors.New("SSE config not initialized")
	}

	// 验证算法类型 (支持AWS官方算法类型)
	validAlgorithms := map[string]bool{
		"AES256":       true,
		"aws:kms":      true,
		"aws:kms:dsse": true,
	}
	if !validAlgorithms[algorithm] {
		return errors.New("invalid encryption algorithm. Valid options: AES256, aws:kms, aws:kms:dsse")
	}

	// KMS 算法需要密钥ID
	if algorithm != "AES256" && kmsKeyID == "" {
		return errors.New("KMS key ID required for KMS encryption")
	}

	// 创建新规则
	newRule := SSERule{
		ApplyServerSideEncryptionByDefault: &SSEDefault{
			SSEAlgorithm:   algorithm,
			KMSMasterKeyID: kmsKeyID,
		},
		BucketKeyEnabled: bucketKeyEnabled,
	}

	// 更新规则集合
	if len(s.Rules) == 0 {
		s.Rules = []SSERule{newRule}
	} else {
		// 替换第一个规则（默认规则）
		s.Rules[0] = newRule
	}
	s.Rule = &s.Rules[0] // 保持XML兼容

	s.UpdatedAt = time.Now().UTC()
	return nil
}

// IsEnabled 检查加密是否启用
func (s *BucketSSEConfiguration) IsEnabled() bool {
	if s == nil || len(s.Rules) == 0 {
		return false
	}

	for _, rule := range s.Rules {
		if rule.ApplyServerSideEncryptionByDefault != nil && rule.ApplyServerSideEncryptionByDefault.SSEAlgorithm != "" {
			return true
		}
	}
	return false
}

// Algorithm 获取加密算法
func (s *BucketSSEConfiguration) Algorithm() string {
	if s == nil || len(s.Rules) == 0 || s.Rules[0].ApplyServerSideEncryptionByDefault == nil {
		return ""
	}
	return s.Rules[0].ApplyServerSideEncryptionByDefault.SSEAlgorithm
}

// KMSKeyID 获取KMS密钥ID
func (s *BucketSSEConfiguration) KMSKeyID() string {
	if s == nil || len(s.Rules) == 0 || s.Rules[0].ApplyServerSideEncryptionByDefault == nil {
		return ""
	}
	return s.Rules[0].ApplyServerSideEncryptionByDefault.KMSMasterKeyID
}

// IsBucketKeyEnabled 检查桶密钥是否启用
func (s *BucketSSEConfiguration) IsBucketKeyEnabled() bool {
	return s != nil && len(s.Rules) > 0 && s.Rules[0].BucketKeyEnabled
}

// MarshalXML 自定义XML序列化 (符合AWS规范)
func (s *BucketSSEConfiguration) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	start.Name = xml.Name{Local: "ServerSideEncryptionConfiguration"}
	if err := e.EncodeToken(start); err != nil {
		return err
	}

	// 显式序列化所有规则
	for _, rule := range s.Rules {
		if err := e.EncodeElement(rule, xml.StartElement{Name: xml.Name{Local: "Rule"}}); err != nil {
			return err
		}
	}

	return e.EncodeToken(xml.EndElement{Name: start.Name})
}

// UnmarshalXML 自定义XML反序列化 (符合AWS规范)
func (s *BucketSSEConfiguration) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	type alias BucketSSEConfiguration
	aux := struct {
		Rules []SSERule `xml:"Rule"`
		*alias
	}{
		alias: (*alias)(s),
	}

	if err := d.DecodeElement(&aux, &start); err != nil {
		return err
	}

	s.Rules = aux.Rules
	if len(s.Rules) > 0 {
		s.Rule = &s.Rules[0]
	}
	return nil
}
