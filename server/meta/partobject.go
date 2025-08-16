package meta

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

// PartObject 表示分段上传中的一个分段
type PartObject struct {
	UploadID     string    `json:"uploadId"`     // 所属上传任务ID
	Bucket       string    `json:"bucket"`       // 所属存储桶
	Key          string    `json:"key"`          // 对象键
	PartNumber   int       `json:"partNumber"`   // 分段编号
	ETag         string    `json:"etag"`         // 分段ETag
	Size         int64     `json:"size"`         // 分段大小
	LastModified time.Time `json:"lastModified"` // 最后修改时间
	DataLocation string    `json:"dataLocation"` // 数据存储位置
}

// NewPartObject 创建新的分段对象
func NewPartObject(uploadID, bucket, key string, partNumber int) *PartObject {
	return &PartObject{
		UploadID:     uploadID,
		Bucket:       bucket,
		Key:          key,
		PartNumber:   partNumber,
		LastModified: time.Now().UTC(),
	}
}

// Validate 验证分段对象是否有效
func (p *PartObject) Validate() error {
	if p.UploadID == "" {
		return errors.New("upload ID is required")
	}

	if p.Bucket == "" {
		return errors.New("bucket name is required")
	}

	if p.Key == "" {
		return errors.New("object key is required")
	}

	if p.PartNumber < 1 || p.PartNumber > 10000 {
		return errors.New("part number must be between 1 and 10000")
	}

	if p.Size < 0 {
		return errors.New("part size must be non-negative")
	}

	return nil
}

// CalculateETag 计算分段的ETag
func (p *PartObject) CalculateETag(data []byte) string {
	hash := md5.Sum(data)
	return hex.EncodeToString(hash[:])
}

// GeneratePartKey 生成分段对象的唯一键
func (p *PartObject) GeneratePartKey() string {
	return fmt.Sprintf("%s/%s/%s/part-%d", p.Bucket, p.Key, p.UploadID, p.PartNumber)
}

// ToPartInfo 转换为分段信息
func (p *PartObject) ToPartInfo() PartInfo {
	return PartInfo{
		PartNumber:   p.PartNumber,
		ETag:         p.ETag,
		Size:         p.Size,
		LastModified: p.LastModified,
	}
}

// PartInfo 表示分段信息（用于列表操作）
type PartInfo struct {
	PartNumber   int       `json:"partNumber" xml:"PartNumber"`
	ETag         string    `json:"etag" xml:"ETag"`
	Size         int64     `json:"size" xml:"Size"`
	LastModified time.Time `json:"lastModified" xml:"LastModified"`
}

// MultipartUpload 表示一个分段上传任务
type MultipartUpload struct {
	UploadID     string            `json:"uploadId"`     // 唯一上传ID
	Bucket       string            `json:"bucket"`       // 目标存储桶
	Key          string            `json:"key"`          // 对象键
	Initiated    time.Time         `json:"initiated"`    // 任务创建时间
	StorageClass string            `json:"storageClass"` // 存储类别
	Owner        Owner             `json:"owner"`        // 任务所有者
	Initiator    Initiator         `json:"initiator"`    // 任务发起者
	ContentType  string            `json:"contentType"`  // 内容类型
	UserMetadata map[string]string `json:"userMetadata"` // 用户自定义元数据
	Encryption   EncryptionInfo    `json:"encryption"`   // 加密信息
	ACL          string            `json:"acl"`          // 访问控制列表
	Tags         map[string]string `json:"tags"`         // 对象标签
	Parts        []PartInfo        `json:"parts"`        // 已上传的分段列表
}

// Initiator 表示任务发起者
type Initiator struct {
	ID          string `json:"id"`
	DisplayName string `json:"displayName"`
}

// EncryptionInfo 表示加密信息
type EncryptionInfo struct {
	Type   string `json:"type"`   // 加密类型 (AES256, aws:kms)
	KMSKey string `json:"kmsKey"` // KMS密钥ID
}

// NewMultipartUpload 创建新的分段上传任务
func NewMultipartUpload(bucket, key string) *MultipartUpload {
	return &MultipartUpload{
		UploadID:     generateUploadID(),
		Bucket:       bucket,
		Key:          key,
		Initiated:    time.Now().UTC(),
		StorageClass: "STANDARD",
		Parts:        make([]PartInfo, 0),
		UserMetadata: make(map[string]string),
		Tags:         make(map[string]string),
	}
}

// generateUploadID 生成唯一的上传ID
func generateUploadID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

// AddPart 添加或更新分段信息
func (m *MultipartUpload) AddPart(part PartInfo) {
	// 查找是否已存在相同分段号的分段
	for i, p := range m.Parts {
		if p.PartNumber == part.PartNumber {
			m.Parts[i] = part
			return
		}
	}

	// 如果不存在，添加新分段
	m.Parts = append(m.Parts, part)
}

// GetPart 获取指定分段号的分段信息
func (m *MultipartUpload) GetPart(partNumber int) (PartInfo, bool) {
	for _, p := range m.Parts {
		if p.PartNumber == partNumber {
			return p, true
		}
	}
	return PartInfo{}, false
}

// CompleteSize 计算已上传的总大小
func (m *MultipartUpload) CompleteSize() int64 {
	var total int64
	for _, p := range m.Parts {
		total += p.Size
	}
	return total
}

// ValidateParts 验证分段列表是否完整有效
func (m *MultipartUpload) ValidateParts(parts []PartETag) error {
	// 检查分段数量
	if len(parts) == 0 {
		return errors.New("at least one part must be specified")
	}

	// 检查分段顺序和完整性
	expectedPart := 1
	for _, reqPart := range parts {
		// 检查分段编号顺序
		if reqPart.PartNumber != expectedPart {
			return errors.New("parts must be in order")
		}
		expectedPart++

		// 检查分段是否存在
		found := false
		for _, upPart := range m.Parts {
			if upPart.PartNumber == reqPart.PartNumber {
				// 检查ETag是否匹配
				if upPart.ETag != reqPart.ETag {
					return fmt.Errorf("etag mismatch for part %d", reqPart.PartNumber)
				}
				found = true
				break
			}
		}

		if !found {
			return fmt.Errorf("part %d not found", reqPart.PartNumber)
		}
	}

	return nil
}

// PartETag 表示分段ETag信息（用于完成上传）
type PartETag struct {
	PartNumber int    `json:"partNumber"` // 分段编号
	ETag       string `json:"etag"`       // 分段ETag
}

// CalculateCompleteETag 计算完整对象的ETag
func CalculateCompleteETag(parts []PartETag) string {
	// 实际实现中，ETag通常是所有分段ETag的MD5的MD5
	// 格式为: "MD5SUM-NUM_PARTS"

	// 计算所有分段ETag的MD5
	hash := md5.New()
	for _, part := range parts {
		hash.Write([]byte(part.ETag))
	}
	compositeMD5 := hex.EncodeToString(hash.Sum(nil))

	return fmt.Sprintf("%s-%d", compositeMD5, len(parts))
}
