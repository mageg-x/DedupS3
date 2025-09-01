package meta

import (
	"encoding/xml"
	"time"
	"unsafe"
)

// PartObject 表示分段上传中的一个分段
type PartObject struct {
	// 对象标识信息
	BaseObject        // 必须匿名嵌入，且是第一个字段
	UploadID   string `json:"uploadId"`   // 所属上传任务ID
	PartNumber int    `json:"partNumber"` // 分段编号
	Owner      Owner  `json:"-"`          // 任务所有者
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
	UploadID           string            `json:"uploadId"`                     // 唯一上传ID
	Bucket             string            `json:"bucket"`                       // 目标存储桶
	Key                string            `json:"key"`                          // 对象键
	Initiated          time.Time         `json:"initiated"`                    // 任务创建时间
	StorageClass       string            `json:"storageClass,omitempty"`       // 存储类别
	Owner              Owner             `json:"owner,omitempty"`              // 任务所有者
	Initiator          Initiator         `json:"initiator,omitempty"`          // 任务发起者
	ContentType        string            `json:"contentType,omitempty"`        // 内容类型
	ContentEncoding    string            `json:"contentEncoding,omitempty"`    // 内容编码
	ContentLanguage    string            `json:"contentLanguage,omitempty"`    // 内容语言
	CacheControl       string            `json:"cacheControl,omitempty"`       // 缓存控制
	ContentDisposition string            `json:"contentDisposition,omitempty"` // 内容处置
	UserMetadata       map[string]string `json:"userMetadata,omitempty"`       // 用户自定义元数据
	Encryption         EncryptionInfo    `json:"encryption,omitempty"`         // 加密信息
	ACL                string            `json:"acl,omitempty"`                // 访问控制列表
	Tags               map[string]string `json:"tags,omitempty"`               // 对象标签
	Parts              []PartInfo        `json:"parts,omitempty"`              // 已上传的分段列表
	Created            time.Time         `json:"created"`                      // 创建时间
	DataLocation       string            `json:"dataLocation"`                 // 数据存储位置
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

// PartETag 表示分段ETag信息（用于完成上传）
type PartETag struct {
	PartNumber int    `json:"partNumber"` // 分段编号
	ETag       string `json:"etag"`       // 分段ETag
}

type CompleteMultipartUpload struct {
	XMLName xml.Name       `xml:"CompleteMultipartUpload"`
	Parts   []CompletePart `xml:"Part"`
}

type CompletePart struct {
	XMLName           xml.Name `xml:"Part"`
	PartNumber        int      `xml:"PartNumber"`
	ETag              string   `xml:"ETag"`
	Size              int64    `xml:"Size,omitempty"`
	ChecksumCRC32     string   `xml:"ChecksumCRC32,omitempty"`
	ChecksumCRC32C    string   `xml:"ChecksumCRC32C,omitempty"`
	ChecksumSHA1      string   `xml:"ChecksumSHA1,omitempty"`
	ChecksumSHA256    string   `xml:"ChecksumSHA256,omitempty"`
	ChecksumCRC64NVME string   `xml:"ChecksumCRC64NVME,omitempty"`
}

func PartToBaseObject(obj *PartObject) *BaseObject {
	return (*BaseObject)(unsafe.Pointer(obj))
}

func BaseObjectToPart(base *BaseObject) *PartObject {
	return (*PartObject)(unsafe.Pointer(base))
}

func (p *PartObject) Clone() *PartObject {
	cp := &PartObject{
		BaseObject: BaseObject{
			Bucket:       p.Bucket,
			Key:          p.Key,
			ETag:         p.ETag,
			Size:         p.Size,
			LastModified: p.LastModified,
			CreatedAt:    p.CreatedAt,
			DataLocation: p.DataLocation,
		},
		UploadID:   p.UploadID,
		PartNumber: p.PartNumber,
	}

	return cp
}
