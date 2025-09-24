package meta

import (
	"encoding/xml"
	"time"
	"unsafe"
)

const (
	MIN_PART_SIZE = 5 * 1024 * 1024
	MAX_PART_ID   = 10000
)

// PartObject 表示分段上传中的一个分段
type PartObject struct {
	// 对象标识信息
	BaseObject             // 必须匿名嵌入，且是第一个字段
	UploadID     string    `json:"uploadId" xml:"UploadId,omitempty"`                   // 所属上传任务ID
	PartNumber   int       `json:"partNumber" xml:"PartNumber"`                         // 分段编号
	Owner        Owner     `json:"owner,omitempty" xml:"Owner,omitempty"`               // 任务所有者
	Initiator    Initiator `json:"initiator,omitempty" xml:"Initiator,omitempty"`       // 任务发起者
	StorageClass string    `json:"storageClass,omitempty" xml:"StorageClass,omitempty"` // 存储类别
}

// PartInfo 表示分段信息（用于列表操作）
type PartInfo struct {
	PartNumber   int       `json:"partNumber" xml:"PartNumber"`
	ETag         Etag      `json:"etag" xml:"ETag"` // 注意：S3 返回的是带引号的 ETag
	Size         int64     `json:"size" xml:"Size"`
	LastModified time.Time `json:"lastModified" xml:"LastModified"`

	// Checksum 字段（可选）
	ChecksumCRC32  string `json:"checksumCrc32,omitempty" xml:"ChecksumCRC32,omitempty"`
	ChecksumCRC32C string `json:"checksumCrc32c,omitempty" xml:"ChecksumCRC32C,omitempty"`
	ChecksumSHA1   string `json:"checksumSha1,omitempty" xml:"ChecksumSHA1,omitempty"`
	ChecksumSHA256 string `json:"checksumSha256,omitempty" xml:"ChecksumSHA256,omitempty"`
}

// MultipartUpload 表示一个分段上传任务
type MultipartUpload struct {
	UploadID           string            `json:"uploadId" xml:"UploadId"`                                         // 唯一上传ID
	Bucket             string            `json:"bucket" xml:"Bucket"`                                             // 目标存储桶
	Key                string            `json:"key" xml:"Key"`                                                   // 对象键
	StorageClass       string            `json:"storageClass,omitempty" xml:"StorageClass,omitempty"`             // 存储类别
	Owner              Owner             `json:"owner,omitempty" xml:"Owner,omitempty"`                           // 任务所有者
	Initiator          Initiator         `json:"initiator,omitempty" xml:"Initiator,omitempty"`                   // 任务发起者
	ContentType        string            `json:"contentType,omitempty" xml:"ContentType,omitempty"`               // 内容类型
	ContentEncoding    string            `json:"contentEncoding,omitempty" xml:"ContentEncoding,omitempty"`       // 内容编码
	ContentLanguage    string            `json:"contentLanguage,omitempty" xml:"ContentLanguage,omitempty"`       // 内容语言
	CacheControl       string            `json:"cacheControl,omitempty" xml:"CacheControl,omitempty"`             // 缓存控制
	ContentDisposition string            `json:"contentDisposition,omitempty" xml:"ContentDisposition,omitempty"` // 内容处置
	UserMetadata       map[string]string `json:"userMetadata,omitempty" xml:"UserMetadata,omitempty"`             // 用户自定义元数据
	Encryption         EncryptionInfo    `json:"encryption,omitempty" xml:"Encryption,omitempty"`                 // 加密信息
	ACL                string            `json:"acl,omitempty" xml:"ACL,omitempty"`                               // 访问控制列表
	Tags               map[string]string `json:"tags,omitempty" xml:"Tags,omitempty"`                             // 对象标签
	Parts              []PartInfo        `json:"parts,omitempty" xml:"Parts>Part,omitempty"`                      // 已上传的分段列表
	Created            time.Time         `json:"created" xml:"Created"`                                           // 创建时间
	DataLocation       string            `json:"dataLocation" xml:"DataLocation,omitempty"`                       // 数据存储位置
}

// Initiator 表示任务发起者
type Initiator struct {
	ID          string `json:"id" xml:"ID"`
	DisplayName string `json:"displayName" xml:"DisplayName,omitempty"`
}

// EncryptionInfo 表示加密信息
type EncryptionInfo struct {
	Type   string `json:"type" xml:"Type"`                 // 加密类型 (AES256, aws:kms)
	KMSKey string `json:"kmsKey" xml:"KMSKeyId,omitempty"` // KMS密钥ID（S3 标准字段名）
}

// PartETag 表示分段ETag信息（用于完成上传）
type PartETag struct {
	PartNumber int  `json:"partNumber" xml:"PartNumber"`
	ETag       Etag `json:"etag" xml:"ETag"`
}

// CompleteMultipartUpload 表示完成分段上传的请求体
type CompleteMultipartUpload struct {
	XMLName xml.Name       `xml:"CompleteMultipartUpload"`
	Parts   []CompletePart `xml:"Part"` // 注意：S3 要求是多个 <Part> 平级元素
}

// CompletePart 表示要完成的分段
type CompletePart struct {
	XMLName           xml.Name `xml:"Part"`
	PartNumber        int      `xml:"PartNumber"`
	ETag              Etag     `xml:"ETag"`           // 必须带引号
	Size              int64    `xml:"Size,omitempty"` // 可选，某些场景需要
	ChecksumCRC32     string   `xml:"ChecksumCRC32,omitempty"`
	ChecksumCRC32C    string   `xml:"ChecksumCRC32C,omitempty"`
	ChecksumSHA1      string   `xml:"ChecksumSHA1,omitempty"`
	ChecksumSHA256    string   `xml:"ChecksumSHA256,omitempty"`
	ChecksumCRC64NVME string   `xml:"ChecksumCRC64NVME,omitempty"`
}

// 转换函数
func PartToBaseObject(obj *PartObject) *BaseObject {
	return (*BaseObject)(unsafe.Pointer(obj))
}

func BaseObjectToPart(base *BaseObject) *PartObject {
	return (*PartObject)(unsafe.Pointer(base))
}

func (p *PartObject) Clone() *PartObject {
	cp := &PartObject{}
	*cp = *p // 浅拷贝所有字段
	cp.Chunks = append([]string(nil), p.Chunks...)

	return cp
}
