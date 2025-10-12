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
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
	"unsafe"
)

const (
	MAX_OBJECT_SIZE = 5 * 1024 * 1024 * 1024 * 1024
)

const (
	NORMAL_OBJECT = iota
	PART_OBJECT
)

type Etag string

type InlineChunk struct {
	Compress bool   `json:"compress" xml:"Compress"`
	Data     []byte `json:"data" xml:"Data"`
}

type BaseObject struct {
	Bucket       string    `json:"bucket" xml:"Bucket"`             // 所属存储桶
	Key          string    `json:"key" xml:"Key"`                   // 对象键
	ETag         Etag      `json:"etag" xml:"ETag"`                 // 分段ETag
	Size         int64     `json:"size" xml:"Size"`                 // 分段大小
	Chunks       []string  `json:"chunks" xml:"Chunks>Chunk"`       // chunk 索引（数组嵌套）
	LastModified time.Time `json:"lastModified" xml:"LastModified"` // 最后修改时间
	CreatedAt    time.Time `json:"createdAt" xml:"CreatedAt"`       // 创建时间
	// 数据位置（实际存储系统中使用）
	DataLocation string `json:"dataLocation" xml:"-"` // 对象数据存储位置，不序列化到 XML
	ObjType      int    `json:"-" xml:"-"`            // 辅助字段，仅仅存在内存中
}

// Object 表示存储桶中的一个对象
type Object struct {
	// 对象标识信息
	BaseObject                // 必须匿名嵌入，且是第一个字段
	VersionID    string       `json:"versionId" xml:"VersionId"`      // 版本ID（如果启用版本控制）
	ChunksInline *InlineChunk `json:"chunk_inline" xml:"ChunkInline"` // inline chunk

	// 内容信息
	ContentType        string `json:"contentType" xml:"ContentType"`               // MIME类型
	ContentEncoding    string `json:"contentEncoding" xml:"ContentEncoding"`       // 内容编码
	ContentLanguage    string `json:"contentLanguage" xml:"ContentLanguage"`       // 内容语言
	ContentDisposition string `json:"contentDisposition" xml:"ContentDisposition"` // 内容处置
	CacheControl       string `json:"cacheControl" xml:"CacheControl"`             // 缓存控制

	// 加密信息
	EncryptionType string `json:"encryptionType" xml:"EncryptionType"` // 加密类型 (AES256, aws:kms)
	KMSKeyID       string `json:"kmsKeyId" xml:"KMSKeyID"`             // KMS密钥ID

	// 存储信息
	StorageClass  string `json:"storageClass" xml:"StorageClass"`   // 存储类别
	RestoreStatus string `json:"restoreStatus" xml:"RestoreStatus"` // 恢复状态

	// 元数据信息
	UserMetadata map[string]string `json:"userMetadata" xml:"-"` // 用户自定义元数据
	Tags         map[string]string `json:"tags" xml:"-"`         // 对象标签

	// 锁定信息
	LockMode        string    `json:"lockMode" xml:"LockMode"`               // 锁定模式
	LockRetainUntil time.Time `json:"lockRetainUntil" xml:"LockRetainUntil"` // 锁定保留截止时间
	LegalHold       bool      `json:"legalHold" xml:"LegalHold"`             // 法律保留状态

	// 所有者信息
	Owner Owner                `json:"owner" xml:"Owner"` // 对象所有者
	ACL   *AccessControlPolicy `json:"acl" xml:"Acl"`     // 访问控制列表
}

// Owner 表示对象所有者信息
type Owner struct {
	ID          string `json:"id" xml:"ID,omitempty"`                   // 所有者ID
	DisplayName string `json:"displayName" xml:"DisplayName,omitempty"` // 显示名称
}

func FormatObjectARN(accountID, bucketName, ObjKey string) string {
	return "arn:aws:s3:::" + bucketName + "/" + ObjKey
}

func ObjectToBaseObject(obj *Object) *BaseObject {
	return (*BaseObject)(unsafe.Pointer(obj))
}

func BaseObjectToObject(base *BaseObject) *Object {
	return (*Object)(unsafe.Pointer(base))
}

// NewObject 创建新的对象
func NewObject(bucket, key string) *Object {
	now := time.Now().UTC()
	return &Object{
		BaseObject: BaseObject{
			Bucket:       bucket,
			Key:          key,
			CreatedAt:    now,
			LastModified: now,
			ObjType:      NORMAL_OBJECT,
			Chunks:       make([]string, 0),
		},
		ContentType:  "application/octet-stream",
		StorageClass: STANDARD_CLASS_STORAGE,
		UserMetadata: make(map[string]string),
		Tags:         make(map[string]string),
	}
}

// 构建资源ARN
func BuildResourceARN(bucketName, objectKey string) string {
	if objectKey != "" {
		return "arn:aws:s3:::" + bucketName + "/" + objectKey
	}
	return "arn:aws:s3:::" + bucketName
}

// MarshalXML 实现：输出 <ETag>"actual-etag"</ETag>
func (e Etag) MarshalXML(enc *xml.Encoder, start xml.StartElement) error {
	// 我们要输出的是字符串内容为 "actual-etag" 的文本节点
	quoted := `"` + string(e) + `"`
	return enc.EncodeElement(quoted, start)
}

// UnmarshalXML 实现：输入 <ETag>"actual-etag"</ETag> → 存为 actual-etag
func (e *Etag) UnmarshalXML(dec *xml.Decoder, start xml.StartElement) error {
	var quoted string
	if err := dec.DecodeElement(&quoted, &start); err != nil {
		return err
	}

	// 去掉外层引号
	unquoted := strings.Trim(quoted, `"`)
	*e = Etag(unquoted)
	return nil
}

// SetStorageClass 设置存储类别
func (o *Object) SetStorageClass(storageClass string) error {
	validClasses := map[string]bool{
		"STANDARD":                 true,
		"STANDARD_IA":              true,
		"ONEZONE_IA":               true,
		"GLACIER":                  true,
		"GLACIER_IR":               true,
		"DEEP_ARCHIVE":             true,
		"INTELLIGENT_TIERING":      true,
		"OUTPOSTS":                 true,
		"GLACIER_DIRECT_RETRIEVAL": true,
	}

	if _, valid := validClasses[storageClass]; !valid {
		return errors.New("invalid storage class")
	}

	o.StorageClass = storageClass
	return nil
}

// ToHeaders 将元数据转换为HTTP头
func (o *Object) ToHeaders() http.Header {
	headers := make(http.Header)

	// 标准HTTP头
	headers.Set("Content-Type", o.ContentType)
	headers.Set("Content-Length", fmt.Sprintf("%d", o.Size))
	headers.Set("Content-Encoding", o.ContentEncoding)
	headers.Set("Content-Language", o.ContentLanguage)
	headers.Set("Content-Disposition", o.ContentDisposition)
	headers.Set("Cache-Control", o.CacheControl)
	headers.Set("Last-Modified", o.LastModified.Format(time.RFC1123))
	headers.Set("ETag", string(o.ETag))

	// S3特定头
	headers.Set("x-amz-storage-class", o.StorageClass)
	headers.Set("x-amz-restore", o.RestoreStatus)
	headers.Set("x-amz-version-id", o.VersionID)

	// 加密头
	if o.EncryptionType != "" {
		headers.Set("x-amz-server-side-encryption", o.EncryptionType)
		if o.KMSKeyID != "" {
			headers.Set("x-amz-server-side-encryption-aws-kms-key-id", o.KMSKeyID)
		}
	}

	// 锁定头
	if o.LockMode != "" {
		headers.Set("x-amz-object-lock-mode", o.LockMode)
		headers.Set("x-amz-object-lock-retain-until-date", o.LockRetainUntil.Format(time.RFC3339))
	}
	if o.LegalHold {
		headers.Set("x-amz-object-lock-legal-hold", "ON")
	} else {
		headers.Set("x-amz-object-lock-legal-hold", "OFF")
	}

	// 用户元数据头
	for key, value := range o.UserMetadata {
		headerKey := "x-amz-meta-" + key
		headers.Set(headerKey, value)
	}

	// 标签头（如果存在）
	if len(o.Tags) > 0 {
		tagParts := make([]string, 0, len(o.Tags))
		for key, value := range o.Tags {
			tagParts = append(tagParts, fmt.Sprintf("%s=%s", key, value))
		}
		headers.Set("x-amz-tagging", strings.Join(tagParts, "&"))
	}

	return headers
}

// ParseHeaders 从HTTP头解析元数据
func (o *Object) ParseHeaders(headers http.Header) {
	// 标准HTTP头
	o.ContentType = headers.Get("Content-Type")
	o.ContentEncoding = headers.Get("Content-Encoding")
	o.ContentLanguage = headers.Get("Content-Language")
	o.ContentDisposition = headers.Get("Content-Disposition")
	o.CacheControl = headers.Get("Cache-Control")

	if lastModified := headers.Get("Last-Modified"); lastModified != "" {
		if t, err := time.Parse(time.RFC1123, lastModified); err == nil {
			o.LastModified = t
		}
	}

	// S3特定头
	o.StorageClass = headers.Get("x-amz-storage-class")
	o.RestoreStatus = headers.Get("x-amz-restore")
	o.VersionID = headers.Get("x-amz-version-id")
	etag := headers.Get("ETag")
	etag = strings.Trim(etag, "\"")
	o.ETag = Etag(etag)

	// 加密头
	o.EncryptionType = headers.Get("x-amz-server-side-encryption")
	o.KMSKeyID = headers.Get("x-amz-server-side-encryption-aws-kms-key-id")

	// 锁定头
	o.LockMode = headers.Get("x-amz-object-lock-mode")
	if retainUntil := headers.Get("x-amz-object-lock-retain-until-date"); retainUntil != "" {
		if t, err := time.Parse(time.RFC3339, retainUntil); err == nil {
			o.LockRetainUntil = t
		}
	}
	legalHold := headers.Get("x-amz-object-lock-legal-hold")
	o.LegalHold = legalHold == "ON"

	// 用户元数据
	o.UserMetadata = make(map[string]string)
	for key, values := range headers {
		if strings.HasPrefix(key, "x-amz-meta-") {
			metaKey := strings.TrimPrefix(key, "x-amz-meta-")
			if len(values) > 0 {
				o.UserMetadata[metaKey] = values[0]
			}
		}
	}

	// 标签
	if tagging := headers.Get("x-amz-tagging"); tagging != "" {
		o.Tags = parseTaggingHeader(tagging)
	}
}

// parseTaggingHeader 解析标签头
func parseTaggingHeader(tagging string) map[string]string {
	tags := make(map[string]string)
	pairs := strings.Split(tagging, "&")

	for _, pair := range pairs {
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) == 2 {
			key := parts[0]
			value := parts[1]
			tags[key] = value
		}
	}

	return tags
}

// CalculateETag 计算对象的ETag
func (o *Object) CalculateETag(data io.Reader) (string, error) {
	hash := md5.New()
	if _, err := io.Copy(hash, data); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// Copy 创建对象的副本
func (o *Object) Clone() *Object {
	cp := &Object{}
	*cp = *o // 浅拷贝所有字段

	// 深拷贝map
	cp.UserMetadata = make(map[string]string)
	for k, v := range o.UserMetadata {
		cp.UserMetadata[k] = v
	}

	cp.Tags = make(map[string]string)
	for k, v := range o.Tags {
		cp.Tags[k] = v
	}

	// 深拷贝Chunks数组
	cp.Chunks = make([]string, len(o.Chunks))
	copy(cp.Chunks, o.Chunks)

	if o.ChunksInline != nil {
		cp.ChunksInline = &InlineChunk{
			Compress: o.ChunksInline.Compress,
			Data:     make([]byte, len(o.ChunksInline.Data)),
		}
		copy(cp.ChunksInline.Data, o.ChunksInline.Data)
	}

	// 深拷贝ACL字段
	if o.ACL != nil {
		cp.ACL = &AccessControlPolicy{
			Owner:             o.ACL.Owner, // 浅拷贝，CanonicalUser是值类型
			AccessControlList: AccessControlList{},
		}
		// 深拷贝Grants切片
		cp.ACL.AccessControlList.Grants = make([]Grant, len(o.ACL.AccessControlList.Grants))
		for i, grant := range o.ACL.AccessControlList.Grants {
			cp.ACL.AccessControlList.Grants[i] = Grant{
				Grantee:    grant.Grantee, // 浅拷贝，Grantee是值类型
				Permission: grant.Permission,
			}
		}
	}

	return cp
}
