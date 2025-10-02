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
package http

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"net/http"

	"github.com/mageg-x/boulder/internal/logger"
)

// 通用响应结构
type AdminResponse struct {
	Code int         `json:"code"`
	Msg  string      `json:"msg"`
	Data interface{} `json:"data,omitempty"` // omitempty 表示 nil 时不输出
}

// 快捷错误响应函数（如密码错误）
func AdminWriteJSONError(w http.ResponseWriter, r *http.Request, code int, msg string, data interface{}, statusCode int) {
	// 构造响应
	resp := AdminResponse{
		Code: code,
		Msg:  msg,
		Data: data,
	}

	// 使用 json.NewEncoder 避免缓冲，更高效
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(statusCode) // 设置 HTTP 状态码
	_ = json.NewEncoder(w).Encode(resp)
}

// AWSErrorResponse AWS 错误响应结构 (同时支持XML和JSON格式)
type AWSErrorResponse struct {
	XMLName   xml.Name `xml:"Error" json:",omitempty"`
	XMLNS     string   `xml:"xmlns,attr" json:",omitempty"`
	Code      string   `xml:"Code" json:"Code"`
	Message   string   `xml:"Message" json:"Message"`
	Resource  string   `xml:"Resource,omitempty" json:"Resource,omitempty"`
	Field     string   `xml:"Field,omitempty" json:"Field,omitempty"`
	RequestID string   `xml:"RequestId" json:"RequestId"`
}

// AWSRequestID RequestIDKey 用于上下文存储
type AWSRequestID struct{}

// GetRequestID 从上下文中获取 Request ID
func GetRequestID(ctx context.Context) string {
	if id, ok := ctx.Value(AWSRequestID{}).(string); ok {
		return id
	}
	return ""
}

// WriteAWSError 写入符合AWS规范的XML错误响应
func WriteAWSError(w http.ResponseWriter, r *http.Request, code, message string, status int) {
	// 获取请求ID
	requestID := GetRequestID(r.Context())

	// 创建错误响应
	errorResponse := AWSErrorResponse{
		Code:      code,
		Message:   message,
		RequestID: requestID,
		XMLNS:     "http://s3.amazonaws.com/doc/2006-03-01/", // 设置AWS S3 XML命名空间
	}

	// 写入响应
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("x-amz-request-id", requestID)
	w.WriteHeader(status)

	// 添加XML声明
	xmlHeader := []byte(xml.Header)
	if _, err := w.Write(xmlHeader); err != nil {
		logger.GetLogger("boulder").Errorf("Failed to write XML header: %v", err)
		return
	}

	// 序列化为XML
	encoder := xml.NewEncoder(w)
	encoder.Indent("", "  ") // 添加缩进提高可读性
	if err := encoder.Encode(errorResponse); err != nil {
		logger.GetLogger("boulder").Errorf("Failed to marshal error response: %v", err)
		// 尝试回退到简单错误响应，包含命名空间
		fallback := []byte(`<Error xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Code>InternalError</Code><Message>Failed to generate error response</Message><RequestId>` + requestID + `</RequestId></Error>`)
		w.Write(fallback)
	}
}

func WriteAWSErr(w http.ResponseWriter, r *http.Request, code APIErrorCode) {
	apiErr := ToApiErr(code)
	WriteAWSError(w, r, apiErr.Code, apiErr.Description, apiErr.HTTPStatusCode)
}

// WriteAWSSuc 写入XML成功响应
func WriteAWSSuc(w http.ResponseWriter, r *http.Request, data interface{}) {
	// 获取请求ID
	requestID := GetRequestID(r.Context())

	// 写入响应头
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("x-amz-request-id", requestID)
	w.WriteHeader(http.StatusOK)

	// 添加XML声明
	xmlHeader := []byte(xml.Header)
	if _, err := w.Write(xmlHeader); err != nil {
		logger.GetLogger("boulder").Errorf("Failed to write XML header: %v", err)
		return
	}

	// 序列化数据为XML
	encoder := xml.NewEncoder(w)
	encoder.Indent("", "  ") // 添加缩进提高可读性
	if err := encoder.Encode(data); err != nil {
		logger.GetLogger("boulder").Errorf("Failed to marshal success response: %v", err)
		// 序列化失败时回退到错误响应
		WriteAWSError(w, r, "InternalError", "Failed to generate response", http.StatusInternalServerError)
		return
	}
}

// WriteAWSJSONError 写入符合AWS规范的JSON错误响应
func WriteAWSJSONError(w http.ResponseWriter, r *http.Request, code, message string, status int) {
	// 获取请求ID
	requestID := GetRequestID(r.Context())

	// 创建错误响应（使用合并后的结构体）
	errorResponse := AWSErrorResponse{
		Code:      code,
		Message:   message,
		RequestID: requestID,
	}

	// 写入响应头
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("x-amz-request-id", requestID)
	w.WriteHeader(status)

	// 序列化数据为JSON
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ") // 添加缩进提高可读性
	if err := encoder.Encode(errorResponse); err != nil {
		logger.GetLogger("boulder").Errorf("Failed to marshal JSON error response: %v", err)
		// 尝试回退到简单错误响应
		fallback := []byte(`{"Code":"InternalError","Message":"Failed to generate error response","RequestId":"` + requestID + `"}`)
		w.Write(fallback)
	}
}

// WriteAWSJSONErr 写入符合AWS规范的JSON错误响应（使用APIErrorCode）
func WriteAWSJSONErr(w http.ResponseWriter, r *http.Request, code APIErrorCode) {
	apiErr := ToApiErr(code)
	WriteAWSJSONError(w, r, apiErr.Code, apiErr.Description, apiErr.HTTPStatusCode)
}

// WriteAWSJSONSuc 写入JSON成功响应
func WriteAWSJSONSuc(w http.ResponseWriter, r *http.Request, data interface{}) {
	// 获取请求ID
	requestID := GetRequestID(r.Context())

	// 写入响应头
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("x-amz-request-id", requestID)
	w.WriteHeader(http.StatusOK)

	// 序列化数据为JSON
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ") // 添加缩进提高可读性
	if err := encoder.Encode(data); err != nil {
		logger.GetLogger("boulder").Errorf("Failed to marshal JSON response: %v", err)
		// 序列化失败时回退到错误响应
		WriteAWSError(w, r, "InternalError", "Failed to generate JSON response", http.StatusInternalServerError)
		return
	}
}
