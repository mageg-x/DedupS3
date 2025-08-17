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
	"net/http"
	"time"

	"github.com/mageg-x/boulder/internal/logger"
)

// AWSErrorResponse AWS 错误响应结构
type AWSErrorResponse struct {
	Error     AWSError `json:"error"`
	RequestID string   `json:"requestId"`
}

// AWSError AWS 错误详情结构
type AWSError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	// 可选字段
	Resource string `json:"resource,omitempty"`
	Field    string `json:"field,omitempty"`
}

// AWSRequestID RequestIDKey 用于上下文存储
type AWSRequestID struct{}

// SuccessResponse 成功响应结构
type SuccessResponse struct {
	Data      interface{} `json:"data,omitempty"`
	RequestID string      `json:"requestId"`
	Timestamp time.Time   `json:"timestamp"`
}

// GetRequestID 从上下文中获取 Request ID
func GetRequestID(ctx context.Context) string {
	if id, ok := ctx.Value(AWSRequestID{}).(string); ok {
		return id
	}
	return ""
}

// WriteAWSError 写入符合AWS规范的JSON错误响应
func WriteAWSError(w http.ResponseWriter, r *http.Request, code, message string, status int) {
	// 获取请求ID
	requestID := GetRequestID(r.Context())

	// 创建错误响应
	errorResponse := AWSErrorResponse{
		Error: AWSError{
			Code:    code,
			Message: message,
		},
		RequestID: requestID,
	}

	// 写入响应
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("x-amz-request-id", requestID)
	w.WriteHeader(status)

	// 序列化为JSON
	jsonData, err := json.Marshal(errorResponse)
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to marshal error response: %v", err)
		w.Write([]byte(`{"error":{"code":"InternalError","message":"Failed to generate error response"},"requestId":"` + requestID + `"}`))
		return
	}

	w.Write(jsonData)
}

func WriteAWSErr(w http.ResponseWriter, r *http.Request, code APIErrorCode) {
	apiErr := ToApiErr(code)
	WriteAWSError(w, r, apiErr.Code, apiErr.Description, apiErr.HTTPStatusCode)
}

// WriteAWSSuc 写入成功响应
func WriteAWSSuc(w http.ResponseWriter, r *http.Request, data interface{}) {
	// 获取请求ID
	requestID := GetRequestID(r.Context())
	// 创建成功响应
	successResponse := SuccessResponse{
		Data:      data,
		RequestID: requestID,
		Timestamp: time.Now().UTC(),
	}

	// 写入响应
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("x-amz-request-id", requestID)
	w.WriteHeader(http.StatusOK)

	// 序列化为JSON
	jsonData, err := json.Marshal(successResponse)
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to marshal success response: %v", err)
		WriteAWSError(w, r, "InternalError", "Failed to generate response", http.StatusInternalServerError)
		return
	}

	w.Write(jsonData)
}
