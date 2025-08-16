// /home/steven/work/boulder/server/middleware/request_id.go
package middleware

import (
	"context"
	"github.com/google/uuid"
	"net/http"
)

// RequestIDKey 用于上下文存储
type RequestIDKey struct{}

// RequestIDMiddleware 为所有请求生成唯一ID
func RequestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 生成唯一Request ID
		// 可以使用UUID或时间戳+随机数的组合
		requestID := uuid.New().String()

		// 将Request ID添加到响应头
		w.Header().Set("x-amz-request-id", requestID)

		// 将Request ID添加到请求上下文
		ctx := context.WithValue(r.Context(), RequestIDKey{}, requestID)

		// 继续处理请求
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// GetRequestID 从上下文中获取 Request ID
func GetRequestID(ctx context.Context) string {
	if id, ok := ctx.Value(RequestIDKey{}).(string); ok {
		return id
	}
	return ""
}
