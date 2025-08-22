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
package middleware

import (
	"encoding/json"
	xhttp "github.com/mageg-x/boulder/internal/http"
	"github.com/mageg-x/boulder/internal/logger"
	"net/http"
	"time"

	"github.com/gorilla/mux"
)

func AuditMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// 创建响应写入器以捕获状态码
		rw := &responseWriter{ResponseWriter: w}

		// 处理请求
		next.ServeHTTP(rw, r)

		// 获取请求上下文中的 Request ID
		requestID := xhttp.GetRequestID(r.Context())

		// 获取当前路由信息
		route := mux.CurrentRoute(r)
		var apiName, apiPattern string

		if route != nil {
			// 获取路由名称
			apiName = route.GetName()

			// 获取路由模式（如 "/{bucket}/{key}"）
			if pattern, err := route.GetPathTemplate(); err == nil {
				apiPattern = pattern
			}
		}

		// 获取路径变量（bucket 和 key）
		vars := mux.Vars(r)
		bucket := vars["bucket"]
		key := vars["key"]

		// 记录日志
		txt, _ := json.Marshal(map[string]interface{}{
			"amz_request_id": requestID,
			"api_name":       apiName,    // API 名称（如 "PutObject"）
			"api_pattern":    apiPattern, // API 模式（如 "/{bucket}/{key}"）
			"method":         r.Method,
			"path":           r.URL.Path,
			"status":         rw.status,
			"duration":       time.Since(start).String(),
			"remote_addr":    r.RemoteAddr,
			"user_agent":     r.UserAgent(),
			"bucket":         bucket,
			"key":            key,
		})
		logger.GetLogger("audit").Info(string(txt))
	})
}

// 自定义 ResponseWriter 以捕获状态码
type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(status int) {
	rw.status = status
	rw.ResponseWriter.WriteHeader(status)
}
