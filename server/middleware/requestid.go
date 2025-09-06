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
	"context"
	"net/http"

	xhttp "github.com/mageg-x/boulder/internal/http"
	"github.com/mageg-x/boulder/internal/utils"
)

// RequestIDMiddleware 为所有请求生成唯一ID
func RequestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		//logger.GetLogger("boulder").Errorf("get req %s %s %d", r.Method, r.URL.Path, r.ContentLength)
		// 生成唯一Request ID
		// 可以使用UUID或时间戳+随机数的组合
		requestID := utils.GenUUID()

		// 将Request ID添加到响应头
		w.Header().Set("x-amz-request-id", requestID)

		// 将Request ID添加到请求上下文
		ctx := context.WithValue(r.Context(), xhttp.AWSRequestID{}, requestID)

		// 继续处理请求
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
