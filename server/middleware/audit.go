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

	xhttp "github.com/mageg-x/dedups3/internal/http"
	"github.com/mageg-x/dedups3/internal/logger"
	"github.com/mageg-x/dedups3/plugs/audit"
	AS "github.com/mageg-x/dedups3/service/audit"
)

func AuditMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logger.GetLogger("dedups3").Tracef("get req %s %s %#v", r.Method, r.URL.Path, r.Header)

		// Wrap the original ResponseWriter
		rw := &xhttp.RespWriter{ResponseWriter: w}
		// 处理请求
		next.ServeHTTP(rw, r)

		filterKeys := map[string]struct{}{
			"password":  {},
			"secretKey": {},
		}

		// 记录审计日志
		as := AS.GetAuditService()
		audit.AuditLog(r, rw, filterKeys, as)
	})
}

func TraceContext(r *http.Request) (*http.Request, *xhttp.TraceCtxt) {
	ctx := r.Context()

	// Try to get existing
	if val := ctx.Value(xhttp.ContextTraceKey); val != nil {
		if tc, ok := val.(*xhttp.TraceCtxt); ok {
			logger.GetLogger("dedups3").Tracef("get exist ctx %#v and tc %#v", ctx, tc)
			return r, tc
		}
	}

	// Create new
	tc := &xhttp.TraceCtxt{Attributes: make(map[string]interface{})}
	newCtx := context.WithValue(ctx, xhttp.ContextTraceKey, tc)
	logger.GetLogger("dedups3").Tracef("get new trace context: %#v", newCtx)
	return r.WithContext(newCtx), tc
}
