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
	"net"
	"net/http"
	"sync"

	"github.com/gorilla/mux"
	xhttp "github.com/mageg-x/dedups3/internal/http"
	"github.com/mageg-x/dedups3/internal/logger"
	"golang.org/x/time/rate"
)

// RateLimiter 限流器接口
type RateLimiter interface {
	Allow() bool
}

// RateLimitConfig 限流配置
type RateLimitConfig struct {
	RequestsPerSecond float64                    // 每秒允许的请求数
	BurstSize         int                        // 突发请求量（桶大小）
	KeyFunc           func(*http.Request) string // 限流键生成函数
}

// RateLimitMiddleware 限流中间件
func RateLimitMiddleware(config RateLimitConfig) mux.MiddlewareFunc {
	return func(next http.Handler) http.Handler {
		// 使用同步Map存储限流器
		var limiters sync.Map
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			logger.GetLogger("dedups3").Tracef("get req %s %s %#v", r.Method, r.URL.Path, r.Header)
			// 获取限流键
			key := "global"
			if config.KeyFunc != nil {
				key = config.KeyFunc(r)
			}

			// 获取或创建限流器
			limiter, _ := limiters.LoadOrStore(key, rate.NewLimiter(
				rate.Limit(config.RequestsPerSecond),
				config.BurstSize,
			))

			// 检查是否允许请求
			if !limiter.(*rate.Limiter).Allow() {
				logger.GetLogger("dedups3").Warnf("too many requests from %s", key)
				xhttp.WriteAWSError(w, r, "TooManyRequests", "Request too frequent, please try again later.", http.StatusTooManyRequests)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// ByIP 辅助函数：按客户端IP限流
func ByIP(r *http.Request) string {
	// 获取真实IP（考虑代理情况）
	ip := r.Header.Get("X-Forwarded-For")
	if ip == "" {
		ip = r.Header.Get("X-Real-IP")
	}
	if ip == "" {
		ip, _, _ = net.SplitHostPort(r.RemoteAddr)
	}
	return ip
}

// ByPath 辅助函数：按API路径限流
func ByPath(r *http.Request) string {
	return r.URL.Path
}

// ByUser 辅助函数：按用户限流
func ByUser(r *http.Request) string {
	// 实际应用中从认证信息中获取用户ID
	if userID := r.Header.Get("X-User-ID"); userID != "" {
		return userID
	}
	return "anonymous"
}
