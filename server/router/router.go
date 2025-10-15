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
package router

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/mageg-x/dedups3/handler"
	"github.com/mageg-x/dedups3/middleware"
)

func SetupAdminRouter() *mux.Router {
	// SkipClean  false  替换为 //  替换 .. .
	mr := mux.NewRouter().SkipClean(false).UseEncodedPath()
	// 添加请求ID中间件（应放在首位）
	mr.Use(mux.CORSMethodMiddleware(mr))
	mr.Use(middleware.RequestIDMiddleware)
	mr.Use(middleware.RateLimitMiddleware(middleware.RateLimitConfig{
		RequestsPerSecond: 10,
		BurstSize:         20,
		KeyFunc:           middleware.ByIP,
	}))

	registerAdminRouter(mr)

	// 使用http.HandlerFunc适配器将函数转换为http.Handler接口
	// 将NotFoundHandler和MethodNotAllowedHandler设置在主路由上，以捕获所有未匹配请求
	mr.NotFoundHandler = http.HandlerFunc(handler.NotFoundHandler)
	mr.MethodNotAllowedHandler = http.HandlerFunc(handler.NotAllowedHandler)

	return mr
}

func SetupS3Router() *mux.Router {
	// SkipClean  false  替换为 //  替换 .. .
	mr := mux.NewRouter().SkipClean(false).UseEncodedPath()

	// 添加请求ID中间件（应放在首位）
	mr.Use(mux.CORSMethodMiddleware(mr))
	mr.Use(middleware.RequestIDMiddleware)
	mr.Use(middleware.TraceMiddleware)
	mr.Use(middleware.RateLimitMiddleware(middleware.RateLimitConfig{
		RequestsPerSecond: 10,
		BurstSize:         20,
		KeyFunc:           middleware.ByIP,
	}))

	registerNodeRouter(mr)

	registerAPIRouter(mr)

	// 使用http.HandlerFunc适配器将函数转换为http.Handler接口
	// 将NotFoundHandler和MethodNotAllowedHandler设置在主路由上，以捕获所有未匹配请求
	mr.NotFoundHandler = http.HandlerFunc(handler.NotFoundHandler)
	mr.MethodNotAllowedHandler = http.HandlerFunc(handler.NotAllowedHandler)

	return mr
}
