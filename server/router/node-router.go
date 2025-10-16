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
)

func registerNodeRouter(mr *mux.Router) {
	// 创建子路由，仅当请求头包含 x-amz-dedups3-api 才匹配
	nr := mr.PathPrefix("/dedups3/node").Headers("x-amz-dedups3-node-api", "").Subrouter()
	nr.Methods(http.MethodGet).Path("/{blockID}").HandlerFunc(handler.ReadBlockHandler).Queries("readBlock", "").Name("ReadBlock")
}
