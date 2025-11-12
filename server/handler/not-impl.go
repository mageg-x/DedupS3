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
package handler

import (
	"net/http"

	"github.com/mageg-x/dedups3/internal/logger"
)

type rejectedAPI struct {
	Api     string
	Methods []string
	Queries []string
	Path    string
}

var RejectedObjAPIs = []rejectedAPI{
	{
		Api:     "torrent",
		Methods: []string{http.MethodPut, http.MethodDelete, http.MethodGet},
		Queries: []string{"torrent", ""},
		Path:    "/{object:.+}",
	},
	{
		Api:     "acl",
		Methods: []string{http.MethodDelete},
		Queries: []string{"acl", ""},
		Path:    "/{object:.+}",
	},
}

var RejectedBucketAPIs = []rejectedAPI{
	{
		Api:     "inventory",
		Methods: []string{http.MethodGet, http.MethodPut, http.MethodDelete},
		Queries: []string{"inventory", ""},
	},
	{
		Api:     "cors",
		Methods: []string{http.MethodPut, http.MethodDelete},
		Queries: []string{"cors", ""},
	},
	{
		Api:     "metrics",
		Methods: []string{http.MethodGet, http.MethodPut, http.MethodDelete},
		Queries: []string{"metrics", ""},
	},
	{
		Api:     "website",
		Methods: []string{http.MethodPut},
		Queries: []string{"website", ""},
	},
	{
		Api:     "logging",
		Methods: []string{http.MethodPut, http.MethodDelete},
		Queries: []string{"logging", ""},
	},
	{
		Api:     "accelerate",
		Methods: []string{http.MethodPut, http.MethodDelete},
		Queries: []string{"accelerate", ""},
	},
	{
		Api:     "requestPayment",
		Methods: []string{http.MethodPut, http.MethodDelete},
		Queries: []string{"requestPayment", ""},
	},
	{
		Api:     "acl",
		Methods: []string{http.MethodDelete, http.MethodPut, http.MethodHead},
		Queries: []string{"acl", ""},
	},
	{
		Api:     "publicAccessBlock",
		Methods: []string{http.MethodDelete, http.MethodPut, http.MethodGet},
		Queries: []string{"publicAccessBlock", ""},
	},
	{
		Api:     "ownershipControls",
		Methods: []string{http.MethodDelete, http.MethodPut, http.MethodGet},
		Queries: []string{"ownershipControls", ""},
	},
	{
		Api:     "intelligent-tiering",
		Methods: []string{http.MethodDelete, http.MethodPut, http.MethodGet},
		Queries: []string{"intelligent-tiering", ""},
	},
	{
		Api:     "analytics",
		Methods: []string{http.MethodDelete, http.MethodPut, http.MethodGet},
		Queries: []string{"analytics", ""},
	},
}

func ErrorResponseHandler(w http.ResponseWriter, r *http.Request) {
	// 打印接口名称
	logger.GetLogger("dedups3").Infof("API called: ErrorResponseHandler")
	// TODO: 实现错误响应逻辑
	http.Error(w, "Method Not Allowed or Operation Not Supported", http.StatusMethodNotAllowed)
}

func NotImplementedHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("dedups3").Infof("API called: NotImplementedHandler, %v", r)
	http.Error(w, "API not implemented", http.StatusNotImplemented)
}

func NotFoundHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("dedups3").Errorf("API called: NotFoundHandler, %v", r)
	http.Error(w, "The specified path does not exist", http.StatusNotFound)
}

func NotAllowedHandler(w http.ResponseWriter, r *http.Request) {
	logger.GetLogger("dedups3").Errorf("API called: NotAllowedHandler, %v", r)
	http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
}
