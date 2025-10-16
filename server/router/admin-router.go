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
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/gorilla/mux"

	"github.com/mageg-x/dedups3/handler"
	"github.com/mageg-x/dedups3/internal/logger"
	"github.com/mageg-x/dedups3/middleware"
	"github.com/mageg-x/dedups3/web"
)

// 自定义文件处理器 (解决目录浏览和SPA路由问题)
type webFS struct {
	root http.FileSystem
}

func (fs *webFS) Open(name string) (http.File, error) {
	f, err := fs.root.Open(name)
	if err != nil {
		// 文件不存在时返回index.html
		return fs.root.Open("index.html")
	}

	// 禁用目录浏览
	if stat, err := f.Stat(); err == nil && stat.IsDir() {
		// 尝试读取目录下的index.html
		indexPath := filepath.Join(name, "index.html")
		if _, err := fs.root.Open(indexPath); err != nil {
			_ = f.Close() // 关闭目录句柄
			return nil, os.ErrNotExist
		}
	}
	return f, nil
}

// registerAdminRouter 注册管理控制台路由
func registerAdminRouter(mr *mux.Router) {
	// 获取嵌入式文件系统
	dist, err := fs.Sub(web.WebDistFS, "dist")
	if err != nil {
		logger.GetLogger("dedups3").Errorf("Failed to get sub filesystem: %v", err)
		return
	}

	// 创建自定义文件系统处理器
	FS := &webFS{
		root: http.FS(dist),
	}

	// 创建admin路由子路由器
	ar := mr.PathPrefix("/").Subrouter()

	api_router := ar.PathPrefix("/api").Subrouter()

	api_router.Use(middleware.RequestIDMiddleware)
	api_router.Use(middleware.AdminAuthMiddleware)
	api_router.Use(middleware.TraceMiddleware)

	// 显式注册 OPTIONS 路由，让请求能“匹配”上
	api_router.Methods(http.MethodOptions).Path("/{rest:.*}").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	api_router.Methods(http.MethodGet).Path("/auth/status").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	api_router.Methods(http.MethodPost).Path("/login").HandlerFunc(handler.AdminLoginHandler).Name("console:Login")
	api_router.Methods(http.MethodPost).Path("/logout").HandlerFunc(handler.AdminLogoutHandler).Name("console:Logout")
	api_router.Methods(http.MethodGet).Path("/stats").HandlerFunc(handler.AdminGetStatsHandler).Name("console:GetStats")
	api_router.Methods(http.MethodGet).Path("/bucket/list").HandlerFunc(handler.AdminListBucketsHandler).Name("console:ListBuckets")
	api_router.Methods(http.MethodPut).Path("/bucket/create").HandlerFunc(handler.AdminCreateBucketHandler).Name("console:CreateBucket")
	api_router.Methods(http.MethodDelete).Path("/bucket/delete").HandlerFunc(handler.AdminDeleteBucketHandler).Name("console:DeleteBucket")
	api_router.Methods(http.MethodGet).Path("/bucket/objects").HandlerFunc(handler.AdminListObjectsHandler).Name("console:ListObjects")
	api_router.Methods(http.MethodPut).Path("/bucket/folder").HandlerFunc(handler.AdminCreateFolderHandler).Name("console:CreateFolder")
	api_router.Methods(http.MethodPost).Path("/bucket/putobject").HandlerFunc(handler.AdminPutObjectHandler).Name("console:PutObject")
	api_router.Methods(http.MethodPost).Path("/bucket/deleteobject").HandlerFunc(handler.AdminDelObjectHandler).Name("console:DeleteObject")
	api_router.Methods(http.MethodPost).Path("/bucket/getobject").HandlerFunc(handler.AdminGetObjectHandler).Name("console:GetObject")

	api_router.Methods(http.MethodGet).Path("/user/info").HandlerFunc(handler.AdminGetUserHandler).Name("console:GetUserInfo")
	api_router.Methods(http.MethodGet).Path("/user/list").HandlerFunc(handler.AdminListUserHandler).Name("console:ListUsers")
	api_router.Methods(http.MethodPost).Path("/user/create").HandlerFunc(handler.AdminCreateUserHandler).Name("console:CreateUser")
	api_router.Methods(http.MethodPost).Path("/user/update").HandlerFunc(handler.AdminUpdateUserHandler).Name("console:UpdateUser")
	api_router.Methods(http.MethodDelete).Path("/user/delete").HandlerFunc(handler.AdminDeleteUserHandler).Name("console:DeleteUser")
	api_router.Methods(http.MethodGet).Path("/group/list").HandlerFunc(handler.AdminListGroupHandler).Name("console:ListGroups")
	api_router.Methods(http.MethodGet).Path("/group/get").HandlerFunc(handler.AdminGetGroupHandler).Name("console:GetGroup")
	api_router.Methods(http.MethodPost).Path("/group/create").HandlerFunc(handler.AdminCreateGroupHandler).Name("console:CreateGroup")
	api_router.Methods(http.MethodPost).Path("/group/update").HandlerFunc(handler.AdminUpdateGroupHandler).Name("console:UpdateGroup")
	api_router.Methods(http.MethodDelete).Path("/group/delete").HandlerFunc(handler.AdminDeleteGroupHandler).Name("console:DeleteGroup")
	api_router.Methods(http.MethodGet).Path("/role/list").HandlerFunc(handler.AdminListRoleHandler).Name("console:ListRoles")
	api_router.Methods(http.MethodGet).Path("/role/get").HandlerFunc(handler.AdminGetRoleHandler).Name("console:GetRole")
	api_router.Methods(http.MethodPost).Path("/role/create").HandlerFunc(handler.AdminCreateRoleHandler).Name("console:CreateRole")
	api_router.Methods(http.MethodPost).Path("/role/update").HandlerFunc(handler.AdminUpdateRoleHandler).Name("console:UpdateRole")
	api_router.Methods(http.MethodDelete).Path("/role/delete").HandlerFunc(handler.AdminDeleteRoleHandler).Name("console:DeleteRole")
	api_router.Methods(http.MethodGet).Path("/policy/list").HandlerFunc(handler.AdminListPolicyHandler).Name("console:ListPolicies")
	api_router.Methods(http.MethodGet).Path("/policy/get").HandlerFunc(handler.AdminGetPolicyHandler).Name("console:GetPolicy")
	api_router.Methods(http.MethodPost).Path("/policy/create").HandlerFunc(handler.AdminCreatePolicyHandler).Name("console:CreatePolicy")
	api_router.Methods(http.MethodPost).Path("/policy/update").HandlerFunc(handler.AdminUpdatePolicyHandler).Name("console:UpdatePolicy")
	api_router.Methods(http.MethodDelete).Path("/policy/delete").HandlerFunc(handler.AdminDeletePolicyHandler).Name("console:DeletePolicy")
	api_router.Methods(http.MethodGet).Path("/accesskey/list").HandlerFunc(handler.AdminListAccessKeyHandler).Name("console:ListAccessKeys")
	api_router.Methods(http.MethodPost).Path("/accesskey/create").HandlerFunc(handler.AdminCreateAccessKeyHandler).Name("console:CreateAccessKey")
	api_router.Methods(http.MethodPost).Path("/accesskey/update").HandlerFunc(handler.AdminUpdateAccessKeyHandler).Name("console:UpdateAccessKey")
	api_router.Methods(http.MethodDelete).Path("/accesskey/delete").HandlerFunc(handler.AdminDeleteAccessKeyHandler).Name("console:DeleteAccessKey")

	api_router.Methods(http.MethodGet).Path("/config/listquota").HandlerFunc(handler.AdminListQuotaHandler).Name("console:ListQuotas")
	api_router.Methods(http.MethodPost).Path("/config/createquota").HandlerFunc(handler.AdminCreateQuotaHandler).Name("console:CreateQuota")
	api_router.Methods(http.MethodPost).Path("/config/updatequota").HandlerFunc(handler.AdminUpdateQuotaHandler).Name("console:UpdateQuota")
	api_router.Methods(http.MethodDelete).Path("/config/deletequota").HandlerFunc(handler.AdminDeleteQuotaHandler).Name("console:DeleteQuota")
	api_router.Methods(http.MethodGet).Path("/config/listchunkcfg").HandlerFunc(handler.AdminListChunkConfigHandler).Name("console:ListChunkConfigs")
	api_router.Methods(http.MethodGet).Path("/config/getchunkcfg").HandlerFunc(handler.AdminGetChunkConfigHandler).Name("console:GetChunkConfig")
	api_router.Methods(http.MethodPost).Path("/config/updatechunkcfg").HandlerFunc(handler.AdminSetChunkConfigHandler).Name("console:UpdateChunkConfig")
	api_router.Methods(http.MethodGet).Path("/config/liststorage").HandlerFunc(handler.AdminListStorageHandler).Name("console:ListStorages")
	api_router.Methods(http.MethodPost).Path("/config/createstorage").HandlerFunc(handler.AdminCreateStorageHandler).Name("console:CreateStorage")
	api_router.Methods(http.MethodPost).Path("/config/teststorage").HandlerFunc(handler.AdminTestStorageHandler).Name("console:TestStorage")
	api_router.Methods(http.MethodDelete).Path("/config/deletestorage").HandlerFunc(handler.AdminDeleteStorageHandler).Name("console:DeleteStorage")
	api_router.Methods(http.MethodGet).Path("/debug/object").HandlerFunc(handler.AdminDebugObjectInfoHandler).Name("console:DebugObjectInfo")
	api_router.Methods(http.MethodGet).Path("/debug/block").HandlerFunc(handler.AdminDebugBlockInfoHandler).Name("console:DebugBlockInfo")
	api_router.Methods(http.MethodGet).Path("/debug/chunk").HandlerFunc(handler.AdminDebugChunkInfoHandler).Name("console:DebugChunkInfo")
	api_router.Methods(http.MethodGet).Path("/audit/list").HandlerFunc(handler.AdminListAuditLogHandler).Name("console:ListAuditLog")
	api_router.Methods(http.MethodGet).Path("/event/list").HandlerFunc(handler.AdminListEventLogHandler).Name("console:ListEventLog")

	// 处理静态资源路由
	ar.Methods(http.MethodGet).Path("/{path:.*}").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logger.GetLogger("dedups3").Debugf("serving static: %s", r.URL.Path)
		if strings.HasPrefix(r.URL.Path, "/api/") {
			http.NotFound(w, r)
			return
		}
		h := http.StripPrefix("/", http.FileServer(FS))
		h.ServeHTTP(w, r)
	})
	logger.GetLogger("dedups3").Infof("Admin console routes registered with prefix /")
}
