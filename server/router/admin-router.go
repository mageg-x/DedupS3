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
	api_router.Use(middleware.AdminAuthMiddleware)
	// 显式注册 OPTIONS 路由，让请求能“匹配”上
	api_router.Methods(http.MethodOptions).Path("/{rest:.*}").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	api_router.Methods(http.MethodGet).Path("/auth/status").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	api_router.Methods(http.MethodPost).Path("/login").HandlerFunc(handler.AdminLoginHandler)
	api_router.Methods(http.MethodPost).Path("/logout").HandlerFunc(handler.AdminLogoutHandler)
	api_router.Methods(http.MethodGet).Path("/stats").HandlerFunc(handler.AdminGetStatsHandler)
	api_router.Methods(http.MethodGet).Path("/bucket/list").HandlerFunc(handler.AdminListBucketsHandler)
	api_router.Methods(http.MethodPut).Path("/bucket/create").HandlerFunc(handler.AdminCreateBucketHandler)
	api_router.Methods(http.MethodDelete).Path("/bucket/delete").HandlerFunc(handler.AdminDeleteBucketHandler)
	api_router.Methods(http.MethodGet).Path("/bucket/objects").HandlerFunc(handler.AdminListObjectsHandler)
	api_router.Methods(http.MethodPut).Path("/bucket/folder").HandlerFunc(handler.AdminCreateFolderHandler)
	api_router.Methods(http.MethodPost).Path("/bucket/putobject").HandlerFunc(handler.AdminPutObjectHandler)
	api_router.Methods(http.MethodPost).Path("/bucket/deleteobject").HandlerFunc(handler.AdminDelObjectHandler)
	api_router.Methods(http.MethodPost).Path("/bucket/getobject").HandlerFunc(handler.AdminGetObjectHandler)

	api_router.Methods(http.MethodGet).Path("/user/info").HandlerFunc(handler.AdminGetUserHandler)
	api_router.Methods(http.MethodGet).Path("/user/list").HandlerFunc(handler.AdminListUserHandler)
	api_router.Methods(http.MethodPost).Path("/user/create").HandlerFunc(handler.AdminCreateUserHandler)
	api_router.Methods(http.MethodPost).Path("/user/update").HandlerFunc(handler.AdminUpdateUserHandler)
	api_router.Methods(http.MethodDelete).Path("/user/delete").HandlerFunc(handler.AdminDeleteUserHandler)
	api_router.Methods(http.MethodGet).Path("/group/list").HandlerFunc(handler.AdminListGroupHandler)
	api_router.Methods(http.MethodGet).Path("/group/get").HandlerFunc(handler.AdminGetGroupHandler)
	api_router.Methods(http.MethodPost).Path("/group/create").HandlerFunc(handler.AdminCreateGroupHandler)
	api_router.Methods(http.MethodPost).Path("/group/update").HandlerFunc(handler.AdminUpdateGroupHandler)
	api_router.Methods(http.MethodDelete).Path("/group/delete").HandlerFunc(handler.AdminDeleteGroupHandler)
	api_router.Methods(http.MethodGet).Path("/role/list").HandlerFunc(handler.AdminListRoleHandler)
	api_router.Methods(http.MethodGet).Path("/role/get").HandlerFunc(handler.AdminGetRoleHandler)
	api_router.Methods(http.MethodPost).Path("/role/create").HandlerFunc(handler.AdminCreateRoleHandler)
	api_router.Methods(http.MethodPost).Path("/role/update").HandlerFunc(handler.AdminUpdateRoleHandler)
	api_router.Methods(http.MethodDelete).Path("/role/delete").HandlerFunc(handler.AdminDeleteRoleHandler)
	api_router.Methods(http.MethodGet).Path("/policy/list").HandlerFunc(handler.AdminListPolicyHandler)
	api_router.Methods(http.MethodGet).Path("/policy/get").HandlerFunc(handler.AdminGetPolicyHandler)
	api_router.Methods(http.MethodPost).Path("/policy/create").HandlerFunc(handler.AdminCreatePolicyHandler)
	api_router.Methods(http.MethodPost).Path("/policy/update").HandlerFunc(handler.AdminUpdatePolicyHandler)
	api_router.Methods(http.MethodDelete).Path("/policy/delete").HandlerFunc(handler.AdminDeletePolicyHandler)
	api_router.Methods(http.MethodGet).Path("/accesskey/list").HandlerFunc(handler.AdminListAccessKeyHandler)
	api_router.Methods(http.MethodPost).Path("/accesskey/create").HandlerFunc(handler.AdminCreateAccessKeyHandler)
	api_router.Methods(http.MethodPost).Path("/accesskey/update").HandlerFunc(handler.AdminUpdateAccessKeyHandler)
	api_router.Methods(http.MethodDelete).Path("/accesskey/delete").HandlerFunc(handler.AdminDeleteAccessKeyHandler)
	api_router.Methods(http.MethodGet).Path("/config/listquota").HandlerFunc(handler.AdminListQuotaHandler)
	api_router.Methods(http.MethodPost).Path("/config/createquota").HandlerFunc(handler.AdminCreateQuotaHandler)
	api_router.Methods(http.MethodPost).Path("/config/updatequota").HandlerFunc(handler.AdminUpdateQuotaHandler)
	api_router.Methods(http.MethodDelete).Path("/config/deletequota").HandlerFunc(handler.AdminDeleteQuotaHandler)
	api_router.Methods(http.MethodGet).Path("/config/listchunkcfg").HandlerFunc(handler.AdminListChunkConfigHandler)
	api_router.Methods(http.MethodGet).Path("/config/getchunkcfg").HandlerFunc(handler.AdminGetChunkConfigHandler)
	api_router.Methods(http.MethodPost).Path("/config/updatechunkcfg").HandlerFunc(handler.AdminSetChunkConfigHandler)
	api_router.Methods(http.MethodGet).Path("/config/liststorage").HandlerFunc(handler.AdminListStorageHandler)
	api_router.Methods(http.MethodPost).Path("/config/createstorage").HandlerFunc(handler.AdminCreateStorageHandler)
	api_router.Methods(http.MethodPost).Path("/config/teststorage").HandlerFunc(handler.AdminTestStorageHandler)
	api_router.Methods(http.MethodDelete).Path("/config/deletestorage").HandlerFunc(handler.AdminDeleteStorageHandler)
	api_router.Methods(http.MethodGet).Path("/debug/object").HandlerFunc(handler.AdminDebugObjectInfoHandler)
	api_router.Methods(http.MethodGet).Path("/debug/block").HandlerFunc(handler.AdminDebugBlockInfoHandler)
	api_router.Methods(http.MethodGet).Path("/debug/chunk").HandlerFunc(handler.AdminDebugChunkInfoHandler)

	// 处理静态资源路由
	ar.Methods(http.MethodGet).Path("/{path:.*}").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logger.GetLogger("dedups3").Errorf("serving static: %s", r.URL.Path)
		if strings.HasPrefix(r.URL.Path, "/api/") {
			http.NotFound(w, r)
			return
		}
		h := http.StripPrefix("/", http.FileServer(FS))
		h.ServeHTTP(w, r)
	})
	logger.GetLogger("dedups3").Infof("Admin console routes registered with prefix /")
}
