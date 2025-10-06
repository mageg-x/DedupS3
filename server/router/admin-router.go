package router

import (
	"github.com/gorilla/mux"
	"github.com/mageg-x/boulder/handler"
	"github.com/mageg-x/boulder/internal/logger"
	"github.com/mageg-x/boulder/middleware"
	"github.com/mageg-x/boulder/web"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
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
		logger.GetLogger("boulder").Errorf("Failed to get sub filesystem: %v", err)
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
	api_router.Methods(http.MethodGet).Path("/listbuckets").HandlerFunc(handler.AdminListBucketsHandler)
	api_router.Methods(http.MethodPut).Path("/createbucket").HandlerFunc(handler.AdminCreateBucketHandler)
	api_router.Methods(http.MethodDelete).Path("/deletebucket").HandlerFunc(handler.AdminDeleteBucketHandler)

	// 处理静态资源路由
	ar.Methods(http.MethodGet).Path("/{path:.*}").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logger.GetLogger("boulder").Errorf("serving static: %s", r.URL.Path)
		if strings.HasPrefix(r.URL.Path, "/api/") {
			http.NotFound(w, r)
			return
		}
		h := http.StripPrefix("/", http.FileServer(FS))
		h.ServeHTTP(w, r)
	})
	logger.GetLogger("boulder").Infof("Admin console routes registered with prefix /")
}
