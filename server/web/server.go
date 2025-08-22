package web

import (
	"context"
	"errors"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mageg-x/boulder/internal/config"
	"github.com/mageg-x/boulder/internal/logger"
)

var (
	consoleServer *http.Server
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

func Start() {
	go func() {
		mux := http.NewServeMux()
		cfg := config.Get()

		// 获取嵌入式文件系统
		dist, err := fs.Sub(WebDistFS, "dist")
		if err != nil {
			log.Fatalf("Failed to get sub filesystem: %v", err)
		}

		// 创建自定义文件系统处理器
		FS := &webFS{
			root: http.FS(dist),
		}

		// SPA处理器函数
		handler := func(w http.ResponseWriter, r *http.Request) {
			// 可选：API路由保护
			if strings.HasPrefix(r.URL.Path, "/api/") {
				http.NotFound(w, r)
				return
			}

			// 使用自定义文件系统
			http.FileServer(FS).ServeHTTP(w, r)
		}

		mux.Handle("/", http.StripPrefix("/", http.HandlerFunc(handler)))

		consoleServer = &http.Server{
			Addr:    cfg.Server.ConsoleAddress,
			Handler: mux,
		}

		logger.GetLogger("boulder").Infof("console server started at %s", consoleServer.Addr)
		if err := consoleServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.GetLogger("boulder").Errorf("console server running failed: %v", err)
		}
	}()
}

func Close() {
	if consoleServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := consoleServer.Shutdown(ctx); err != nil {
			logger.GetLogger("boulder").Errorf("stop console server failed: %v", err)
		} else {
			logger.GetLogger("boulder").Infof("console server stopped")
		}
	}
}
