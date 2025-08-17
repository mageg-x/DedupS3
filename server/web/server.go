package web

import (
	"context"
	"io/fs"
	"log"
	"net/http"
	"time"

	"github.com/mageg-x/boulder/internal/config"
	"github.com/mageg-x/boulder/internal/logger"
)

var (
	consoleServer *http.Server
)

func Start() {
	// 启动控制台服务，提供静态文件服务
	go func() {
		mux := http.NewServeMux()
		// 使用嵌入式文件系统提供静态文件服务
		dist, err := fs.Sub(WebDistFS, "dist")
		if err != nil {
			log.Fatalf("Failed to get sub filesystem: %v", err)
		}
		httpFS := http.FS(dist)
		mux.Handle("/", http.StripPrefix("/", http.FileServer(httpFS)))

		cfg := config.Get()

		consoleServer = &http.Server{
			Addr:    cfg.ConsoleAddress,
			Handler: mux,
		}

		logger.GetLogger("boulder").Infof("console server started at %s", cfg.ConsoleAddress)
		if err := consoleServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.GetLogger("boulder").Errorf("console server running failed: %v", err)
		}
	}()
}

func Close() {
	// 关闭控制台服务器
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
