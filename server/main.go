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
package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/mageg-x/boulder/internal/storage/block"
	"github.com/mageg-x/boulder/web"
	"go.uber.org/zap"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mageg-x/boulder/internal/config"
	"github.com/mageg-x/boulder/internal/logger"
	"github.com/mageg-x/boulder/internal/storage/kv"
	"github.com/mageg-x/boulder/router"

	xhttp "github.com/mageg-x/boulder/internal/http"
)

type CLI struct {
	ConfigPath  string
	ShowHelp    bool
	ShowVersion bool
	DebugMode   bool
}

func ParseCLI() *CLI {
	cli := &CLI{}

	flag.StringVar(&cli.ConfigPath, "config", "", "Path to configuration file")
	flag.StringVar(&cli.ConfigPath, "c", "", "Short form for config path")
	flag.BoolVar(&cli.ShowHelp, "help", false, "Show help message")
	flag.BoolVar(&cli.ShowHelp, "h", false, "Show help message")
	flag.BoolVar(&cli.ShowVersion, "version", false, "Show version information")
	flag.BoolVar(&cli.ShowVersion, "v", false, "Show version information")
	flag.BoolVar(&cli.DebugMode, "debug", false, "Enable debug mode")

	flag.Parse()

	return cli
}

func main() {
	rand.New(rand.NewSource(time.Now().UnixNano()))

	cli := ParseCLI()
	if cli.ShowHelp {
		flag.Usage()
		os.Exit(0)
	}

	if cli.ShowVersion {
		fmt.Println("Boulder Server v1.0.0")
		os.Exit(0)
	}

	// 配置文件路径从从 命令行参数 或者 环境变量获取
	confPath := cli.ConfigPath
	_ = config.Load(confPath)
	cfg := config.Get()

	logger.Init(&logger.Config{
		LogDir:     cfg.LogDir,
		MaxSize:    cfg.LogSize,
		MaxBackups: cfg.LogMaxBackups,
		MaxAge:     cfg.LogMaxAge,
		Compress:   cfg.LogCompress,
	})

	// kv 存放元数据
	_, err := kv.InitKvStore(&kv.Config{
		Type: kv.StorageBadger,
		Badger: kv.BadgerConfig{
			Path: "./data/kv",
		},
	})

	if err != nil {
		logger.GetLogger("boulder").Error("failed to init kv store", zap.Error(err))
		panic(err)
	}

	// block 存放对象数据块
	_, err = block.InitBlockStore("local-disk", "disk", "./data/block")
	if err != nil {
		logger.GetLogger("boulder").Error("failed to init block store", zap.Error(err))
		panic(err)
	}

	// 1. 创建路由处理器
	mux := router.SetupRouter()

	// 2. 创建服务器实例（监听 3000 端口）
	tcpOpt := xhttp.TCPOptions{
		DriveOPTimeout: func() time.Duration {
			return 5 * time.Second
		},
		IdleTimeout: cfg.IdleTimeout,
		NoDelay:     true,
		RecvBufSize: cfg.RecvBufSize,
		SendBufSize: cfg.SendBufSize,
		Trace: func(msg string) {
			logger.GetLogger("boulder").Infof(msg)
		},
		UserTimeout: int(cfg.ConnUserTimeout.Milliseconds()),
	}
	listenCtx := context.Background()
	listenErrCallback := func(addr string, err error) {
		if err != nil {
			logger.GetLogger("boulder").Fatalf("list %s failed: %v", addr, err)
		}
	}
	// 创建多个服务器实例
	servers := make([]*xhttp.Server, cfg.Listeners)
	serveFuncs := make([]func() error, cfg.Listeners)
	for i := 0; i < cfg.Listeners; i++ {
		servers[i] = xhttp.NewServer([]string{cfg.Address})
		// 配置服务器参数
		servers[i].UseHandler(mux).UseIdleTimeout(cfg.IdleTimeout).UseReadTimeout(cfg.ReadTimeout).UseWriteTimeout(cfg.WriteTimeout)
		servers[i].UseTCPOptions(tcpOpt).UseBaseContext(context.Background())

		// 初始化服务器
		serveFunc, err := servers[i].Init(listenCtx, listenErrCallback)
		if err != nil {
			logger.GetLogger("boulder").Fatalf("init server failed: %v", err)
		}
		serveFuncs[i] = serveFunc
	}
	logger.GetLogger("boulder").Infof("server starting")
	// 启动所有服务器（在不同协程中）
	for i := 0; i < cfg.Listeners; i++ {
		go func(idx int) {
			if err := serveFuncs[idx](); err != nil {
				logger.GetLogger("boulder").Errorf("server %d running failed: %v", idx, err)
			}
		}(i)
	}

	// 启动console服务
	web.Start()

	// 创建一个通道来接收操作系统的中断信号
	quit := make(chan os.Signal, 1)
	// 注册中断信号
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	// 等待中断信号
	<-quit

	// 执行优雅关机
	logger.GetLogger("boulder").Infof("stop servers ...")

	// 关闭console 服务
	web.Close()

	// 关闭主服务器
	for _, srv := range servers {
		if err := srv.Shutdown(); err != nil {
			logger.GetLogger("boulder").Errorf("stop server failed: %v", err)
		}
	}
	logger.GetLogger("boulder").Infof("server ended")
}
