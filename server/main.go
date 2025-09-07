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
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"go.uber.org/zap"

	"github.com/mageg-x/boulder/internal/config"
	xhttp "github.com/mageg-x/boulder/internal/http"
	"github.com/mageg-x/boulder/internal/logger"
	"github.com/mageg-x/boulder/internal/storage/cache"
	"github.com/mageg-x/boulder/internal/storage/kv"
	"github.com/mageg-x/boulder/router"
	"github.com/mageg-x/boulder/service/storage"
	gc2 "github.com/mageg-x/boulder/service/task"
	"github.com/mageg-x/boulder/web"
)

type CLI struct {
	ConfigPath  string
	ShowHelp    bool
	ShowVersion bool
	Verbose     int
}

func ParseCLI() *CLI {
	cli := &CLI{}

	pflag.StringVar(&cli.ConfigPath, "config", "", "Path to configuration file")
	pflag.StringVar(&cli.ConfigPath, "c", "", "Short form for config path")
	pflag.BoolVar(&cli.ShowHelp, "help", false, "Show help message")
	pflag.BoolVar(&cli.ShowHelp, "h", false, "Show help message")
	pflag.BoolVar(&cli.ShowVersion, "version", false, "Show version information")
	pflag.CountVarP(&cli.Verbose, "verbose", "v", "Increase verbosity: -v for INFO, -vv for DEBUG, -vvv for TRACE")
	pflag.Parse()

	return cli
}

func main() {
	rand.New(rand.NewSource(time.Now().UnixNano()))
	// 1、 初始化配置和 日志部分
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
		LogDir:     cfg.Log.Dir,
		MaxSize:    cfg.Log.Size,
		MaxBackups: cfg.Log.MaxBackups,
		MaxAge:     cfg.Log.MaxAge,
		Compress:   cfg.Log.Compress,
	})

	logger.GetLogger("boulder").SetLevel(logrus.Level(int(logrus.WarnLevel) + cli.Verbose))
	logger.GetLogger("boulder").Tracef("get config %v", cfg)

	// 2、初始化存储部分
	// 初始kv， 初始meta数据地方
	_, err := kv.GetKvStore()
	if err != nil {
		logger.GetLogger("boulder").Error("failed to init kv store", zap.Error(err))
		panic(err)
	}

	// 初始化cache， 缓存元数据的地方
	_, err = cache.GetCache()
	if err != nil {
		logger.GetLogger("boulder").Error("failed to init cache store", zap.Error(err))
		panic(err)
	}

	// 初始化 缺省block存储
	bs := storage.GetStorageService()
	store, _ := bs.AddStorage("disk", "STANDARD", config.BlockConfig{
		Disk: config.DiskConfig{
			Path: "./data/blocks",
		},
	})
	stores := bs.ListStorages()

	logger.GetLogger("boulder").Infof("list store %v strores %#v", store, stores)

	// 初始化 垃圾回收后台服务
	gc := gc2.GetGCService()
	if gc == nil {
		logger.GetLogger("boulder").Error("failed to init task service")
		panic(err)
	}
	err = gc.Start()
	if err != nil {
		logger.GetLogger("boulder").Error("failed to start task service", zap.Error(err))
		panic(err)
	}

	// 制造一些测试数据
	//iamService := iam.GetIamService()
	//account, _ := iamService.CreateAccount("stevenrao", "Abcd@1234")
	//ak, err := iamService.CreateAccessKey(account.AccountID, account.Name, time.Now().Add(time.Hour*24*365))
	//logger.GetLogger("boulder").Errorf("create account %v ak %v ", account, ak)

	// 3. 创建路由处理器
	mux := router.SetupRouter()

	// 4. 创建服务器实例（监听 3000 端口）
	tcpOpt := xhttp.TCPOptions{
		DriveOPTimeout: func() time.Duration {
			return 5 * time.Second
		},
		IdleTimeout: cfg.Server.IdleTimeout,
		NoDelay:     true,
		RecvBufSize: cfg.Server.RecvBufSize,
		SendBufSize: cfg.Server.SendBufSize,
		Trace: func(msg string) {
			logger.GetLogger("boulder").Tracef(msg)
		},
		UserTimeout: int(cfg.Server.ConnUserTimeout.Milliseconds()),
	}
	listenCtx := context.Background()
	listenErrCallback := func(addr string, err error) {
		if err != nil {
			logger.GetLogger("boulder").Fatalf("listen %s failed: %v", addr, err)
		}
	}
	// 创建多个服务器实例
	servers := make([]*xhttp.Server, cfg.Server.Listeners)
	serveFuncs := make([]func() error, cfg.Server.Listeners)
	for i := 0; i < cfg.Server.Listeners; i++ {
		servers[i] = xhttp.NewServer([]string{cfg.Server.Address})
		// 配置服务器参数
		servers[i].UseHandler(mux).UseIdleTimeout(cfg.Server.IdleTimeout).UseReadTimeout(cfg.Server.ReadTimeout).UseWriteTimeout(cfg.Server.WriteTimeout)
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
	for i := 0; i < cfg.Server.Listeners; i++ {
		go func(idx int) {
			if err := serveFuncs[idx](); err != nil {
				logger.GetLogger("boulder").Errorf("server %d running failed: %v", idx, err)
			}
		}(i)
	}

	// 5、启动console服务
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
