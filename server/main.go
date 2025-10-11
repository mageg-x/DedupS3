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
	"errors"
	"flag"
	"fmt"
	stats2 "github.com/mageg-x/dedups3/service/stats"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"

	"github.com/mageg-x/dedups3/internal/config"
	xhttp "github.com/mageg-x/dedups3/internal/http"
	"github.com/mageg-x/dedups3/internal/logger"
	"github.com/mageg-x/dedups3/internal/storage/block"
	"github.com/mageg-x/dedups3/internal/storage/cache"
	"github.com/mageg-x/dedups3/internal/storage/kv"
	"github.com/mageg-x/dedups3/internal/vfs"
	"github.com/mageg-x/dedups3/router"
	gc2 "github.com/mageg-x/dedups3/service/gc"
	"github.com/mageg-x/dedups3/service/iam"
	"github.com/mageg-x/dedups3/service/storage"
)

var (
	once singleflight.Group
)

type CLI struct {
	ConfigPath  string
	ShowHelp    bool
	ShowVersion bool
	Verbose     int
}

func parseCLI() *CLI {
	cli := &CLI{}

	pflag.StringVarP(&cli.ConfigPath, "config", "c", "", "Path to configuration file")
	pflag.BoolVarP(&cli.ShowHelp, "help", "h", false, "Show help message")
	pflag.BoolVarP(&cli.ShowVersion, "version", "V", false, "Show version information")
	pflag.CountVarP(&cli.Verbose, "verbose", "v", "Increase verbosity: -v for INFO, -vv for DEBUG, -vvv for TRACE")
	pflag.Parse()

	return cli
}

func startAdminSvr() error {
	errCh := make(chan error, 1) // 缓冲 1，防止 goroutine 泄漏

	go once.Do("start-admin-server", func() (interface{}, error) {
		cfg := config.Get()
		mr := router.SetupAdminRouter()
		adminServer := &http.Server{
			Addr:    cfg.Server.ConsoleAddress,
			Handler: mr,
		}
		logger.GetLogger("dedups3").Infof("admin server started at %s", adminServer.Addr)
		if err := adminServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.GetLogger("dedups3").Errorf("admin server running failed: %v", err)
		}

		defer func(svr *http.Server) {
			if svr != nil {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := svr.Shutdown(ctx); err != nil {
					logger.GetLogger("dedups3").Errorf("stop admin server %s failed: %v", svr.Addr, err)
				} else {
					logger.GetLogger("dedups3").Infof("admin server %s stopped", svr.Addr)
				}
			}
		}(adminServer)

		close(errCh) // 正常退出
		return nil, nil
	})

	// 等待1秒 errCh, 如果没有就表示成功
	// 等待最多 1 秒，看是否启动失败
	select {
	case <-errCh:
		// 一秒钟进来，必然是失败
		return fmt.Errorf("admin server  started failed")
	case <-time.After(1 * time.Second):
		// 1 秒内没有错误，也没有 close → 说明服务正在正常运行
		logger.GetLogger("dedups3").Infof("admin server started successfully")
		return nil //认为启动成功
	}
}

func startS3Server() ([]*xhttp.Server, error) {
	cfg := config.Get()
	mr := router.SetupS3Router()
	tcpOpt := xhttp.TCPOptions{
		DriveOPTimeout: func() time.Duration {
			return 5 * time.Second
		},
		IdleTimeout: cfg.Server.IdleTimeout,
		NoDelay:     true,
		RecvBufSize: cfg.Server.RecvBufSize,
		SendBufSize: cfg.Server.SendBufSize,
		Trace: func(msg string) {
			logger.GetLogger("dedups3").Tracef(msg)
		},
		UserTimeout: int(cfg.Server.ConnUserTimeout.Milliseconds()),
	}
	listenCtx := context.Background()
	listenErrCallback := func(addr string, err error) {
		if err != nil {
			logger.GetLogger("dedups3").Fatalf("listen %s failed: %v", addr, err)
		}
	}
	// 创建多个服务器实例
	servers := make([]*xhttp.Server, cfg.Server.Listeners)
	serveFuncs := make([]func() error, cfg.Server.Listeners)
	for i := 0; i < cfg.Server.Listeners; i++ {
		servers[i] = xhttp.NewServer([]string{cfg.Server.Address})
		// 配置服务器参数
		servers[i].UseHandler(mr).UseIdleTimeout(cfg.Server.IdleTimeout).UseReadTimeout(cfg.Server.ReadTimeout).UseWriteTimeout(cfg.Server.WriteTimeout)
		servers[i].UseTCPOptions(tcpOpt).UseBaseContext(context.Background())

		// 初始化服务器
		serveFunc, err := servers[i].Init(listenCtx, listenErrCallback)
		if err != nil {
			logger.GetLogger("dedups3").Fatalf("init server failed: %v", err)
		}
		serveFuncs[i] = serveFunc
	}
	logger.GetLogger("dedups3").Infof("server starting")
	// 启动所有服务器（在不同协程中）
	for i := 0; i < cfg.Server.Listeners; i++ {
		go func(idx int) {
			if err := serveFuncs[idx](); err != nil {
				logger.GetLogger("dedups3").Errorf("server %d running failed: %v", idx, err)
			}
		}(i)
	}
	return servers, nil
}

func initStorage() error {
	// 初始kv， 初始meta数据地方
	_, err := kv.GetKvStore()
	if err != nil {
		logger.GetLogger("dedups3").Error("failed to init kv store", zap.Error(err))
		panic(err)
	}

	// 初始化cache， 缓存元数据的地方
	_, err = cache.GetCache()
	if err != nil {
		logger.GetLogger("dedups3").Error("failed to init cache store", zap.Error(err))
		panic(err)
	}

	// 初始化 block存储
	bs := storage.GetStorageService()
	// 先把本地配置，加入到云配置中
	//cfg := config.Get()
	//for _, s := range cfg.Storages {
	//	if s.Disk != nil {
	//		bs.AddStorage("disk", s.Class, config.StorageConfig{Disk: s.Disk})
	//	} else if s.S3 != nil {
	//		bs.AddStorage("s3", s.Class, config.StorageConfig{S3: s.S3})
	//	}
	//}
	// 拉取所有云配置
	storages := bs.ListStorages()
	for i, s := range storages {
		logger.GetLogger("dedups3").Warnf("storage %d %#v", i, s)
		_storage, err := bs.GetStorage(s.ID)
		if err != nil {
			logger.GetLogger("dedups3").Error("failed to init storage", zap.Error(err))
			os.Exit(1)
		}
		// 关键：检查 inst 是否也实现了 vfs.SyncTarget
		syncTargetor, ok := _storage.Instance.(vfs.SyncTargetor) // 类型断言
		if !ok {
			logger.GetLogger("dedups3").Errorf("storage instance for id %#v does not implement SyncTarget", _storage)
			os.Exit(1)
		}
		vfile, err := block.GetTieredFs()
		if err == nil && vfile != nil {
			_ = vfile.AddSyncTargetor(s.ID, syncTargetor)
		} else {
			logger.GetLogger("dedups3").Errorf("failed to get tiered vfs for storage %#v", _storage)
			os.Exit(1)
		}
	}
	return nil
}

func main() {
	rand.New(rand.NewSource(time.Now().UnixNano()))
	// 1、 初始化配置和 日志部分
	cli := parseCLI()
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

	logger.GetLogger("dedups3").SetLevel(logrus.Level(int(logrus.WarnLevel) + cli.Verbose))
	logger.GetLogger("dedups3").Tracef("get config %v", cfg)

	// 2、初始化存储部分
	err := initStorage()
	if err != nil {
		logger.GetLogger("dedups3").Error("failed to init storage", zap.Error(err))
		panic(err)
	}

	// 缺省账户信息
	iamService := iam.GetIamService()
	account, err := iamService.CreateAccount(cfg.Iam.Username, cfg.Iam.Password)
	if err != nil {
		if !errors.Is(err, iam.ERR_ACCOUNT_EXISTS) {
			logger.GetLogger("dedups3").Fatal("failed to create account", zap.Error(err))
		}
	}
	ak, err := iamService.CreateAccessKey(account.AccountID, account.Name, cfg.Iam.AK, cfg.Iam.SK, time.Now().Add(time.Hour*24*365), true)
	logger.GetLogger("dedups3").Warnf("create account %v ak %v ", account, ak)

	iamService.CreateUser(account.AccountID, "admin", "Abcd@1234", nil, nil, nil, true)
	iamService.CreateAccessKey(account.AccountID, "admin", "D"+cfg.Iam.AK, "D"+cfg.Iam.SK, time.Now().Add(time.Hour*24*365), true)
	// 启动 admin server
	if err := startAdminSvr(); err != nil {
		logger.GetLogger("dedups3").Error("failed to start admin server", zap.Error(err))
		panic(err)
	}

	// 启动S3 Server（监听 3000 端口）
	servers, err := startS3Server()
	if err != nil {
		logger.GetLogger("dedups3").Error("failed to start s3 server", zap.Error(err))
		panic(err)
	}

	// 初始化 垃圾回收后台服务
	gc := gc2.GetGCService()
	if gc == nil {
		logger.GetLogger("dedups3").Error("failed to init gc service")
		panic(err)
	}
	if err = gc.Start(); err != nil {
		logger.GetLogger("dedups3").Error("failed to start gc service", zap.Error(err))
		panic(err)
	}

	// 初始化数据统计后台服务
	stats := stats2.GetStatsService()
	if stats == nil {
		logger.GetLogger("dedups3").Error("failed to init stats service")
		panic(err)
	}
	if err = stats.Start(); err != nil {
		logger.GetLogger("dedups3").Error("failed to start stats service", zap.Error(err))
		panic(err)
	}

	// 创建一个通道来接收操作系统的中断信号
	quit := make(chan os.Signal, 1)
	// 注册中断信号
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	// 等待中断信号
	<-quit

	// 执行优雅关机
	logger.GetLogger("dedups3").Infof("stop servers ...")

	// 关闭主服务器
	for _, srv := range servers {
		if err := srv.Shutdown(); err != nil {
			logger.GetLogger("dedups3").Errorf("stop server failed: %v", err)
		}
	}

	logger.GetLogger("dedups3").Infof("server ended")
}
