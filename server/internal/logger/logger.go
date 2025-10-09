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
package logger

import (
	"fmt"
	"github.com/aws/smithy-go/logging"
	"github.com/mageg-x/dedups3/internal/color"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	loggers = sync.Map{} // 替换 map[string]*logrus.Logger
	mu      sync.Mutex
	config  = Config{
		LogDir:     "./logs",
		MaxSize:    10,
		MaxBackups: 7,
		MaxAge:     30,
		Compress:   true,
	}
)

type Config struct {
	LogDir     string // 日志根目录
	MaxSize    int    // 每个日志文件最大 10MB
	MaxBackups int    // 保留 7 个备份
	MaxAge     int    // 30 天过期
	Compress   bool   // 压缩旧日志
}

// 首先，定义一个空记录器类型
// 定义符合 AWS SDK 接口的空日志记录器
type AWSNullLogger struct{}

// 实现 AWS SDK 所需的 Logf 方法
func (AWSNullLogger) Logf(classification logging.Classification, format string, v ...interface{}) {
	// 什么都不做，完全忽略所有日志
}

type CustomLogFormatter struct{}

func (f *CustomLogFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	var levelColor string
	switch entry.Level {
	case logrus.DebugLevel:
		levelColor = color.BlueBold("[DEBU]")
	case logrus.InfoLevel:
		levelColor = color.GreenBold("[INFO]")
	case logrus.WarnLevel:
		levelColor = color.YellowBold("[WARN]")
	case logrus.ErrorLevel, logrus.FatalLevel, logrus.PanicLevel:
		levelColor = color.RedBold("[ERRO]")
	case logrus.TraceLevel:
		levelColor = color.CyanBold("[TRAC]")
	default:
		levelColor = fmt.Sprintf("[%s]", entry.Level.String())
	}

	// 构建最终日志行：[LEVEL] file:func:line msg
	// 注意：entry.Message 已经包含了 caller 和内容（由 Debugf/Infof 等构造）
	ts := time.Now().Format("2006-01-02 15:04:05")
	msg := fmt.Sprintf("%s %s %s\n", levelColor, ts, entry.Message)
	return []byte(msg), nil
}

// getLoggerKey 标准化 logger 名称（避免路径问题）
func getLoggerKey(name string) string {
	name = strings.TrimSuffix(name, ".log")
	name = filepath.Base(name)
	return fmt.Sprintf("%s.log", name)
}

func Init(cfg *Config) {
	mu.Lock()
	defer mu.Unlock()
	if cfg != nil {
		config = *cfg
	}
}

// Logger === 封装的 Logger 结构体 ===
type Logger struct {
	*logrus.Logger
}

// getCaller 获取调用 Infof/Debugf 等函数的源码位置
// skip = 2: 跳过 getCaller 和 外层日志函数（如 Infof）
func (l *Logger) getCaller() string {
	pc, file, line, ok := runtime.Caller(2)
	if !ok {
		return "???:0"
	}
	_, filename := filepath.Split(file)
	fn := runtime.FuncForPC(pc)
	funcName := fn.Name()
	parts := strings.Split(funcName, ".")
	return fmt.Sprintf("%s:%s:%d", filename, parts[len(parts)-1], line)
}

// Debug === Debug 级别 ===
func (l *Logger) Debug(args ...any) {
	if !l.Logger.IsLevelEnabled(logrus.DebugLevel) {
		return
	}
	caller := l.getCaller()
	l.Logger.Debug(fmt.Sprintf("%s %v", caller, fmt.Sprint(args...)))
}

func (l *Logger) Debugf(format string, args ...any) {
	if !l.Logger.IsLevelEnabled(logrus.DebugLevel) {
		return
	}
	caller := l.getCaller()
	l.Logger.Debugf("%s "+format, append([]any{caller}, args...)...)
}

// Trace === Trace 级别 ===
func (l *Logger) Trace(args ...any) {
	if !l.Logger.IsLevelEnabled(logrus.TraceLevel) {
		return
	}
	caller := l.getCaller()
	l.Logger.Trace(fmt.Sprintf("%s %v", caller, fmt.Sprint(args...)))
}

func (l *Logger) Tracef(format string, args ...any) {
	if !l.Logger.IsLevelEnabled(logrus.TraceLevel) {
		return
	}
	caller := l.getCaller()
	l.Logger.Tracef("%s "+format, append([]any{caller}, args...)...)
}

// Info === Info 级别 ===
func (l *Logger) Info(args ...any) {
	if !l.Logger.IsLevelEnabled(logrus.InfoLevel) {
		return
	}
	caller := l.getCaller()
	l.Logger.Info(fmt.Sprintf("%s %v", caller, fmt.Sprint(args...)))
}

func (l *Logger) Infof(format string, args ...any) {
	if !l.Logger.IsLevelEnabled(logrus.InfoLevel) {
		return
	}
	caller := l.getCaller()
	l.Logger.Infof("%s "+format, append([]any{caller}, args...)...)
}

// Warn === Warn 级别 ===
func (l *Logger) Warn(args ...any) {
	if !l.Logger.IsLevelEnabled(logrus.WarnLevel) {
		return
	}
	caller := l.getCaller()
	l.Logger.Warn(fmt.Sprintf("%s %v", caller, fmt.Sprint(args...)))
}

func (l *Logger) Warnf(format string, args ...any) {
	if !l.Logger.IsLevelEnabled(logrus.WarnLevel) {
		return
	}
	caller := l.getCaller()
	l.Logger.Warnf("%s "+format, append([]any{caller}, args...)...)
}

// === Error 级别 ===
func (l *Logger) Error(args ...any) {
	if !l.Logger.IsLevelEnabled(logrus.ErrorLevel) {
		return
	}
	caller := l.getCaller()
	l.Logger.Error(fmt.Sprintf("%s %v", caller, fmt.Sprint(args...)))
}

func (l *Logger) Errorf(format string, args ...any) {
	if !l.Logger.IsLevelEnabled(logrus.ErrorLevel) {
		return
	}
	caller := l.getCaller()
	l.Logger.Errorf("%s "+format, append([]any{caller}, args...)...)
}

// Fatal === Fatal 级别（会 os.Exit(1)）===
func (l *Logger) Fatal(args ...any) {
	if !l.Logger.IsLevelEnabled(logrus.FatalLevel) {
		return
	}

	caller := l.getCaller()
	l.Logger.Fatal(fmt.Sprintf("%s %v", caller, fmt.Sprint(args...)))
}

func (l *Logger) Fatalf(format string, args ...any) {
	if !l.Logger.IsLevelEnabled(logrus.FatalLevel) {
		return
	}

	caller := l.getCaller()
	l.Logger.Fatalf("%s "+format, append([]any{caller}, args...)...)
}

// Panic === Panic 级别（会 panic）===
func (l *Logger) Panic(args ...any) {
	if !l.Logger.IsLevelEnabled(logrus.PanicLevel) {
		return
	}
	caller := l.getCaller()
	l.Logger.Panic(fmt.Sprintf("%s %v", caller, fmt.Sprint(args...)))
}

func (l *Logger) Panicf(format string, args ...any) {
	if !l.Logger.IsLevelEnabled(logrus.PanicLevel) {
		return
	}
	caller := l.getCaller()
	l.Logger.Panicf("%s "+format, append([]any{caller}, args...)...)
}

// GetLogger === GetLogger：返回封装后的 Logger ===
func GetLogger(name string) *Logger {
	if name == "" {
		return &Logger{logrus.StandardLogger()}
	}

	key := getLoggerKey(name)
	// 先尝试读取（无锁）
	if logger, ok := loggers.Load(key); ok {
		return &Logger{logger.(*logrus.Logger)}
	}

	mu.Lock()
	defer mu.Unlock()
	// 再次检查，防止重复创建（双重检查）
	if logger, ok := loggers.Load(key); ok {
		return &Logger{logger.(*logrus.Logger)}
	}
	// 创建日志目录
	if err := os.MkdirAll(config.LogDir, 0755); err != nil {
		fmt.Printf("无法创建日志目录: %v\n", err)
		return &Logger{logrus.StandardLogger()} // fallback
	}

	// 日志文件路径
	logPath := filepath.Join(config.LogDir, key)

	// 创建新的 logrus.Logger
	logger := logrus.New()
	// 创建一个multi-writer，同时写入到文件和控制台
	fileWriter := &lumberjack.Logger{
		Filename:   logPath,
		MaxSize:    config.MaxSize,
		MaxBackups: config.MaxBackups,
		MaxAge:     config.MaxAge,
		Compress:   config.Compress,
	}
	logger.SetOutput(io.MultiWriter(os.Stdout, fileWriter))

	logger.SetFormatter(&CustomLogFormatter{})

	// 缓存原始 *logrus.Logger
	loggers.Store(key, logger)

	// 返回封装的 Logger
	return &Logger{logger}
}
