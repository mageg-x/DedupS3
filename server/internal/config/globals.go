package config

import (
	"os"
	"strings"
)

var (
	GlobalNodeID = ""
	IsDev        = isDevelopment()
)

// IsDev 表示是否是开发环境
func isDevelopment() bool {
	env := strings.ToLower(os.Getenv("ENV"))
	return env == "dev" || env == "development"
}
