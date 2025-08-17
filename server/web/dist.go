package web

import "embed"

//go:embed all:dist
var WebDistFS embed.FS
