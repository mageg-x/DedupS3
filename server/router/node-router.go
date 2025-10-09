package router

import (
	"net/http"

	"github.com/gorilla/mux"

	"github.com/mageg-x/dedups3/handler"
)

func registerNodeRouter(mr *mux.Router) {
	// 创建子路由，仅当请求头包含 x-amz-dedups3-api 才匹配
	nr := mr.PathPrefix("/dedups3/node").Headers("x-amz-dedups3-node-api", "").Subrouter()
	nr.Methods(http.MethodGet).Path("/{blockID}").HandlerFunc(handler.ReadBlockHandler).Queries("readBlock", "").Name("ReadBlock")
}
