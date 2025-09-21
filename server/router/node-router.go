package router

import (
	"net/http"

	"github.com/gorilla/mux"

	"github.com/mageg-x/boulder/handler"
)

func registerNodeRouter(mr *mux.Router) {
	// 创建子路由，仅当请求头包含 x-amz-boulder-api 才匹配
	nr := mr.PathPrefix("/boulder/node").Headers("x-amz-boulder-node-api", "").Subrouter()
	nr.Methods(http.MethodGet).Path("/{blockID}").HandlerFunc(handler.ReadBlockHandler).Queries("readBlock", "").Name("ReadBlock")
}
