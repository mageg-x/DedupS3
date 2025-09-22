package handler

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"

	xhttp "github.com/mageg-x/boulder/internal/http"
	"github.com/mageg-x/boulder/internal/logger"
	"github.com/mageg-x/boulder/internal/storage/block"
	"github.com/mageg-x/boulder/internal/utils"
	"github.com/mageg-x/boulder/service/node"
)

func ReadBlockHandler(w http.ResponseWriter, r *http.Request) {
	// 从路径中提取blockID
	vars := utils.DecodeVars(mux.Vars(r))
	blockID := vars["blockID"]
	logger.GetLogger("boulder").Debugf("API called: ReadBlockDataHandler blockID %s head %#v", blockID, r.Header)
	if !utils.IsValidUUID(blockID) {
		logger.GetLogger("boulder").Errorf("Missing or invalid block_id in read request: %s", blockID)
		xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidArgument)
		return
	}

	// 从查询参数中提取offset和size
	query := utils.DecodeQuerys(r.URL.Query())

	offset := int64(0)
	if offsetStr := query.Get("offset"); offsetStr != "" {
		val, err := strconv.ParseInt(offsetStr, 10, 64)
		if err != nil {
			logger.GetLogger("boulder").Errorf("Invalid offset parameter: %s", offsetStr)
			xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidArgument)
			return
		}
		offset = val
	}

	size := int64(0)
	if sizeStr := query.Get("size"); sizeStr != "" {
		val, err := strconv.ParseInt(sizeStr, 10, 64)
		if err != nil {
			logger.GetLogger("boulder").Errorf("Invalid size parameter: %s", sizeStr)
			xhttp.WriteAWSErr(w, r, xhttp.ErrInvalidArgument)
			return
		}
		size = val
	}

	logger.GetLogger("boulder").Debugf("ReadBlockHandler called with blockID=%s, offset=%d, size=%d", blockID, offset, size)

	localStore := node.NodeService{}
	data, err := localStore.ReadLocalBlock(blockID, offset, size)
	if err != nil || len(data) == 0 {
		logger.GetLogger("boulder").Errorf("ReadLocalBlock %s failed: %v", blockID, err)
		if errors.Is(err, block.ErrBlockNotFound) {
			xhttp.WriteAWSErr(w, r, xhttp.ErrNoSuchKey)
			return
		}
		xhttp.WriteAWSErr(w, r, xhttp.ErrInternalError)
		return
	}

	// 获取请求ID并添加到响应头
	requestID := xhttp.GetRequestID(r.Context())
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
	w.Header().Set("x-amz-request-id", requestID)
	w.WriteHeader(http.StatusOK)

	_, err = w.Write(data)
	if err != nil {
		logger.GetLogger("boulder").Errorf("Failed to write block %s data to response: %v", blockID, err)
		return
	}
	logger.GetLogger("boulder").Debugf("Successfully preparing block %s for response, size: %d bytes", blockID, len(data))
}
