package event

import (
	"encoding/json"
	"fmt"
	xconf "github.com/mageg-x/dedups3/internal/config"
	"net/http"
	"time"

	xhttp "github.com/mageg-x/dedups3/internal/http"
	"github.com/mageg-x/dedups3/internal/logger"
	"github.com/mageg-x/dedups3/internal/utils"
	"github.com/mageg-x/dedups3/meta"
)

type Sender interface {
	Send(data []byte) error
}

// Record 表示单个事件记录
type Record struct {
	RequestID         string            `json:"requestID""`
	EventID           string            `json:"eventID"`
	EventVersion      string            `json:"eventVersion"`
	EventSource       string            `json:"eventSource"`
	AwsRegion         string            `json:"awsRegion"`
	EventTime         time.Time         `json:"eventTime"`
	EventName         string            `json:"eventName"`
	UserIdentity      UserIdentity      `json:"userIdentity"`
	SourceIPAddress   string            `json:"sourceIPAddress"`
	RequestParameters map[string]string `json:"requestParameters"`
	ResponseElements  map[string]string `json:"responseElements"`
	S3                *S3Entity         `json:"s3,omitempty"`
	GlacierEventData  *GlacierEventData `json:"glacierEventData,omitempty"`
}

// 用户身份信息
type UserIdentity struct {
	Type        string `json:"type"`
	PrincipalID string `json:"principalId"`
	ARN         string `json:"arn"`
	AccountID   string `json:"accountId,omitempty"`
	UserName    string `json:"userName,omitempty"`
	AccessKeyID string `json:"accessKeyId,omitempty"`
}

// S3Entity 表示S3相关的事件数据
type S3Entity struct {
	S3SchemaVersion string `json:"s3SchemaVersion"`
	ConfigurationID string `json:"configurationId"`
	Bucket          Bucket `json:"bucket"`
	Object          Object `json:"object"`
}

// Bucket 表示存储桶信息
type Bucket struct {
	Name          string        `json:"name"`
	OwnerIdentity OwnerIdentity `json:"ownerIdentity"`
	ARN           string        `json:"arn"`
}

// OwnerIdentity 表示存储桶所有者标识
type OwnerIdentity struct {
	PrincipalID string `json:"principalId"`
}

// Object 表示对象信息
type Object struct {
	Key       string `json:"key"`
	Size      int64  `json:"size,omitempty"`
	ETag      string `json:"eTag,omitempty"`
	VersionID string `json:"versionId,omitempty"`
	Sequencer string `json:"sequencer"`
}

// GlacierEventData 表示Glacier相关的事件数据
type GlacierEventData struct {
	RestoreEventData RestoreEventData `json:"restoreEventData"`
}

// RestoreEventData 表示对象恢复相关的事件数据
type RestoreEventData struct {
	LifecycleRestorationExpiryTime string `json:"lifecycleRestorationExpiryTime"`
	LifecycleRestoreStorageClass   string `json:"lifecycleRestoreStorageClass"`
}

func EventLog(r *http.Request, w *xhttp.RespWriter, filterKeys map[string]struct{}, sender Sender) {
	// 从上下文中获取跟踪数据
	ctx := r.Context()
	traceCtxt, ok := ctx.Value(xhttp.ContextTraceKey).(*xhttp.TraceCtxt)
	logger.GetLogger("dedups3").Tracef("get trace context: %#v", traceCtxt)
	if !ok {
		traceCtxt = &xhttp.TraceCtxt{
			Attributes: make(map[string]interface{}),
		}
	}

	// 从上下文中获取各种属性
	requestID := toString(traceCtxt.Attributes["requestID"])
	accountID := toString(traceCtxt.Attributes["accountId"])
	username := toString(traceCtxt.Attributes["username"])
	apiName := toString(traceCtxt.Attributes["apiName"])
	accessKeyId := toString(traceCtxt.Attributes["accessKeyId"])
	principalId := toString(traceCtxt.Attributes["principalId"])
	region := toString(traceCtxt.Attributes["region"])
	bucketName := toString(traceCtxt.Attributes["bucketName"])
	objectKey := toString(traceCtxt.Attributes["objectKey"])
	objectSize := toInt64(traceCtxt.Attributes["objectSize"])
	objectETag := toString(traceCtxt.Attributes["objectETag"])
	versionID := toString(traceCtxt.Attributes["versionID"])

	if apiName == "" {
		return
	}
	if region == "" {
		cfg := xconf.Get()
		region = cfg.Node.Region
	}

	// 构建用户身份信息
	arn := fmt.Sprintf("arn:aws:iam::%s:user/%s", accountID, username)
	if accountID == "" || username == "" {
		arn = ""
	}

	userIdentity := UserIdentity{
		Type:        "IAMUser",
		PrincipalID: principalId,
		ARN:         arn,
		AccountID:   accountID,
		UserName:    username,
		AccessKeyID: accessKeyId,
	}

	if accountID == meta.GenerateAccountID(username) {
		userIdentity.UserName = "root"
		userIdentity.Type = "IAMAccount"
		userIdentity.ARN = fmt.Sprintf("arn:aws:iam::%s:user/%s", accountID, "root")
	}

	// 创建事件记录
	record := &Record{
		RequestID:         requestID,
		EventVersion:      "2.1",
		EventSource:       "aws:s3",
		EventID:           utils.GenUUID(),
		AwsRegion:         region,
		EventTime:         time.Now().UTC(),
		EventName:         apiName,
		SourceIPAddress:   utils.GetSourceIP(r),
		UserIdentity:      userIdentity,
		RequestParameters: utils.ExtractReqParams(r, filterKeys),
		ResponseElements:  utils.ExtractRespElements(w),
	}

	// 如果有bucket和object信息，构建S3Entity
	if bucketName != "" {
		record.S3 = &S3Entity{
			S3SchemaVersion: "1.0",
			ConfigurationID: "Config",
			Bucket: Bucket{
				Name:          bucketName,
				OwnerIdentity: OwnerIdentity{PrincipalID: principalId},
				ARN:           meta.FormatBucketARN(accountID, bucketName),
			},
		}

		// 如果有object信息
		if objectKey != "" {
			record.S3.Object = Object{
				Key:       objectKey,
				Size:      objectSize,
				ETag:      objectETag,
				VersionID: versionID,
				Sequencer: fmt.Sprintf("%X", record.EventTime.UnixNano()),
			}
		}

	}

	// 记录事件日志（序列化并发送）
	recordJSON, err := json.Marshal(record)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("Failed to marshal event record: %v", err)
		return
	}

	if err := sender.Send(recordJSON); err != nil {
		logger.GetLogger("dedups3").Errorf("Failed to send event record: %v", err)
	}
	// 记录到日志文件
	logger.GetLogger("dedups3").Debugf("EVENT: %s", string(recordJSON))
}

// toString 辅助函数，将任意类型转换为字符串
func toString(val interface{}) string {
	if val == nil {
		return ""
	}
	s, ok := val.(string)
	if !ok {
		return fmt.Sprintf("%v", val)
	}
	return s
}

// toInt64 辅助函数，将任意类型转换为int64
func toInt64(val interface{}) int64 {
	if val == nil {
		return 0
	}
	s, ok := val.(int64)
	if !ok {
		return 0
	}
	return s
}
