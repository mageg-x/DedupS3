package audit

import (
	"encoding/json"
	"fmt"
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

type Entry struct {
	RequestID          string            `json:"requestID,omitempty"`
	Version            string            `json:"version"`     // e.g., "1.08"
	EventID            string            `json:"eventId"`     // UUID
	EventTime          time.Time         `json:"eventTime"`   // ISO8601 UTC
	EventName          string            `json:"eventName"`   // API name, e.g., "PutObject"
	EventSource        string            `json:"eventSource"` // e.g., "s3.amazonaws.com"
	EventType          string            `json:"eventType"`   // "AwsApiCall", "AwsServiceEvent", etc.
	ReadOnly           *bool             `json:"readOnly,omitempty"`
	AccessKeyId        string            `json:"accessKeyId,omitempty"`
	UserAgent          string            `json:"userAgent,omitempty"`
	Region             string            `json:"region,omitempty"`
	SourceIPAddress    string            `json:"sourceIPAddress"`              // IP or "AWS Internal"
	RecipientAccountID string            `json:"recipientAccountId,omitempty"` // 12-digit AWS account ID
	Resources          []Resource        `json:"resources,omitempty"`
	RequestPath        string            `json:"requestPath,omitempty"`
	RequestHost        string            `json:"requestHost,omitempty"`
	RequestParameters  map[string]string `json:"requestParameters,omitempty"`
	ResponseElements   map[string]string `json:"responseElements,omitempty"`
	UserIdentity       UserIdentity      `json:"userIdentity"`
	StatusCode         int               `json:"statusCode"`
	InputBytes         int64             `json:"inputBytes"`
	OutputBytes        int64             `json:"outputBytes"`
	ErrorCode          string            `json:"errorCode,omitempty"`
	ErrorMessage       string            `json:"errorMessage,omitempty"`
}

type UserIdentity struct {
	Type        string `json:"type"` // "IAMUser", "AssumedRole", "Root", "AWSService"
	PrincipalID string `json:"principalId,omitempty"`
	ARN         string `json:"arn,omitempty"`
	AccountID   string `json:"accountId,omitempty"`
	AccessKeyId string `json:"accessKeyId,omitempty"`
	UserName    string `json:"userName,omitempty"`
	InvokedBy   string `json:"invokedBy,omitempty"` // e.g., "elasticloadbalancing.amazonaws.com" 上游服务
}

type Resource struct {
	ARN  string `json:"ARN,omitempty"`
	Type string `json:"type,omitempty"` // e.g., "AWS::S3::Object"
}

// AuditLog - logs audit logs to all audit targets.
func AuditLog(r *http.Request, w *xhttp.RespWriter, filterKeys map[string]struct{}, sender Sender) {
	// 从上下文中获取跟踪数据
	ctx := r.Context()
	traceCtxt, ok := ctx.Value(xhttp.ContextTraceKey).(*xhttp.TraceCtxt)
	logger.GetLogger("dedups3").Tracef("get trace context: %#v", traceCtxt)
	if !ok {
		traceCtxt = &xhttp.TraceCtxt{
			Attributes: make(map[string]interface{}),
		}
	}

	// 要在其他地方设置好这些kv
	requestID := toString(traceCtxt.Attributes["requestID"])
	accountID := toString(traceCtxt.Attributes["accountId"])
	username := toString(traceCtxt.Attributes["username"])
	apiName := toString(traceCtxt.Attributes["apiName"])
	accessKeyId := toString(traceCtxt.Attributes["accessKeyId"])
	errorCode := toString(traceCtxt.Attributes["errorCode"])
	errorMessage := toString(traceCtxt.Attributes["errorMessage"])
	principalId := toString(traceCtxt.Attributes["principalId"])
	region := toString(traceCtxt.Attributes["region"])

	if apiName == "" {
		return
	}

	arn := fmt.Sprintf("arn:aws:iam::%s:user/%s", accountID, username)
	if accountID == "" || username == "" {
		arn = ""
	}

	identity := UserIdentity{
		Type:        "IAMUser",
		PrincipalID: principalId,
		ARN:         arn,
		AccountID:   accountID,
		AccessKeyId: accessKeyId,
		UserName:    username,
		InvokedBy:   "",
	}

	if accountID == meta.GenerateAccountID(username) {
		identity.UserName = "root"
		identity.Type = "IAMAccount"
		identity.ARN = fmt.Sprintf("arn:aws:iam::%s:user/%s", accountID, "root")
	}

	// 创建审计日志条目
	entry := &Entry{
		RequestID:          requestID,
		Version:            "1.08",
		EventID:            utils.GenUUID(),
		EventTime:          time.Now().UTC(),
		EventName:          apiName,
		EventSource:        "s3.amazonaws.com",
		EventType:          "AwsApiCall",
		UserAgent:          r.UserAgent(),
		Region:             region,
		SourceIPAddress:    utils.GetSourceIP(r),
		RecipientAccountID: accountID, // 默认账户ID
		RequestPath:        r.URL.Path,
		RequestHost:        r.Host,
		RequestParameters:  make(map[string]string),
		ResponseElements:   make(map[string]string),
		UserIdentity:       identity,
		Resources:          getResource(traceCtxt),
		ErrorCode:          errorCode,
		ErrorMessage:       errorMessage,
	}

	// 设置是否为只读操作
	readOnly := (r.Method == "GET" || r.Method == "HEAD" || r.Method == "OPTIONS")
	entry.ReadOnly = &readOnly

	// 从请求中提取参数
	entry.RequestParameters = utils.ExtractReqParams(r, filterKeys)
	entry.ResponseElements = utils.ExtractRespElements(w)
	entry.InputBytes = r.ContentLength
	entry.StatusCode = w.StatusCode()
	entry.OutputBytes = w.BytesWritten()
	entry.AccessKeyId = accessKeyId

	// 记录审计日志（当前仅记录到日志文件）
	entryJSON, err := json.Marshal(entry)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("Failed to marshal audit entry: %v", err)
		return
	}

	if err := sender.Send(entryJSON); err != nil {
		logger.GetLogger("dedups3").Errorf("Failed to send audit entry: %v", err)
	}
	// 记录到日志文件
	logger.GetLogger("dedups3").Errorf("AUDIT: %s", string(entryJSON))
}

func getResource(traceCtxt *xhttp.TraceCtxt) []Resource {
	// 定义字段名到 Resource.Type 的映射
	typeMap := map[string]string{
		"iamUser":      "AWS::IAM::User",
		"iamGroup":     "AWS::IAM::Group",
		"iamRole":      "AWS::IAM::Role",
		"iamPolicy":    "AWS::IAM::Policy",
		"iamAccessKey": "AWS::IAM::User",
		"iamQuota":     "AWS::IAM::Quota",
		"iamStorage":   "AWS::Storage",
		"iamDebug":     "AWS::IAM::Debug",
		"iamAudit":     "AWS::IAM::Logs",
		"iamEvent":     "AWS::IAM::Logs",
		"iamStats":     "AWS::IAM::User",
	}

	var resources []Resource
	accountID := toString(traceCtxt.Attributes["accountId"])
	bucketName := toString(traceCtxt.Attributes["bucketName"])
	for attrKey, resType := range typeMap {
		if val, exists := traceCtxt.Attributes[attrKey]; exists && val != nil {
			arn := ""
			switch resType {
			case "AWS::IAM::User":
				if attrKey == "iamUser" {
					if meta.GenerateAccountID(toString(val)) == accountID {
						arn = meta.FormatUserARN(accountID, "root")
					} else {
						arn = meta.FormatUserARN(accountID, toString(val))
					}
				} else if attrKey == "iamAccessKey" {
					username := toString(traceCtxt.Attributes["username"])
					if meta.GenerateAccountID(username) == accountID {
						username = "root"
					}
					arn = fmt.Sprintf("arn:aws:iam::%s:user/%s", accountID, username)
				} else if attrKey == "iamStats" {
					arn = meta.FormatUserARN(accountID, toString(val))
				}
			case "AWS::IAM::Group":
				arn = meta.FormatGroupARN(accountID, toString(val))
			case "AWS::IAM::Role":
				arn = meta.FormatRoleARN(accountID, toString(val))
			case "AWS::IAM::Quota":
				arn = "arn:aws:quota::" + toString(val)
			case "AWS::IAM::Debug":
				arn = "arn:aws:debug::" + toString(val)
			case "AWS::IAM::Policy":
				arn = meta.FormatPolicyARN(accountID, toString(val))
			case "AWS::Storage":
				arn = fmt.Sprintf("arn:aws:storage::%s", toString(val))
			case "AWS::IAM::Logs":
				if attrKey == "iamAudit" {
					arn = fmt.Sprintf("arn:aws:log::audit/%s", toString(val))
				} else if attrKey == "iamEvent" {
					arn = fmt.Sprintf("arn:aws:log::event/%s", toString(val))
				}
			}

			if arn != "" {
				resource := Resource{
					ARN:  arn,
					Type: resType,
				}
				resources = append(resources, resource)
			}
		}
	}

	if len(resources) == 0 {
		// objectKey 有可能是 string, 也有可能是 [] string
		objKey := traceCtxt.Attributes["objectKey"]
		var objectKeys []string
		switch v := objKey.(type) {
		case string:
			objectKeys = []string{v}
		case []string:
			objectKeys = v
		default:
			objectKeys = nil
		}
		if bucketName != "" {
			if objectKeys != nil {
				for _, objectKey := range objectKeys {
					resource := Resource{
						ARN:  meta.FormatObjectARN(accountID, bucketName, objectKey),
						Type: "AWS::Object",
					}
					resources = append(resources, resource)
				}
			} else {
				resource := Resource{
					ARN:  meta.FormatBucketARN(accountID, bucketName),
					Type: "AWS::Bucket",
				}
				resources = append(resources, resource)
			}
		}
	}

	return resources
}

func toString(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
