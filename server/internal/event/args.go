package event

import (
	"fmt"
	"time"

	"github.com/mageg-x/dedups3/internal/event/target"
	xhttp "github.com/mageg-x/dedups3/internal/http"
)

type ObjectInfo struct {
	VersionID string    `json:"versionId"`
	Name      string    `json:"name"`
	Size      int64     `json:"size"`
	ETag      string    `json:"eTag"`
	ModTime   time.Time `json:"modTime"`
}

type EventArgs struct {
	EventName    string
	BucketName   string
	Object       ObjectInfo
	ReqParams    map[string]string
	RespElements map[string]string
	Host         string
	UserAgent    string
}

// ToEvent - converts to notification event.
func (args EventArgs) ToEvent() target.Event {
	eventTime := time.Now().UTC()
	uniqueID := fmt.Sprintf("%X", eventTime.UnixNano())
	if !args.Object.ModTime.IsZero() {
		uniqueID = fmt.Sprintf("%X", args.Object.ModTime.UnixNano())
	}

	respElements := map[string]string{
		xhttp.AmzRequestID:     args.RespElements["requestId"],
		xhttp.AmzRequestHostID: args.RespElements["nodeId"],
	}

	if args.RespElements["content-length"] != "" {
		respElements["content-length"] = args.RespElements["content-length"]
	}

	record := target.Record{
		EventVersion:      "2.0",
		EventSource:       "minio:s3",
		AwsRegion:         args.ReqParams["region"],
		EventTime:         eventTime.Format(target.AMZTimeFormat),
		EventName:         args.EventName,
		UserIdentity:      target.UserIdentity{PrincipalID: args.ReqParams["principalId"]},
		RequestParameters: args.ReqParams,
		ResponseElements:  respElements,
		S3: target.S3Entity{
			S3SchemaVersion: "1.0",
			ConfigurationID: "Config",
			Bucket: target.Bucket{
				Name:          args.BucketName,
				OwnerIdentity: target.OwnerIdentity{PrincipalID: args.ReqParams["principalId"]},
				ARN:           "arn:aws:s3:::" + args.BucketName,
			},
			Object: target.Object{
				Key:       args.Object.Name,
				VersionID: args.Object.VersionID,
				Sequencer: uniqueID,
				ETag:      args.Object.ETag,
				Size:      args.Object.Size,
			},
		},
	}

	// 根据AWS文档，glacierEventData 只应在 ObjectRestoreCompleted 事件中可见
	if args.EventName == "s3:ObjectRestore" {
		// 优先从请求参数中获取恢复相关信息
		// 实际实现中，这些值应该来自于实际的恢复请求参数或对象元数据
		restorationExpiryTime := eventTime.Add(24 * time.Hour).Format(target.AMZTimeFormat)
		restoreStorageClass := "GLACIER_IR"

		// 尝试从请求参数中获取恢复过期时间
		if expiryTime, exists := args.ReqParams["lifecycleRestorationExpiryTime"]; exists && expiryTime != "" {
			restorationExpiryTime = expiryTime
		}

		// 尝试从请求参数中获取恢复存储类别
		if storageClass, exists := args.ReqParams["lifecycleRestoreStorageClass"]; exists && storageClass != "" {
			restoreStorageClass = storageClass
		}

		record.GlacierEventData = &target.GlacierEventData{
			RestoreEventData: target.RestoreEventData{
				LifecycleRestorationExpiryTime: restorationExpiryTime,
				LifecycleRestoreStorageClass:   restoreStorageClass,
			},
		}
	}
	return target.Event{
		Records: []target.Record{record},
	}
}
