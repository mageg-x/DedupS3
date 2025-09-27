package target

const (
	EventTargetPrefix = "aws:event:target:"
	AMZTimeFormat     = "2006-01-02T15:04:05.000Z"

	EVENT_TARGET_TYPE_MYSQL    = "mysql-target"
	EVENT_TARGET_TYPE_REDIS    = "redis-target"
	EVENT_TARGET_TYPE_RABITMQ  = "rabbitmq-target"
	EVENT_TARGET_TYPE_ROCKETMQ = "rocketmq-target"
	EVENT_TARGET_TYPE_WEBHOOK  = "webhook-target"
)

// Event 表示AWS S3事件通知的事件结构
type Event struct {
	Records []Record `json:"Records"`
}

// Record 表示单个事件记录
type Record struct {
	EventVersion      string            `json:"eventVersion"`
	EventSource       string            `json:"eventSource"`
	AwsRegion         string            `json:"awsRegion"`
	EventTime         string            `json:"eventTime"`
	EventName         string            `json:"eventName"`
	UserIdentity      UserIdentity      `json:"userIdentity"`
	RequestParameters map[string]string `json:"requestParameters"`
	ResponseElements  map[string]string `json:"responseElements"`
	S3                S3Entity          `json:"s3"`
	GlacierEventData  *GlacierEventData `json:"glacierEventData,omitempty"`
}

// UserIdentity 表示引起事件的IAM资源标识
type UserIdentity struct {
	PrincipalID string `json:"principalId"`
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

type TargetArgHead struct {
	ID    string `json:"id"`
	Type  string `json:"type"`
	Owner string `json:"owner"`
	Arn   string `json:"arn"`
}

// Target 代表一个接收点的接口列表
type EventTarget interface {
	Init() error
	ID() string
	Arn() string
	Owner() string
	Type() string
	IsActive() (bool, error)
	Send(Event) error
	GetArg() (interface{}, error)
	Close() error
}
