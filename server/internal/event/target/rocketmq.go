package target

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"strings"
	"sync"
	"time"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/mageg-x/dedups3/internal/utils"
)

// RocketMQArgs 包含RocketMQ目标的配置参数
type RocketMQArgs struct {
	TargetArgHead
	NameServerAddr string `json:"nameServerAddr"` // NameServer地址，多个地址用逗号分隔
	Topic          string `json:"topic"`          // 主题名称
	Group          string `json:"group"`          // 生产者组名
}

// 添加连接状态管理
type rocketMQConnectionState struct {
	producer rocketmq.Producer
	mu       sync.RWMutex
}

// RocketMQTarget 优化后的 RocketMQTarget 结构
type RocketMQTarget struct {
	initOnce    utils.Init
	args        RocketMQArgs
	state       *rocketMQConnectionState
	cancel      context.CancelFunc
	cancelCh    <-chan struct{}
	maxRetries  int
	retryDelay  time.Duration
	mu          sync.RWMutex
	sendTimeout time.Duration
}

// NewRocketMQTarget 创建新的RocketMQ目标
func NewRocketMQTarget(ctx context.Context, args RocketMQArgs) (*RocketMQTarget, error) {
	if args.NameServerAddr == "" {
		return nil, errors.New("rocketmq name server address cannot be empty")
	}

	if args.Topic == "" {
		args.Topic = "minio_events"
	}

	if args.Group == "" {
		args.Group = "minio_event_producer_group"
	}

	_ctx, cancel := context.WithCancel(ctx)
	target := &RocketMQTarget{
		args:     args,
		cancel:   cancel,
		cancelCh: _ctx.Done(),
	}

	return target, nil
}

// ID 返回目标ID
func (target *RocketMQTarget) ID() string {
	return target.args.ID
}

func (target *RocketMQTarget) Arn() string {
	return target.args.Arn
}

func (target *RocketMQTarget) Owner() string {
	return target.args.Owner
}

func (target *RocketMQTarget) Type() string {
	return EVENT_TARGET_TYPE_ROCKETMQ
}

func (target *RocketMQTarget) Init() error {
	return target.initOnce.Do(func() error {
		target.state = &rocketMQConnectionState{}
		target.maxRetries = 3
		target.retryDelay = 2 * time.Second
		target.sendTimeout = 30 * time.Second
		return target.connect()
	})
}

func (target *RocketMQTarget) IsActive() (bool, error) {
	if err := target.Init(); err != nil {
		return false, err
	}

	target.state.mu.RLock()
	defer target.state.mu.RUnlock()

	select {
	case <-target.cancelCh:
		return false, errors.New("rocketmq target is closed")
	default:
	}

	// 更轻量级的健康检查 - 检查生产者状态
	// RocketMQ 客户端没有直接的状态检查方法，我们使用快速测试
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// 创建一个非常小的测试消息
	testMsg := &primitive.Message{
		Topic: target.args.Topic,
		Body:  []byte("ping"),
	}
	testMsg.WithProperty("health_check", "true")

	_, err := target.state.producer.SendSync(ctx, testMsg)
	if err != nil {
		return false, fmt.Errorf("rocketmq health check failed: %w", err)
	}

	return true, nil
}

func (target *RocketMQTarget) Send(eventData Event) error {
	if err := target.Init(); err != nil {
		return err
	}

	select {
	case <-target.cancelCh:
		return errors.New("rocketmq target is closed")
	default:
	}

	// 序列化事件数据
	data, err := json.Marshal(eventData)
	if err != nil {
		return fmt.Errorf("failed to marshal event data: %w", err)
	}

	// 带重试的发送
	return target.sendWithRetry(data, eventData)
}

func (target *RocketMQTarget) Close() error {
	target.cancel()

	target.mu.Lock()
	defer target.mu.Unlock()

	return target.cleanup()
}

// GetArg 返回RocketMQ目标的参数
func (target *RocketMQTarget) GetArg() (interface{}, error) {
	return target.args, nil
}

func (target *RocketMQTarget) connect() error {
	target.state.mu.Lock()
	defer target.state.mu.Unlock()

	// 清理旧连接
	_ = target.cleanup()

	// 解析 NameServer 地址
	nameServers := strings.Split(target.args.NameServerAddr, ",")
	for i, addr := range nameServers {
		nameServers[i] = strings.TrimSpace(addr)
	}

	// 创建生产者配置
	opts := []producer.Option{
		producer.WithNameServer(nameServers),
		producer.WithRetry(2), // 减少默认重试次数，由应用层控制
		producer.WithGroupName(target.args.Group),
		producer.WithInstanceName(fmt.Sprintf("dedups3_%s_%d", target.args.ID, time.Now().Unix())),
		producer.WithQueueSelector(producer.NewRoundRobinQueueSelector()), // 轮询选择队列
		producer.WithSendMsgTimeout(target.sendTimeout),                   // 设置发送超时
	}

	// 创建生产者
	var err error
	target.state.producer, err = rocketmq.NewProducer(opts...)
	if err != nil {
		return fmt.Errorf("failed to create rocketmq producer: %w", err)
	}

	// 启动生产者
	if err = target.state.producer.Start(); err != nil {
		return fmt.Errorf("failed to start rocketmq producer: %w", err)
	}

	// 验证连接
	if err = target.verifyConnection(); err != nil {
		_ = target.state.producer.Shutdown()
		return fmt.Errorf("rocketmq connection verification failed: %w", err)
	}

	return nil
}

func (target *RocketMQTarget) verifyConnection() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 发送一个轻量级的测试消息
	testMsg := &primitive.Message{
		Topic: target.args.Topic,
		Body:  []byte("connection_test"),
	}
	testMsg.WithProperty("test", "true")
	testMsg.WithProperty("timestamp", fmt.Sprintf("%d", time.Now().Unix()))

	// 使用异步发送避免阻塞
	result, err := target.state.producer.SendSync(ctx, testMsg)
	if err != nil {
		return err
	}

	if result.Status != primitive.SendOK {
		return fmt.Errorf("test message send failed with status: %#v", result.Status)
	}

	return nil
}

func (target *RocketMQTarget) sendWithRetry(data []byte, eventData Event) error {
	var lastErr error

	for i := 0; i < target.maxRetries; i++ {
		select {
		case <-target.cancelCh:
			return errors.New("target closed during send")
		default:
		}

		if err := target.trySend(data, eventData); err != nil {
			lastErr = err

			// 如果是连接错误，尝试重连
			if isRocketMQConnectionError(err) {
				if reconnectErr := target.reconnect(); reconnectErr != nil {
					lastErr = fmt.Errorf("send failed and reconnect failed: %w (original: %v)", reconnectErr, err)
				}
				// 等待后重试
				time.Sleep(target.retryDelay * time.Duration(i+1))
				continue
			}
			// 非连接错误直接返回
			return err
		}
		return nil
	}

	return fmt.Errorf("failed to send message after %d attempts: %w", target.maxRetries, lastErr)
}

func (target *RocketMQTarget) trySend(data []byte, eventData Event) error {
	target.state.mu.RLock()
	defer target.state.mu.RUnlock()

	if target.state.producer == nil {
		return errors.New("rocketmq producer not available")
	}

	// 创建消息
	msg := &primitive.Message{
		Topic: target.args.Topic,
		Body:  data,
	}

	// 设置消息属性
	target.enrichMessage(msg, eventData)

	// 创建带超时的上下文
	ctx, cancel := context.WithTimeout(context.Background(), target.sendTimeout)
	defer cancel()

	// 发送消息
	result, err := target.state.producer.SendSync(ctx, msg)
	if err != nil {
		return fmt.Errorf("failed to send message to rocketmq: %w", err)
	}

	// 检查发送结果
	if result.Status != primitive.SendOK {
		return fmt.Errorf("rocketmq send failed with status: %#v", result.Status)
	}

	return nil
}

// 丰富消息属性
func (target *RocketMQTarget) enrichMessage(msg *primitive.Message, eventData Event) {
	// 添加基本属性
	msg.WithProperty("producer_id", target.args.ID)
	msg.WithProperty("send_time", time.Now().Format(time.RFC3339))
	msg.WithProperty("event_count", fmt.Sprintf("%d", len(eventData.Records)))

	// 添加事件属性
	if len(eventData.Records) > 0 {
		record := eventData.Records[0]
		msg.WithProperty("event_name", record.EventName)
		msg.WithProperty("event_source", record.EventSource)
		msg.WithProperty("aws_region", record.AwsRegion)
		msg.WithProperty("event_time", record.EventTime)

		if record.S3.Bucket.Name != "" {
			msg.WithProperty("bucket_name", record.S3.Bucket.Name)
			// 设置消息Tag用于过滤
			msg.WithTag(record.S3.Bucket.Name)
		}

		if record.S3.Object.Key != "" {
			msg.WithProperty("object_key", record.S3.Object.Key)
			// 设置消息Key用于去重
			msg.WithKeys([]string{record.S3.Object.Key})
		}

		// 添加对象大小信息
		msg.WithProperty("object_size", fmt.Sprintf("%d", record.S3.Object.Size))
	}
}

// 判断是否为RocketMQ连接错误
func isRocketMQConnectionError(err error) bool {
	if err == nil {
		return false
	}

	errorStr := err.Error()
	connectionErrors := []string{
		"connection refused",
		"connection reset",
		"dial tcp",
		"timeout",
		"io timeout",
		"EOF",
		"network error",
		"broker unavailable",
		"no route info",
	}

	for _, connErr := range connectionErrors {
		if strings.Contains(strings.ToLower(errorStr), connErr) {
			return true
		}
	}

	return false
}

// 重连方法
func (target *RocketMQTarget) reconnect() error {
	target.mu.Lock()
	defer target.mu.Unlock()

	return target.connect()
}

func (target *RocketMQTarget) cleanup() error {
	if target.state == nil {
		return nil
	}

	target.state.mu.Lock()
	defer target.state.mu.Unlock()

	if target.state.producer != nil {
		// 优雅关闭，等待未完成的消息发送
		if err := target.state.producer.Shutdown(); err != nil {
			return fmt.Errorf("failed to shutdown rocketmq producer: %w", err)
		}
		target.state.producer = nil
	}

	return nil
}
