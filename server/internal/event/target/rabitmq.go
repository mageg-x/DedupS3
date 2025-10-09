package target

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/mageg-x/dedups3/internal/utils"
)

// RabbitMQArgs 包含RabbitMQ目标的配置参数
type RabbitMQArgs struct {
	TargetArgHead
	URL        string `json:"url"`        // RabbitMQ连接URL，格式: amqp://user:password@host:port/
	Exchange   string `json:"exchange"`   // 交换机名称
	RoutingKey string `json:"routingKey"` // 路由键
	Queue      string `json:"queue"`      // 队列名称
	Durable    bool   `json:"durable"`    // 队列是否持久化
	AutoDelete bool   `json:"autoDelete"` // 最后一个消费者断开连接时是否自动删除队列
	Exclusive  bool   `json:"exclusive"`  // 队列是否独占
	NoWait     bool   `json:"noWait"`     // 是否不等待服务器确认
}

// 添加连接状态管理
type ampConnectionState struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	mu      sync.RWMutex
}

// 优化后的 RabbitMQTarget 结构
type RabbitMQTarget struct {
	initOnce   utils.Init
	args       RabbitMQArgs
	state      *ampConnectionState
	cancel     context.CancelFunc
	cancelCh   <-chan struct{}
	maxRetries int
	retryDelay time.Duration
	mu         sync.RWMutex
}

func NewRabbitMQTarget(ctx context.Context, args RabbitMQArgs) (*RabbitMQTarget, error) {
	// 创建上下文和取消函数，用于管理资源生命周期
	_ctx, cancel := context.WithCancel(ctx)

	// 初始化RabbitMQTarget结构体
	target := &RabbitMQTarget{
		args:       args,
		cancel:     cancel,
		cancelCh:   _ctx.Done(),
		maxRetries: 3,
		retryDelay: 2 * time.Second,
	}

	return target, nil
}

func (target *RabbitMQTarget) Init() error {
	return target.initOnce.Do(func() error {
		target.state = &ampConnectionState{}
		target.maxRetries = 3
		target.retryDelay = 2 * time.Second
		return target.reconnect()
	})
}

// ID 返回目标ID
func (target *RabbitMQTarget) ID() string {
	return target.args.ID
}

func (target *RabbitMQTarget) Arn() string {
	return target.args.Arn
}

func (target *RabbitMQTarget) Owner() string {
	return target.args.Owner
}

func (target *RabbitMQTarget) Type() string {
	return EVENT_TARGET_TYPE_RABITMQ
}

func (target *RabbitMQTarget) IsActive() (bool, error) {
	if err := target.Init(); err != nil {
		return false, err
	}

	target.state.mu.RLock()
	defer target.state.mu.RUnlock()

	if target.state.conn == nil || target.state.channel == nil {
		return false, errors.New("rabbitmq connection not initialized")
	}

	// 检查连接状态
	if target.state.conn.IsClosed() {
		return false, errors.New("rabbitmq connection is closed")
	}

	// 更轻量级的健康检查 - 检查通道状态
	_, err := target.state.channel.QueueInspect(target.args.Queue)
	if err != nil {
		return false, fmt.Errorf("channel health check failed: %w", err)
	}

	return true, nil
}

func (target *RabbitMQTarget) Send(eventData Event) error {
	if err := target.Init(); err != nil {
		return err
	}

	select {
	case <-target.cancelCh:
		return errors.New("rabbitmq target is closed")
	default:
	}

	// 序列化事件数据
	data, err := json.Marshal(eventData)
	if err != nil {
		return fmt.Errorf("failed to marshal event data: %w", err)
	}

	// 发送消息（带重试）
	return target.sendWithRetry(data)
}

// Close 关闭RabbitMQ连接和通道
func (target *RabbitMQTarget) Close() error {
	target.cancel()

	target.mu.Lock()
	defer target.mu.Unlock()

	return target.cleanup()
}

// GetArg 返回RabbitMQ目标的参数
func (target *RabbitMQTarget) GetArg() (interface{}, error) {
	return target.args, nil
}

// 重连机制
func (target *RabbitMQTarget) reconnect() error {
	target.state.mu.Lock()
	defer target.state.mu.Unlock()

	// 清理旧连接
	_ = target.cleanup()

	var err error
	for i := 0; i < target.maxRetries; i++ {
		select {
		case <-target.cancelCh:
			return errors.New("target closed during reconnect")
		default:
		}

		if err = target.connect(); err == nil {
			return nil
		}

		if i < target.maxRetries-1 {
			time.Sleep(target.retryDelay * time.Duration(i+1))
		}
	}

	return fmt.Errorf("failed to reconnect after %d attempts: %w", target.maxRetries, err)
}

func (target *RabbitMQTarget) connect() error {
	var err error

	// 使用带超时的连接
	target.state.conn, err = amqp.DialConfig(target.args.URL, amqp.Config{
		Dial: func(network, addr string) (net.Conn, error) {
			return amqp.DefaultDial(30*time.Second)(network, addr)
		},
	})
	if err != nil {
		return fmt.Errorf("failed to connect to rabbitmq: %w", err)
	}

	// 创建通道
	target.state.channel, err = target.state.conn.Channel()
	if err != nil {
		_ = target.state.conn.Close()
		return fmt.Errorf("failed to open channel: %w", err)
	}

	// 设置确认模式
	if err = target.state.channel.Confirm(false); err != nil {
		return fmt.Errorf("failed to set confirm mode: %w", err)
	}

	// 声明交换机和队列
	if err = target.declareResources(); err != nil {
		return err
	}

	return nil
}

func (target *RabbitMQTarget) declareResources() error {
	// 声明交换机
	err := target.state.channel.ExchangeDeclare(
		target.args.Exchange,
		"topic",
		target.args.Durable,
		false, // auto-delete
		false, // internal
		false, // no-wait
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to declare exchange: %w", err)
	}

	// 声明队列
	_, err = target.state.channel.QueueDeclare(
		target.args.Queue,
		target.args.Durable,
		target.args.AutoDelete,
		target.args.Exclusive,
		target.args.NoWait,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue: %w", err)
	}

	// 绑定队列
	err = target.state.channel.QueueBind(
		target.args.Queue,
		target.args.RoutingKey,
		target.args.Exchange,
		false, // no-wait
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to bind queue: %w", err)
	}

	return nil
}

func (target *RabbitMQTarget) sendWithRetry(data []byte) error {
	var lastErr error

	for i := 0; i < target.maxRetries; i++ {
		select {
		case <-target.cancelCh:
			return errors.New("target closed during send")
		default:
		}

		if err := target.trySend(data); err != nil {
			lastErr = err

			// 如果是连接问题，尝试重连
			if errors.Is(err, amqp.ErrClosed) || strings.Contains(err.Error(), "connection closed") {
				if reconnectErr := target.reconnect(); reconnectErr != nil {
					lastErr = fmt.Errorf("send failed and reconnect failed: %w (original: %v)", reconnectErr, err)
				}
				continue
			}
			return err
		}
		return nil
	}

	return fmt.Errorf("failed to send message after %d attempts: %w", target.maxRetries, lastErr)
}

func (target *RabbitMQTarget) trySend(data []byte) error {
	target.state.mu.RLock()
	defer target.state.mu.RUnlock()

	if target.state.channel == nil {
		return errors.New("rabbitmq channel not available")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	msg := amqp.Publishing{
		ContentType:  "application/json",
		Body:         data,
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
	}

	// 发布消息
	err := target.state.channel.PublishWithContext(ctx,
		target.args.Exchange,
		target.args.RoutingKey,
		false, // mandatory
		false, // immediate
		msg,
	)
	if err != nil {
		return fmt.Errorf("publish failed: %w", err)
	}

	// 等待确认（使用更高效的方式）
	select {
	case confirm := <-target.state.channel.NotifyPublish(make(chan amqp.Confirmation, 1)):
		if !confirm.Ack {
			return errors.New("message not acknowledged by broker")
		}
	case <-time.After(5 * time.Second):
		return errors.New("timeout waiting for message confirmation")
	case <-target.cancelCh:
		return errors.New("target closed during confirmation")
	}

	return nil
}

func (target *RabbitMQTarget) cleanup() error {
	var errs []error

	if target.state != nil {
		target.state.mu.Lock()
		defer target.state.mu.Unlock()

		if target.state.channel != nil {
			if err := target.state.channel.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
				errs = append(errs, fmt.Errorf("channel close: %w", err))
			}
			target.state.channel = nil
		}

		if target.state.conn != nil {
			if err := target.state.conn.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
				errs = append(errs, fmt.Errorf("connection close: %w", err))
			}
			target.state.conn = nil
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors during cleanup: %v", errs)
	}
	return nil
}
