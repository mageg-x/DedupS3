package target

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/mageg-x/boulder/internal/utils"
	"github.com/redis/go-redis/v9"
)

// 存储类型常量
const (
	StorageTypeList   = "list"
	StorageTypeStream = "stream"
	StorageTypePubSub = "pubsub"
	StorageTypeSet    = "set"
)

// 添加连接状态管理
type redisConnectionState struct {
	client *redis.Client
	mu     sync.RWMutex
}

type RedisArgs struct {
	TargetArgHead
	Addr         string        `json:"addr"`
	Password     string        `json:"password"`
	DB           int           `json:"db"`
	Key          string        `json:"key"`
	Type         string        `json:"type"`         // 存储类型
	MaxLength    int64         `json:"maxLength"`    // 列表/流的最大长度
	TrimApprox   bool          `json:"trimApprox"`   // 是否使用近似修剪
	StreamMaxLen int64         `json:"streamMaxLen"` // 流的最大长度
	SetExpire    time.Duration `json:"setExpire"`    // 集合过期时间
}

// RedisTarget 实现Target接口的Redis目标
type RedisTarget struct {
	initOnce   utils.Init
	args       RedisArgs
	state      *redisConnectionState
	cancel     context.CancelFunc
	cancelCh   <-chan struct{}
	maxRetries int
	retryDelay time.Duration
	mu         sync.RWMutex
}

// NewRedisTarget 创建新的Redis目标
func NewRedisTarget(ctx context.Context, args RedisArgs) (*RedisTarget, error) {
	if args.Addr == "" {
		return nil, errors.New("redis address cannot be empty")
	}

	if args.Key == "" {
		args.Key = "boulder:events"
	}

	_ctx, cancel := context.WithCancel(ctx)
	target := &RedisTarget{
		args:     args,
		cancel:   cancel,
		cancelCh: _ctx.Done(),
	}

	return target, nil
}

// ID 返回目标ID
func (target *RedisTarget) ID() string {
	return target.args.ID
}

func (target *RedisTarget) Arn() string {
	return target.args.Arn
}

func (target *RedisTarget) Owner() string {
	return target.args.Owner
}

func (target *RedisTarget) Type() string {
	return EVENT_TARGET_TYPE_REDIS
}

func (target *RedisTarget) Init() error {
	return target.initOnce.Do(func() error {
		target.state = &redisConnectionState{}
		target.maxRetries = 3
		target.retryDelay = 2 * time.Second
		return target.connect()
	})
}

func (target *RedisTarget) IsActive() (bool, error) {
	if err := target.Init(); err != nil {
		return false, err
	}

	target.state.mu.RLock()
	defer target.state.mu.RUnlock()

	select {
	case <-target.cancelCh:
		return false, errors.New("redis target is closed")
	default:
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// 更轻量级的健康检查
	if err := target.state.client.Ping(ctx).Err(); err != nil {
		return false, fmt.Errorf("redis health check failed: %w", err)
	}

	return true, nil
}

func (target *RedisTarget) Send(eventData Event) error {
	if err := target.Init(); err != nil {
		return err
	}

	select {
	case <-target.cancelCh:
		return errors.New("redis target is closed")
	default:
	}

	// 序列化事件数据
	data, err := json.Marshal(eventData)
	if err != nil {
		return fmt.Errorf("failed to marshal event data: %w", err)
	}

	// 带重试的发送
	return target.sendWithRetry(data)
}

func (target *RedisTarget) Close() error {
	target.cancel()

	target.mu.Lock()
	defer target.mu.Unlock()

	return target.cleanup()
}

// GetArg 返回Redis目标的参数
func (target *RedisTarget) GetArg() (interface{}, error) {
	return target.args, nil
}

func (target *RedisTarget) connect() error {
	target.state.mu.Lock()
	defer target.state.mu.Unlock()

	// 清理旧连接
	_ = target.cleanup()

	// 配置 Redis 客户端选项
	options := &redis.Options{
		Addr:     target.args.Addr,
		Password: target.args.Password,
		DB:       target.args.DB,

		// 连接池优化
		PoolSize:     20,
		MinIdleConns: 5,
		MaxIdleConns: 10,

		// 超时设置
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolTimeout:  4 * time.Second,

		// 连接生命周期
		ConnMaxLifetime: time.Hour,
		ConnMaxIdleTime: 5 * time.Minute,
	}

	target.state.client = redis.NewClient(options)

	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := target.state.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("failed to connect to redis: %w", err)
	}

	return nil
}

func (target *RedisTarget) sendWithRetry(data []byte) error {
	var lastErr error

	for i := 0; i < target.maxRetries; i++ {
		select {
		case <-target.cancelCh:
			return errors.New("target closed during send")
		default:
		}

		if err := target.trySend(data); err != nil {
			lastErr = err

			// 如果是连接错误，尝试重连
			if isRedisConnectionError(err) {
				if reconnectErr := target.reconnect(); reconnectErr != nil {
					lastErr = fmt.Errorf("send failed and reconnect failed: %w (original: %v)", reconnectErr, err)
				}
				// 等待后重试
				time.Sleep(target.retryDelay * time.Duration(i+1))
				continue
			}
			return err
		}
		return nil
	}

	return fmt.Errorf("failed to send message after %d attempts: %w", target.maxRetries, lastErr)
}

func (target *RedisTarget) trySend(data []byte) error {
	target.state.mu.RLock()
	defer target.state.mu.RUnlock()

	if target.state.client == nil {
		return errors.New("redis client not available")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 根据存储类型选择不同的命令
	switch target.args.Type {
	case StorageTypeList:
		return target.sendToList(ctx, data)
	case StorageTypeStream:
		return target.sendToStream(ctx, data)
	case StorageTypePubSub:
		return target.sendToPubSub(ctx, data)
	case StorageTypeSet:
		return target.sendToSet(ctx, data)
	default: // 默认使用列表
		return target.sendToList(ctx, data)
	}
}

// 发送到列表
func (target *RedisTarget) sendToList(ctx context.Context, data []byte) error {
	// 使用管道提高性能
	pipe := target.state.client.Pipeline()

	// 添加到列表头部
	pipe.LPush(ctx, target.args.Key, data)

	// 如果设置了最大长度，进行修剪
	if target.args.MaxLength > 0 {
		if target.args.TrimApprox {
			// 使用近似修剪，性能更好
			pipe.LTrim(ctx, target.args.Key, 0, target.args.MaxLength-1)
		} else {
			// 精确修剪
			pipe.LTrim(ctx, target.args.Key, 0, target.args.MaxLength-1)
		}
	}

	// 执行管道命令
	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to execute pipeline commands: %w", err)
	}

	return nil
}

// 发送到流（Redis 5.0+）
func (target *RedisTarget) sendToStream(ctx context.Context, data []byte) error {
	// 使用流存储，支持更丰富的数据结构
	args := &redis.XAddArgs{
		Stream: target.args.Key,
		Values: map[string]interface{}{
			"data":      data,
			"timestamp": time.Now().UnixMilli(),
		},
	}

	// 如果设置了最大长度，添加限制
	if target.args.StreamMaxLen > 0 {
		args.MaxLen = target.args.StreamMaxLen
		args.Approx = target.args.TrimApprox
	}

	err := target.state.client.XAdd(ctx, args).Err()
	if err != nil {
		return fmt.Errorf("failed to add to stream: %w", err)
	}

	return nil
}

// 发送到发布订阅
func (target *RedisTarget) sendToPubSub(ctx context.Context, data []byte) error {
	err := target.state.client.Publish(ctx, target.args.Key, data).Err()
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}
	return nil
}

// 发送到集合
func (target *RedisTarget) sendToSet(ctx context.Context, data []byte) error {
	// 使用当前时间戳作为成员，确保唯一性
	member := &redis.Z{
		Score:  float64(time.Now().UnixNano()),
		Member: data,
	}

	err := target.state.client.ZAdd(ctx, target.args.Key, *member).Err()
	if err != nil {
		return fmt.Errorf("failed to add to sorted set: %w", err)
	}

	// 如果设置了过期时间
	if target.args.SetExpire > 0 {
		target.state.client.Expire(ctx, target.args.Key, target.args.SetExpire)
	}

	return nil
}

// 判断是否为Redis连接错误
func isRedisConnectionError(err error) bool {
	if err == nil {
		return false
	}

	errorStr := err.Error()
	connectionErrors := []string{
		"connection refused",
		"connection reset",
		"broken pipe",
		"dial tcp",
		"timeout",
		"io timeout",
		"EOF",
		"use of closed network connection",
	}

	for _, connErr := range connectionErrors {
		if strings.Contains(strings.ToLower(errorStr), connErr) {
			return true
		}
	}

	// 检查Redis特定错误
	if errors.Is(err, redis.ErrClosed) {
		return true
	}

	return false
}

func (target *RedisTarget) cleanup() error {
	if target.state == nil {
		return nil
	}

	target.state.mu.Lock()
	defer target.state.mu.Unlock()

	if target.state.client != nil {
		if err := target.state.client.Close(); err != nil && !errors.Is(err, redis.ErrClosed) {
			return fmt.Errorf("redis client close: %w", err)
		}
		target.state.client = nil
	}

	return nil
}

// 重连方法
func (target *RedisTarget) reconnect() error {
	target.mu.Lock()
	defer target.mu.Unlock()

	return target.connect()
}
