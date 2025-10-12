package event

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/mageg-x/dedups3/internal/event/target"
	"github.com/mageg-x/dedups3/plugs/kv"
	"path/filepath"
	"sync"
	"time"

	xconf "github.com/mageg-x/dedups3/internal/config"
	"github.com/mageg-x/dedups3/internal/event"
	"github.com/mageg-x/dedups3/internal/logger"
	"github.com/mageg-x/dedups3/internal/queue"
)

const (
	maxBytesPerFile = 256 << 20 // 256MB // 64MB
	minMsgSize      = 8
	maxMsgSize      = 1 << 20 // 1MB
	syncEvery       = 100     // 每条都 fsync（性能低但安全）
	syncTimeout     = 1 * time.Second
)

type EventService struct {
	kvstore   kv.KVStore
	queue     queue.Queue
	targetMap map[string]target.EventTarget
}

var (
	instance *EventService
	mu       = sync.Mutex{}
)

func GetEventService() *EventService {
	mu.Lock()
	defer mu.Unlock()
	if instance != nil && instance.queue != nil {
		return instance
	}
	cfg := xconf.Get()
	dir := filepath.Join(cfg.Node.LocalDir, "queue")
	dq := queue.NewDiskQueue("event-queue", dir, maxBytesPerFile, minMsgSize, maxMsgSize, syncEvery, syncTimeout)
	if dq == nil {
		logger.GetLogger("dedups3").Errorf("failed to create event queue")
		return nil
	}

	store, err := kv.GetKvStore()
	if err != nil || store == nil {
		logger.GetLogger("dedups3").Errorf("failed to get kv store: %v", err)
		return nil
	}

	instance = &EventService{
		queue:     dq,
		kvstore:   store,
		targetMap: make(map[string]target.EventTarget),
	}

	instance.doSyncEvent(context.Background())
	return instance
}

func (e *EventService) doSyncEvent(ctx context.Context) {
	go func() {
		// 从队列中读取消息
		readChan := e.queue.ReadChan()

		for {
			select {
			case <-ctx.Done(): // 可以响应取消信号
				logger.GetLogger("dedups3").Info("sync block stopping due to context cancellation")
				return
			case msg := <-readChan:
				record := target.Record{}
				err := json.Unmarshal(msg, &record)
				if err != nil {
					logger.GetLogger("dedups3").Errorf("failed to unmarshal event: %v", err)
					continue
				}
				//_event := target.Event{
				//	Records: []target.Record{record},
				//}

			default:
				// 正常的处理逻辑
			}
		}
	}()
}

func (e *EventService) AddTarget(targetType string, args interface{}) (target.EventTarget, error) {
	var ID string
	switch targetType {
	case target.EVENT_TARGET_TYPE_MYSQL:
		_args := args.(target.MySQLArgs)
		_target, err := target.NewMySQLTarget(context.Background(), _args)
		if err != nil {
			logger.GetLogger("dedups3").Errorf("failed to create MySQL target: %v", err)
			return nil, fmt.Errorf("failed to create MySQL target: %w", err)
		}
		ID = _args.ID
		e.targetMap[_args.ID] = _target
	case target.EVENT_TARGET_TYPE_RABITMQ:
		_args := args.(target.RabbitMQArgs)
		_target, err := target.NewRabbitMQTarget(context.Background(), _args)
		if err != nil {
			logger.GetLogger("dedups3").Errorf("failed to create RabbitMQ target: %v", err)
			return nil, fmt.Errorf("failed to create RabbitMQ target: %w", err)
		}
		ID = _args.ID
		e.targetMap[_args.ID] = _target
	case target.EVENT_TARGET_TYPE_REDIS:
		_args := args.(target.RedisArgs)
		_target, err := target.NewRedisTarget(context.Background(), _args)
		if err != nil {
			logger.GetLogger("dedups3").Errorf("failed to create Redis target: %v", err)
			return nil, fmt.Errorf("failed to create Redis target: %w", err)
		}
		ID = _args.ID
		e.targetMap[_args.ID] = _target
	case target.EVENT_TARGET_TYPE_ROCKETMQ:
		_args := args.(target.RocketMQArgs)
		_target, err := target.NewRocketMQTarget(context.Background(), _args)
		if err != nil {
			logger.GetLogger("dedups3").Errorf("failed to create RocketMQ target: %v", err)
			return nil, fmt.Errorf("failed to create RocketMQ target: %w", err)
		}
		ID = _args.ID
		e.targetMap[_args.ID] = _target
	case target.EVENT_TARGET_TYPE_WEBHOOK:
		_args := args.(target.WebhookArgs)
		_target, err := target.NewWebhookTarget(context.Background(), _args)
		if err != nil {
			logger.GetLogger("dedups3").Errorf("failed to create Webhook target: %v", err)
			return nil, fmt.Errorf("failed to create Webhook target: %w", err)
		}
		ID = _args.ID
		e.targetMap[_args.ID] = _target
	default:
		logger.GetLogger("dedups3").Errorf("unknown target type: %v", targetType)
		return nil, fmt.Errorf("unknown target type: %v", targetType)
	}

	t := e.targetMap[ID]
	// 保存target arg到kv存储
	key := target.EventTargetPrefix + t.Owner() + ":" + t.ID()
	if err := e.kvstore.Set(key, args); err != nil {
		return nil, fmt.Errorf("failed to save target to kv store: %w", err)
	}
	return e.targetMap[ID], nil
}

func (e *EventService) GetTargetByArn(arn string) (target.EventTarget, error) {
	// 遍历targetMap查找匹配的arn
	for _, t := range e.targetMap {
		if t != nil && t.Arn() == arn {
			return t, nil
		}
	}

	// 从kv store中查找
	txn, err := e.kvstore.BeginTxn(context.Background(), &kv.TxnOpt{IsReadOnly: true})
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to begin transaction: %v", err)
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer txn.Rollback()

	keys, _, err := txn.Scan(target.EventTargetPrefix, "", 1000)
	if err != nil {
		return nil, fmt.Errorf("failed to scan targets: %w", err)
	}

	for _, key := range keys {
		// 首先尝试获取目标头部信息来匹配ARN
		data, exists, err := txn.GetRaw(key)
		if err != nil {
			logger.GetLogger("dedups3").Errorf("failed to get target head: %v", err)
			continue
		}
		if !exists {
			continue
		}
		var header target.TargetArgHead
		if err := json.Unmarshal(data, &header); err != nil {
			logger.GetLogger("dedups3").Errorf("failed to unmarshal target head: %v", err)
			continue
		}
		if header.Arn != arn {
			continue
		}
		switch header.Type {
		case target.EVENT_TARGET_TYPE_MYSQL:
			var args target.MySQLArgs
			if err := json.Unmarshal(data, &args); err != nil {
				logger.GetLogger("dedups3").Errorf("failed to unmarshal target head: %v", err)
				continue
			}
			if _target, err := e.AddTarget(args.Type, args); err != nil {
				logger.GetLogger("dedups3").Errorf("failed to add target: %v", err)
				continue
			} else {
				return _target, nil
			}
		case target.EVENT_TARGET_TYPE_RABITMQ:
			var args target.RabbitMQArgs
			if err := json.Unmarshal(data, &args); err != nil {
				logger.GetLogger("dedups3").Errorf("failed to unmarshal target head: %v", err)
				continue
			}
			if _target, err := e.AddTarget(args.Type, args); err != nil {
				logger.GetLogger("dedups3").Errorf("failed to add target: %v", err)
				continue
			} else {
				return _target, nil
			}
		case target.EVENT_TARGET_TYPE_REDIS:
			var args target.RedisArgs
			if err := json.Unmarshal(data, &args); err != nil {
				logger.GetLogger("dedups3").Errorf("failed to unmarshal target head: %v", err)
				continue
			}
			if _target, err := e.AddTarget(args.Type, args); err != nil {
				logger.GetLogger("dedups3").Errorf("failed to add target: %v", err)
				continue
			} else {
				return _target, nil
			}
		case target.EVENT_TARGET_TYPE_ROCKETMQ:
			var args target.RocketMQArgs
			if err := json.Unmarshal(data, &args); err != nil {
				logger.GetLogger("dedups3").Errorf("failed to unmarshal target head: %v", err)
				continue
			}
			if _target, err := e.AddTarget(args.Type, args); err != nil {
				logger.GetLogger("dedups3").Errorf("failed to add target: %v", err)
				continue
			} else {
				return _target, nil
			}
		case target.EVENT_TARGET_TYPE_WEBHOOK:
			var args target.WebhookArgs
			if err := json.Unmarshal(data, &args); err != nil {
				logger.GetLogger("dedups3").Errorf("failed to unmarshal target head: %v", err)
				continue
			}
			if _target, err := e.AddTarget(args.Type, args); err != nil {
				logger.GetLogger("dedups3").Errorf("failed to add target: %v", err)
				continue
			} else {
				return _target, nil
			}
		}
	}
	return nil, fmt.Errorf("target not found with arn: %s", arn)
}

func (e *EventService) GetTargetById(id string) (target.EventTarget, error) {
	// 遍历targetMap查找匹配的id
	for _, t := range e.targetMap {
		if t != nil && t.ID() == id {
			return t, nil
		}
	}

	// 从kv store中查找
	txn, err := e.kvstore.BeginTxn(context.Background(), &kv.TxnOpt{IsReadOnly: true})
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to begin transaction: %v", err)
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer txn.Rollback()

	keys, _, err := txn.Scan(target.EventTargetPrefix, "", 1000)
	if err != nil {
		return nil, fmt.Errorf("failed to scan targets: %w", err)
	}

	for _, key := range keys {
		// 首先尝试获取目标头部信息来匹配ID
		data, exists, err := txn.GetRaw(key)
		if err != nil {
			logger.GetLogger("dedups3").Errorf("failed to get target head: %v", err)
			continue
		}
		if !exists {
			continue
		}
		var header target.TargetArgHead
		if err := json.Unmarshal(data, &header); err != nil {
			logger.GetLogger("dedups3").Errorf("failed to unmarshal target head: %v", err)
			continue
		}
		if header.ID != id {
			continue
		}

		// 根据类型创建对应目标实例
		switch header.Type {
		case target.EVENT_TARGET_TYPE_MYSQL:
			var args target.MySQLArgs
			if err := json.Unmarshal(data, &args); err != nil {
				logger.GetLogger("dedups3").Errorf("failed to unmarshal target head: %v", err)
				continue
			}
			if _target, err := e.AddTarget(args.Type, args); err != nil {
				logger.GetLogger("dedups3").Errorf("failed to add target: %v", err)
				continue
			} else {
				return _target, nil
			}
		case target.EVENT_TARGET_TYPE_RABITMQ:
			var args target.RabbitMQArgs
			if err := json.Unmarshal(data, &args); err != nil {
				logger.GetLogger("dedups3").Errorf("failed to unmarshal target head: %v", err)
				continue
			}
			if _target, err := e.AddTarget(args.Type, args); err != nil {
				logger.GetLogger("dedups3").Errorf("failed to add target: %v", err)
				continue
			} else {
				return _target, nil
			}
		case target.EVENT_TARGET_TYPE_REDIS:
			var args target.RedisArgs
			if err := json.Unmarshal(data, &args); err != nil {
				logger.GetLogger("dedups3").Errorf("failed to unmarshal target head: %v", err)
				continue
			}
			if _target, err := e.AddTarget(args.Type, args); err != nil {
				logger.GetLogger("dedups3").Errorf("failed to add target: %v", err)
				continue
			} else {
				return _target, nil
			}
		case target.EVENT_TARGET_TYPE_ROCKETMQ:
			var args target.RocketMQArgs
			if err := json.Unmarshal(data, &args); err != nil {
				logger.GetLogger("dedups3").Errorf("failed to unmarshal target head: %v", err)
				continue
			}
			if _target, err := e.AddTarget(args.Type, args); err != nil {
				logger.GetLogger("dedups3").Errorf("failed to add target: %v", err)
				continue
			} else {
				return _target, nil
			}
		case target.EVENT_TARGET_TYPE_WEBHOOK:
			var args target.WebhookArgs
			if err := json.Unmarshal(data, &args); err != nil {
				logger.GetLogger("dedups3").Errorf("failed to unmarshal target head: %v", err)
				continue
			}
			if _target, err := e.AddTarget(args.Type, args); err != nil {
				logger.GetLogger("dedups3").Errorf("failed to add target: %v", err)
				continue
			} else {
				return _target, nil
			}
		}
	}

	return nil, fmt.Errorf("target not found with id: %s", id)
}

func (e *EventService) InitTargets() error {
	// 从kv store中读取target配置
	txn, err := e.kvstore.BeginTxn(context.Background(), &kv.TxnOpt{IsReadOnly: true})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer txn.Rollback()

	// 遍历所有以target.EventTargetPrefix为前缀的键值对
	keys, _, err := txn.Scan(target.EventTargetPrefix, "", 1000)
	if err != nil {
		return fmt.Errorf("failed to scan targets: %w", err)
	}

	// 用于存储未成功创建的目标键，稍后重新尝试
	var failedKeys []string

	for _, key := range keys {
		// 首先获取存储的参数
		var argsMap map[string]interface{}
		exists, err := txn.Get(key, &argsMap)
		if err != nil {
			logger.GetLogger("dedups3").Errorf("failed to get target from kv store: %v", err)
			continue
		}
		if !exists {
			continue
		}

		// 尝试将map转换为JSON，然后再尝试不同类型的结构体
		jsonData, err := json.Marshal(argsMap)
		if err != nil {
			logger.GetLogger("dedups3").Errorf("failed to marshal argsMap to JSON: %v", err)
			continue
		}

		// 尝试不同类型的目标结构体
		success := false

		// 尝试MySQL目标
		var mysqlArgs target.MySQLArgs
		if err := json.Unmarshal(jsonData, &mysqlArgs); err == nil && mysqlArgs.DSN != "" {
			mysqlTarget, err := target.NewMySQLTarget(context.Background(), mysqlArgs)
			if err == nil {
				e.targetMap[mysqlArgs.ID] = mysqlTarget
				success = true
				logger.GetLogger("dedups3").Infof("initialized MySQL target: %s", mysqlArgs.ID)
			}
		}

		// 尝试RabbitMQ目标
		if !success {
			var rabbitMQArgs target.RabbitMQArgs
			if err := json.Unmarshal(jsonData, &rabbitMQArgs); err == nil && rabbitMQArgs.URL != "" {
				rabbitMQTarget, err := target.NewRabbitMQTarget(context.Background(), rabbitMQArgs)
				if err == nil {
					e.targetMap[rabbitMQArgs.ID] = rabbitMQTarget
					success = true
					logger.GetLogger("dedups3").Infof("initialized RabbitMQ target: %s", rabbitMQArgs.ID)
				}
			}
		}

		// 尝试Redis目标
		if !success {
			var redisArgs target.RedisArgs
			if err := json.Unmarshal(jsonData, &redisArgs); err == nil && redisArgs.Addr != "" {
				redisTarget, err := target.NewRedisTarget(context.Background(), redisArgs)
				if err == nil {
					e.targetMap[redisArgs.ID] = redisTarget
					success = true
					logger.GetLogger("dedups3").Infof("initialized Redis target: %s", redisArgs.ID)
				}
			}
		}

		// 尝试RocketMQ目标
		if !success {
			var rocketMQArgs target.RocketMQArgs
			if err := json.Unmarshal(jsonData, &rocketMQArgs); err == nil && rocketMQArgs.NameServerAddr != "" {
				rocketMQTarget, err := target.NewRocketMQTarget(context.Background(), rocketMQArgs)
				if err == nil {
					e.targetMap[rocketMQArgs.ID] = rocketMQTarget
					success = true
					logger.GetLogger("dedups3").Infof("initialized RocketMQ target: %s", rocketMQArgs.ID)
				}
			}
		}

		// 尝试Webhook目标
		if !success {
			var webhookArgs target.WebhookArgs
			if err := json.Unmarshal(jsonData, &webhookArgs); err == nil && webhookArgs.Endpoint.String() != "" {
				webhookTarget, err := target.NewWebhookTarget(context.Background(), webhookArgs)
				if err == nil {
					e.targetMap[webhookArgs.ID] = webhookTarget
					success = true
					logger.GetLogger("dedups3").Infof("initialized Webhook target: %s", webhookArgs.ID)
				}
			}
		}

		if !success {
			logger.GetLogger("dedups3").Errorf("failed to initialize target from key: %s", key)
			failedKeys = append(failedKeys, key)
		}
	}

	// 如果有未成功初始化的目标，记录日志
	if len(failedKeys) > 0 {
		logger.GetLogger("dedups3").Warningf("failed to initialize %d targets", len(failedKeys))
	}

	logger.GetLogger("dedups3").Infof("initialized %d event targets", len(e.targetMap))
	return nil
}

func (e *EventService) SendEvent(args event.EventArgs) error {
	recorder := args.ToEvent()
	data, err := json.Marshal(&recorder)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to marshal event to json: %v", err)
		return fmt.Errorf("failed to marshal event to json: %w", err)
	}
	err = e.queue.Put(data)
	if err != nil {
		logger.GetLogger("dedups3").Errorf("failed to put event to disk: %v", err)
		return fmt.Errorf("failed to put event to disk: %w", err)
	}
	return nil
}
