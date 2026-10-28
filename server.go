// server启动的初始化代码
package taskq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const DefaultQueue = "default" //第一版队列默认为default
const MaxResultSize = 1000

// 任务结构体
type Task struct {
	ID       string
	Name     string
	Attempts int    //尝试次数
	Payload  []byte //序列化之后的参数

	result []byte          //server读取，用户不可见
	ctx    context.Context //当前正在处理的任务上下文
}
type handler func(ctx context.Context, task *Task) error

func (t *Task) SetResult(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal task result: %w", err)
	}
	if len(b) > MaxResultSize {
		return fmt.Errorf("task result too large: %d bytes (max %d)", len(b), MaxResultSize)
	}
	t.result = b
	return nil
}


// Server 是任务平台的执行服务接口。
type Server interface {
	HandleFunc(funcName string, fn handler) error
}

// server 是 Server 的内部实现。
type server struct {
	Rdb *redis.Client
	Sem chan (struct{}) //执行槽

	// 租约
	LeaseDuration time.Duration // 每次新增租约时长
	RenewInterval time.Duration // 续租间隔

	// 回收
	ReclaimInterval time.Duration // 回收间隔

	Handler map[string]handler //任务注册表

	Tasks map[string]*Task //领取的任务表
}

// serverConfig 是内部使用的、未导出的配置结构体。
type serverConfig struct {
	concurrency int

	// 租约
	leaseDuration time.Duration // 每次新增租约时长
	renewInterval time.Duration // 续租间隔

	// 回收
	reclaimInterval time.Duration // 回收间隔
}

// defaultServerConfig 返回默认配置。
func defaultServerConfig() serverConfig {
	return serverConfig{
		concurrency:     10,
		leaseDuration:   30 * time.Second,
		renewInterval:   6 * time.Second,
		reclaimInterval: 10 * time.Second,
	}
}

// Option 用于配置 server。
type Option func(*serverConfig) error

// WithConcurrency 设置并发执行槽数量。
func WithConcurrency(n int) Option {
	return func(c *serverConfig) error {
		if n <= 0 {
			return fmt.Errorf("concurrency must be > 0, got %d", n)
		}
		c.concurrency = n
		return nil
	}
}

// WithLeaseDuration 设置每次租约时长。
func WithLeaseDuration(d time.Duration) Option {
	return func(c *serverConfig) error {
		if d < 2*time.Second {
			return fmt.Errorf("lease duration must be at least 2s, got %v", d)
		}
		c.leaseDuration = d
		return nil
	}
}

// WithRenewInterval 设置续租间隔。
func WithRenewInterval(d time.Duration) Option {
	return func(c *serverConfig) error {
		if d < 2*time.Second {
			return fmt.Errorf("renew interval must be at least 2s, got %v", d)
		}
		c.renewInterval = d
		return nil
	}
}

// WithReclaimInterval 设置回收间隔。
func WithReclaimInterval(d time.Duration) Option {
	return func(c *serverConfig) error {
		if d < 2*time.Second {
			return fmt.Errorf("reclaim interval must be at least 2s, got %v", d)
		}
		c.reclaimInterval = d
		return nil
	}
}

// NewServer 创建一个新的 Server，rdb 为必传参数。
func NewServer(rdb *redis.Client, opts ...Option) (Server, error) {
	if rdb == nil {
		return nil, errors.New("rdb is required")
	}

	cfg := defaultServerConfig()
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(&cfg); err != nil {
			return nil, err
		}
	}

	if cfg.renewInterval > cfg.leaseDuration/3 {
		return nil, fmt.Errorf("renew interval must be <= lease duration / 3: renewInterval=%v, leaseDuration/3=%v", cfg.renewInterval, cfg.leaseDuration/3)
	}

	return &server{
		Rdb:             rdb,
		Sem:             make(chan struct{}, cfg.concurrency),
		LeaseDuration:   cfg.leaseDuration,
		RenewInterval:   cfg.renewInterval,
		ReclaimInterval: cfg.reclaimInterval,
		Handler:         make(map[string]handler),
	}, nil
}

func (s *server) HandleFunc(funcName string, fn handler) error {
	_, ok := s.Handler[funcName]
	if ok {
		return fmt.Errorf("%s already registered", funcName)
	}
	s.Handler[funcName] = fn
	return nil
}
