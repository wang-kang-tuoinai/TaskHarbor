// Package taskq 提供异步任务执行。
package taskq

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/wang-kang-tuoinai/TaskHarbor/internal/redisstore"
)

const DefaultQueue = "default"

type serverStatus uint8

const (
	serverCreated serverStatus = iota
	serverStarting
	serverRunning
	serverStopping
	serverStopped
)

// Server 是任务平台对外暴露的执行服务接口。内部实现类型未导出，
// 外部只能通过该接口使用，无法直接修改 Server 的内部状态。
type Server interface {
	// HandleFunc 在启动前注册任务类型。实例须注册其消费队列可能领取的全部类型。
	// 重复注册会返回错误，并保留原有的 Handler。
	HandleFunc(taskType string, fn HandlerFunc) error
	// Start 封存注册，等待取消订阅确认后启动领取、续租、回收和通知消费。对象不能重复启动。
	Start() error
	// Shutdown 停止新领取并等待本实例的执行、上报及后台循环实际结束。
	// context 到期会请求强制取消并返回其错误；仍存活的工作在返回后继续收尾。
	// 不关闭调用方的 Redis 客户端，nil 不表示所有任务都成功。
	Shutdown(ctx context.Context) error
	// Errors 供单个调用方接收首个导致停止领取的后台致命错误。
	// channel 不关闭来表达退出，收到通知不表示已有任务或整个服务已退出。
	Errors() <-chan error
}

// server 是 Server 的内部实现，必须通过 NewServer 构造，且不得被复制。
type server struct {
	redisClient     *redis.Client
	claimClient     *redis.Client // 共享调用方池，使用独立领取预算，不得 Close。
	renewClient     *redis.Client // WithTimeout 包装共享调用方连接池，不得 Close。
	reportClient    *redis.Client // 同样共享调用方池，独立使用上报的 I/O 预算，不得 Close。
	reclaimClient   *redis.Client // 共享调用方池，使用独立回收预算，不得 Close。
	slots           chan struct{}
	queues          []string      // 构造完成后不可变，所有队列共用 slots。
	leaseDuration   time.Duration // Claim/Renew 的有效期从 Redis 当前时间起算。
	renewInterval   time.Duration
	reclaimInterval time.Duration

	mu           sync.Mutex // 保护生命周期状态、ownerID 与 handler 注册。
	ownerID      string
	status       serverStatus
	startDone    chan struct{}      // 只由实际调用 Start 的启动流程关闭。
	cancelStart  context.CancelFunc // 由持 mu 的 Start 保存，Shutdown 在 starting 阶段调用。
	subscription *redis.PubSub      // 订阅确认后移交给消费协程，退出时在 mu 下清除。
	shutdownDone chan struct{}
	handlers     map[string]HandlerFunc

	executionsMu     sync.RWMutex
	executions       map[executionKey]*execution
	executionWG      sync.WaitGroup // 分别计入监督清理与 Handler 实际退出，不代表执行权。
	launchMu         sync.Mutex     // 使最终 Handler 启动批准与关闭 abort 具有明确先后。
	abort            chan struct{}  // 强制阶段广播；关闭由 launchMu 串行化。
	reportCtx        context.Context
	cancelAllReports context.CancelFunc

	// 构造时绑定真实上报；监督测试可在启动执行前替换以观察唯一结果分支。
	onComplete func(context.Context, *execution, executionKey, []byte)
	onFailure  func(context.Context, *execution, executionKey, error)

	claimCtx        context.Context
	cancelClaim     context.CancelFunc // 停止新领取，独立于 Renew 和已有 Handler。
	claimDone       chan struct{}
	errors          chan error
	fatalOnce       sync.Once
	renewCtx        context.Context
	cancelRenew     context.CancelFunc
	renewDone       chan struct{}
	reclaimCtx      context.Context
	cancelReclaim   context.CancelFunc
	reclaimDone     chan struct{}
	newAttemptToken func() (string, error) // 独立的随机性接缝；测试在 Start 前替换。

	subscriptionCtx   context.Context
	closeSubscription context.CancelFunc
	subscriptionDone  chan struct{}
}

type serverConfig struct {
	concurrency     int
	queues          []string
	leaseDuration   time.Duration
	renewInterval   time.Duration
	reclaimInterval time.Duration
}

func defaultServerConfig() serverConfig {
	return serverConfig{
		concurrency:     10,
		queues:          []string{DefaultQueue},
		leaseDuration:   30 * time.Second,
		renewInterval:   6 * time.Second,
		reclaimInterval: 10 * time.Second,
	}
}

// Option 在构造前配置 Server。
type Option func(*serverConfig) error

// WithQueues 设置实例按顺序轮转消费的队列。各队列共享执行槽。
// 省略该选项时只消费 default，显式传入空列表会使构造失败。
func WithQueues(queues ...string) Option {
	return func(c *serverConfig) error {
		if len(queues) == 0 {
			return errors.New("at least one consumption queue is required")
		}
		seen := make(map[string]struct{}, len(queues))
		for _, queue := range queues {
			if err := redisstore.ValidateQueueName(queue); err != nil {
				return fmt.Errorf("invalid consumption queue %q: %w", queue, err)
			}
			if _, exists := seen[queue]; exists {
				return fmt.Errorf("duplicate consumption queue %q", queue)
			}
			seen[queue] = struct{}{}
		}
		c.queues = slices.Clone(queues)
		return nil
	}
}

// WithConcurrency 设置执行槽总数。
func WithConcurrency(n int) Option {
	return func(c *serverConfig) error {
		if n <= 0 {
			return fmt.Errorf("concurrency must be > 0, got %d", n)
		}
		c.concurrency = n
		return nil
	}
}

// WithLeaseDuration 设置每次 Claim/Renew 租约的有效期。
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

// WithReclaimInterval 设置回收扫描间隔。
func WithReclaimInterval(d time.Duration) Option {
	return func(c *serverConfig) error {
		if d < 2*time.Second {
			return fmt.Errorf("reclaim interval must be at least 2s, got %v", d)
		}
		c.reclaimInterval = d
		return nil
	}
}

// NewServer 只构造本地状态，不发起 Redis 请求或启动 goroutine。
// 调用方须以 MaxRetries=-1 创建 rdb，并负责关闭它；平台不修改共享配置。
// ContextTimeoutEnabled 必须为 true，使请求截止时间约束实际网络等待。
func NewServer(rdb *redis.Client, opts ...Option) (Server, error) {
	if rdb == nil {
		return nil, errors.New("rdb is required")
	}
	// go-redis 将构造参数 -1 规范化为有效值 0；Claim 不能透明重试结果未知的请求。
	if rdb.Options().MaxRetries != 0 {
		return nil, errors.New("Redis automatic retries must be disabled: construct client with MaxRetries=-1")
	}
	if !rdb.Options().ContextTimeoutEnabled {
		return nil, errors.New("Redis ContextTimeoutEnabled must be true for bounded background requests")
	}
	cfg := defaultServerConfig()
	for _, opt := range opts {
		if opt == nil {
			return nil, errors.New("server option must not be nil")
		}
		if err := opt(&cfg); err != nil {
			return nil, err
		}
	}
	if cfg.renewInterval > cfg.leaseDuration/3 {
		return nil, fmt.Errorf("renew interval must be <= lease duration / 3: renewInterval=%v, leaseDuration/3=%v", cfg.renewInterval, cfg.leaseDuration/3)
	}
	claimCtx, cancelClaim := context.WithCancel(context.Background())
	renewCtx, cancelRenew := context.WithCancel(context.Background())
	reclaimCtx, cancelReclaim := context.WithCancel(context.Background())
	reportCtx, cancelAllReports := context.WithCancel(context.Background())
	subscriptionCtx, closeSubscription := context.WithCancel(context.Background())
	s := &server{
		redisClient:       rdb,
		claimClient:       boundedRedisClient(rdb, claimRequestTimeout),
		renewClient:       boundedRedisClient(rdb, cfg.renewInterval),
		reportClient:      boundedRedisClient(rdb, reportRequestTimeout),
		reclaimClient:     boundedRedisClient(rdb, reclaimRequestTimeout),
		slots:             make(chan struct{}, cfg.concurrency),
		queues:            cfg.queues,
		leaseDuration:     cfg.leaseDuration,
		renewInterval:     cfg.renewInterval,
		reclaimInterval:   cfg.reclaimInterval,
		status:            serverCreated,
		startDone:         make(chan struct{}),
		shutdownDone:      make(chan struct{}),
		handlers:          make(map[string]HandlerFunc),
		executions:        make(map[executionKey]*execution),
		abort:             make(chan struct{}),
		reportCtx:         reportCtx,
		cancelAllReports:  cancelAllReports,
		claimCtx:          claimCtx,
		cancelClaim:       cancelClaim,
		claimDone:         make(chan struct{}),
		renewCtx:          renewCtx,
		cancelRenew:       cancelRenew,
		renewDone:         make(chan struct{}),
		reclaimCtx:        reclaimCtx,
		cancelReclaim:     cancelReclaim,
		reclaimDone:       make(chan struct{}),
		errors:            make(chan error, 1),
		newAttemptToken:   newAttemptToken,
		subscriptionCtx:   subscriptionCtx,
		closeSubscription: closeSubscription,
		subscriptionDone:  make(chan struct{}),
	}
	s.onComplete = s.complete
	s.onFailure = s.reportFailure
	return s, nil
}
func (s *server) HandleFunc(taskType string, fn HandlerFunc) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.status != serverCreated {
		return errors.New("cannot register handler after server startup begins")
	}
	if taskType == "" {
		return errors.New("task type must not be empty")
	}
	if fn == nil {
		return errors.New("handler must not be nil")
	}
	if _, ok := s.handlers[taskType]; ok {
		return fmt.Errorf("%s already registered", taskType)
	}
	s.handlers[taskType] = fn
	return nil
}

func (s *server) Errors() <-chan error { return s.errors }

func (s *server) reportFatal(err error) {
	// Once 仅限制通知次数，各调用方仍负责停止受影响的循环。
	s.cancelClaim()
	s.fatalOnce.Do(func() { s.errors <- err })
}
