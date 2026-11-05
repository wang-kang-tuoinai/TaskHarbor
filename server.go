// Package taskq 提供异步任务执行。
package taskq

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

const DefaultQueue = "default"
const MaxResultSize = 1000

// Task 是传递给 handler 的业务视图，执行身份单独存储。
type Task struct {
	ID       string
	Type     string
	Attempts int
	Payload  []byte
	result   []byte
}

// HandlerFunc 处理一个任务。所有结果写入必须在返回前完成。
// handler 返回后不得再修改 task，也不得并发调用 SetResult。
type HandlerFunc func(ctx context.Context, task *Task) error

// SetResult 将 v JSON 编码到独立的 result 缓冲区。Server 只在
// HandlerFunc 成功返回后读取结果。
func (t *Task) SetResult(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal task result: %w", err)
	}
	if len(b) > MaxResultSize {
		return fmt.Errorf("task result too large: %d bytes (max %d)", len(b), MaxResultSize)
	}
	t.result = append([]byte(nil), b...)
	return nil
}

type serverStatus uint8

const (
	serverCreated serverStatus = iota
	serverStarting
	serverRunning
	serverStopping
	serverStopped
)

// executionKey 独立于可变的 Task 字段来标识一次执行尝试。
type executionKey struct {
	taskID       string
	attemptToken string
}

// execution 保存续租资格，不证明租约有效或 Renew 正在运行；除原子禁止标记外字段不可变。
// 请在表锁之外调用 cancel。
type execution struct {
	executionTimeout time.Duration
	cancel           context.CancelCauseFunc
	reportForbidden  atomic.Bool // 超时为首个取消原因时，后来已知失权仍须禁止新上报。
}

// Server 是任务平台对外暴露的执行服务接口。内部实现类型未导出，
// 外部只能通过该接口使用，无法直接修改 Server 的内部状态。
type Server interface {
	// HandleFunc 在启动开始前注册任务类型。重复注册会返回错误，
	// 并保留原有的 handler。
	HandleFunc(taskType string, fn HandlerFunc) error
	// Start 封存注册并启动独立领取、续租循环，非阻塞返回；对象不能重复启动。
	Start() error
	// Errors 供单个调用方接收 Claim/Renew 首个导致停止领取的后台致命错误。
	// channel 不关闭来表达退出，收到通知不表示已有任务或整个服务已退出。
	Errors() <-chan error
}

// server 是 Server 的内部实现，必须通过 NewServer 构造，且不得被复制。
// 已接通最小 Start 与单轮本地执行监督，完整退出生命周期尚未实现。
type server struct {
	redisClient     *redis.Client
	renewClient     *redis.Client // WithTimeout 包装共享调用方连接池，不得 Close。
	slots           chan struct{}
	leaseDuration   time.Duration // Claim/Renew 的有效期从 Redis 当前时间起算。
	renewInterval   time.Duration
	reclaimInterval time.Duration

	mu       sync.Mutex // 保护生命周期状态、ownerID 与 handler 注册。
	ownerID  string
	status   serverStatus
	handlers map[string]HandlerFunc

	executionsMu sync.RWMutex
	executions   map[executionKey]*execution
	abort        chan struct{} // 由未来唯一的生命周期管理方关闭；当前没有公开退出入口。

	// 构造时绑定内部上报占位；测试可在启动执行前替换，观察分支而非伪造存储结果。
	onComplete func(executionKey, []byte)
	onFailure  func(executionKey, error)

	claimCtx        context.Context
	cancelClaim     context.CancelFunc // 停止新领取，独立于 Renew 和已有 Handler。
	claimDone       chan struct{}
	errors          chan error
	fatalOnce       sync.Once
	renewCtx        context.Context
	cancelRenew     context.CancelFunc
	renewDone       chan struct{}
	newAttemptToken func() (string, error) // 独立的随机性接缝；测试在 Start 前替换。
}

// beginStart 在任何订阅或 Claim 之前封存注册。后续的启动代码必须首先调用
// 它，并在启动失败时清理回到 serverStopped 状态。
// Server 对象不可重启。
func (s *server) beginStart() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.status != serverCreated {
		return errors.New("server has already started")
	}
	if len(s.handlers) == 0 {
		return errors.New("at least one handler is required")
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return fmt.Errorf("generate owner ID: %w", err)
	}
	s.ownerID = fmt.Sprintf("%x", id)
	s.status = serverStarting
	return nil
}

func (s *server) addExecution(key executionKey, e *execution) error {
	if key.taskID == "" || key.attemptToken == "" || e == nil || e.cancel == nil || e.executionTimeout <= 0 {
		return errors.New("invalid execution")
	}
	s.executionsMu.Lock()
	defer s.executionsMu.Unlock()
	if _, exists := s.executions[key]; exists {
		return errors.New("execution already exists")
	}
	s.executions[key] = e
	return nil
}

func (s *server) getExecution(key executionKey) (*execution, bool) {
	s.executionsMu.RLock()
	defer s.executionsMu.RUnlock()
	e, ok := s.executions[key]
	return e, ok
}

func (s *server) deleteExecution(key executionKey) {
	s.executionsMu.Lock()
	defer s.executionsMu.Unlock()
	delete(s.executions, key)
}

// snapshotExecutions 复制表条目，共享不可变的 execution 记录。
// 请在本方法返回后再发起 Redis 请求和取消操作。
func (s *server) snapshotExecutions() map[executionKey]*execution {
	s.executionsMu.RLock()
	defer s.executionsMu.RUnlock()
	snapshot := make(map[executionKey]*execution, len(s.executions))
	for key, e := range s.executions {
		snapshot[key] = e
	}
	return snapshot
}

type serverConfig struct {
	concurrency     int
	leaseDuration   time.Duration
	renewInterval   time.Duration
	reclaimInterval time.Duration
}

func defaultServerConfig() serverConfig {
	return serverConfig{
		concurrency:     10,
		leaseDuration:   30 * time.Second,
		renewInterval:   6 * time.Second,
		reclaimInterval: 10 * time.Second,
	}
}

// Option 在构造前配置 Server。
type Option func(*serverConfig) error

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
// ContextTimeoutEnabled 必须为 true，使 Renew 的请求截止时间约束实际网络等待。
func NewServer(rdb *redis.Client, opts ...Option) (Server, error) {
	if rdb == nil {
		return nil, errors.New("rdb is required")
	}
	// go-redis 将构造参数 -1 规范化为有效值 0；Claim 不能透明重试结果未知的请求。
	if rdb.Options().MaxRetries != 0 {
		return nil, errors.New("Redis automatic retries must be disabled: construct client with MaxRetries=-1")
	}
	if !rdb.Options().ContextTimeoutEnabled {
		return nil, errors.New("Redis ContextTimeoutEnabled must be true for bounded Renew requests")
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
	// WithTimeout 只复制客户端配置、共享原池。取最短正 I/O 预算，既启用负数超时
	// 配置下的 socket deadline，也不会延长调用方已有的更短超时。
	ioTimeout := cfg.renewInterval
	for _, limit := range []time.Duration{rdb.Options().ReadTimeout, rdb.Options().WriteTimeout} {
		if limit > 0 && limit < ioTimeout {
			ioTimeout = limit
		}
	}
	s := &server{
		redisClient:     rdb,
		renewClient:     rdb.WithTimeout(ioTimeout),
		slots:           make(chan struct{}, cfg.concurrency),
		leaseDuration:   cfg.leaseDuration,
		renewInterval:   cfg.renewInterval,
		reclaimInterval: cfg.reclaimInterval,
		status:          serverCreated,
		handlers:        make(map[string]HandlerFunc),
		executions:      make(map[executionKey]*execution),
		abort:           make(chan struct{}),
		claimCtx:        claimCtx,
		cancelClaim:     cancelClaim,
		claimDone:       make(chan struct{}),
		renewCtx:        renewCtx,
		cancelRenew:     cancelRenew,
		renewDone:       make(chan struct{}),
		errors:          make(chan error, 1),
		newAttemptToken: newAttemptToken,
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
