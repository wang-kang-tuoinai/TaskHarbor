//go:build recovery

package recovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	taskq "github.com/wang-kang-tuoinai/TaskHarbor"
)

const workerEnvironment = "TASKHARBOR_DRILL_WORKER"

type workerConfig struct {
	Address string `json:"address"`
	Number  int    `json:"number"`
}

type workload struct {
	Value int  `json:"value"`
	Long  bool `json:"long"`
	Fail  bool `json:"fail"`
}

type observedConnection struct {
	net.Conn
	emit func(event)
}

func (c *observedConnection) Read(data []byte) (int, error) {
	n, err := c.Conn.Read(data)
	if err != nil {
		var timeout net.Error
		c.emit(event{Kind: "redis_network_error", Detail: err.Error(), Timeout: errors.As(err, &timeout) && timeout.Timeout()})
	}
	return n, err
}

// 同一个测试二进制作为子进程运行；所有平台交互仅使用公开入口。
func TestRecoveryWorker(t *testing.T) {
	encoded := os.Getenv(workerEnvironment)
	if encoded == "" {
		t.Skip("worker helper runs only in child processes")
	}
	var config workerConfig
	if err := json.Unmarshal([]byte(encoded), &config); err != nil {
		t.Fatal(err)
	}
	var output sync.Mutex
	emit := func(e event) {
		e.At, e.Worker, e.PID = time.Now().UTC(), config.Number, os.Getpid()
		output.Lock()
		defer output.Unlock()
		if err := json.NewEncoder(os.Stdout).Encode(e); err != nil {
			fmt.Fprintln(os.Stderr, "worker event output failed", err)
			os.Exit(2)
		}
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))
	rdb := redis.NewClient(&redis.Options{
		Addr: config.Address, MaxRetries: -1, ContextTimeoutEnabled: true, Protocol: 2, DisableIdentity: true,
		DialTimeout: time.Second, ReadTimeout: 2 * time.Second, WriteTimeout: 2 * time.Second,
		PoolTimeout: 2 * time.Second, PoolSize: 32,
		Dialer: func(ctx context.Context, network, address string) (net.Conn, error) {
			conn, err := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, network, address)
			if err != nil {
				emit(event{Kind: "redis_network_error", Detail: err.Error()})
				return nil, err
			}
			return &observedConnection{Conn: conn, emit: emit}, nil
		},
	})
	server, err := taskq.NewServer(rdb, taskq.WithConcurrency(10), taskq.WithLeaseDuration(30*time.Second),
		taskq.WithRenewInterval(6*time.Second), taskq.WithReclaimInterval(10*time.Second))
	if err != nil {
		emit(event{Kind: "fatal", Detail: err.Error()})
		t.Fatal(err)
	}
	if err := server.HandleFunc("drill", func(ctx context.Context, task *taskq.Task) error {
		var work workload
		if err := json.Unmarshal(task.Payload, &work); err != nil {
			return err
		}
		emit(event{Kind: "handler_start", TaskID: task.ID, Attempt: task.Attempts, Long: work.Long})
		duration := time.Second
		if work.Long {
			duration = 20 * time.Second
		}
		timer := time.NewTimer(duration)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			emit(event{Kind: "handler_canceled", TaskID: task.ID, Attempt: task.Attempts, Detail: ctx.Err().Error()})
			return ctx.Err()
		case <-timer.C:
		}
		if work.Fail && task.Attempts <= 2 {
			emit(event{Kind: "handler_return", TaskID: task.ID, Attempt: task.Attempts, Detail: "planned failure"})
			return errors.New("drill planned failure in attempt <=2")
		}
		if err := task.SetResult(struct {
			Square int `json:"square"`
		}{Square: work.Value * work.Value}); err != nil {
			return err
		}
		emit(event{Kind: "handler_return", TaskID: task.ID, Attempt: task.Attempts, Detail: "success"})
		return nil
	}); err != nil {
		emit(event{Kind: "fatal", Detail: err.Error()})
		t.Fatal(err)
	}
	if err := server.Start(); err != nil {
		emit(event{Kind: "fatal", Detail: err.Error()})
		t.Fatal(err)
	}
	emit(event{Kind: "worker_ready"})
	for err := range server.Errors() {
		// Errors 仅通知首个故障；保持进程存活以保留健康续租和上报机会。
		emit(event{Kind: "fatal", Detail: err.Error()})
	}
}
