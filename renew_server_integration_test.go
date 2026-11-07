//go:build integration

package taskq

import (
	"context"
	"errors"
	"io"
	"net"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/wang-kang-tuoinai/TaskHarbor/internal/redisstore"
)

type observeRenewHook struct {
	process func(redis.ProcessHook) redis.ProcessHook
}

func (h observeRenewHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h observeRenewHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h observeRenewHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return h.process(next)
}

func TestRenewRedisServerKeepsLongTaskManaged(t *testing.T) {
	rdb := startSubmitRedis(t)
	ctx := context.Background()
	c := integrationClient(t, rdb)
	mustSubmit(t, c, "long", "job", nil, WithSubmitExecutionTimeout(20*time.Second))
	s := integrationServer(t, rdb, WithConcurrency(1), WithLeaseDuration(6*time.Second), WithRenewInterval(2*time.Second))
	started := make(chan context.Context, 1)
	release := make(chan struct{})
	completed := make(chan struct{}, 1)
	renewed := make(chan struct{}, 8)
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	s.renewClient.AddHook(observeRenewHook{process: func(next redis.ProcessHook) redis.ProcessHook {
		return func(ctx context.Context, cmd redis.Cmder) error {
			err := next(ctx, cmd)
			if err == nil && isRenewCommand(cmd) {
				renewed <- struct{}{}
			}
			return err
		}
	}})
	if err := s.HandleFunc("job", func(ctx context.Context, _ *Task) error { started <- ctx; <-release; return nil }); err != nil {
		t.Fatal(err)
	}
	s.onComplete = func(ctx context.Context, record *execution, key executionKey, result []byte) {
		s.complete(ctx, record, key, result)
		completed <- struct{}{}
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	var handlerCtx context.Context
	select {
	case handlerCtx = <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("handler not started")
	}
	initial, err := rdb.ZScore(ctx, claimLease, "long").Result()
	if err != nil {
		t.Fatal(err)
	}
	before := submitHash(t, rdb, "long")
	for range 3 {
		select {
		case <-renewed:
		case <-time.After(4 * time.Second):
			t.Fatal("periodic renew not confirmed")
		}
	}
	now, err := rdb.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	if float64(now.UnixMilli()) < initial {
		timer := time.NewTimer(time.Duration(initial-float64(now.UnixMilli())+20) * time.Millisecond)
		<-timer.C
	}
	after, err := rdb.ZScore(ctx, claimLease, "long").Result()
	if err != nil || after <= initial || handlerCtx.Err() != nil {
		t.Fatalf("lease=%v initial=%v handler=%v err=%v", after, initial, handlerCtx.Err(), err)
	}
	if !reflect.DeepEqual(before, submitHash(t, rdb, "long")) {
		t.Fatal("renew changed task fields")
	}
	close(release)
	select {
	case <-completed:
	case <-time.After(3 * time.Second):
		t.Fatal("local completion not observed")
	}
	stopStartLoop(t, s)
	awaitStartExecutions(t, s)
	if fields := submitHash(t, rdb, "long"); fields["status"] != "completed" || fields["attempts"] != "1" || fields["finished_at"] == "" {
		t.Fatalf("long task did not complete in Redis: %q", fields)
	}
}

func TestRenewRedisServerLostReplyRevalidatesNextRound(t *testing.T) {
	admin := startSubmitRedis(t)
	ctx := context.Background()
	if _, err := redisstore.Renew(ctx, admin, redisstore.RenewParams{TaskID: "warm", AttemptToken: "warm", LeaseDurationMS: 6000}); err != nil {
		t.Fatal(err)
	}
	mustSubmit(t, integrationClient(t, admin), "task", "job", nil, WithSubmitExecutionTimeout(20*time.Second))
	var armed, dropped atomic.Bool
	rdb := redis.NewClient(&redis.Options{Addr: admin.Options().Addr, MaxRetries: -1, ContextTimeoutEnabled: true, PoolSize: 1, ReadTimeout: time.Second, WriteTimeout: time.Second, Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return &dropSubmitReplyConn{Conn: conn, armed: &armed, dropped: &dropped, expected: []byte(":1\r\n")}, nil
	}})
	t.Cleanup(func() { _ = rdb.Close() })
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	s := integrationServer(t, rdb, WithConcurrency(1), WithLeaseDuration(6*time.Second), WithRenewInterval(2*time.Second))
	started := make(chan context.Context, 1)
	lost := make(chan error, 1)
	nextRound := make(chan struct{})
	allow := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() {
		for _, gate := range []chan struct{}{allow, release} {
			select {
			case <-gate:
			default:
				close(gate)
			}
		}
	})
	var renewCalls, reports atomic.Int32
	s.renewClient.AddHook(observeRenewHook{process: func(next redis.ProcessHook) redis.ProcessHook {
		return func(ctx context.Context, cmd redis.Cmder) error {
			if !isRenewCommand(cmd) {
				return next(ctx, cmd)
			}
			call := renewCalls.Add(1)
			if call == 2 {
				close(nextRound)
				select {
				case <-allow:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			err := next(ctx, cmd)
			if call == 1 {
				lost <- err
			}
			return err
		}
	}})
	if err := s.HandleFunc("job", func(ctx context.Context, _ *Task) error { started <- ctx; <-release; return nil }); err != nil {
		t.Fatal(err)
	}
	s.onComplete = func(ctx context.Context, record *execution, _ executionKey, _ []byte) { reports.Add(1) }
	s.onFailure = func(ctx context.Context, record *execution, _ executionKey, _ error) { reports.Add(1) }
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	var handlerCtx context.Context
	select {
	case handlerCtx = <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("handler not started")
	}
	before, err := admin.ZScore(ctx, claimLease, "task").Result()
	if err != nil {
		t.Fatal(err)
	}
	armed.Store(true)
	select {
	case err := <-lost:
		if !(errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed)) {
			t.Fatalf("lost response error=%v", err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("renew response not dropped")
	}
	if !dropped.Load() || handlerCtx.Err() != nil || len(s.slots) != 1 || len(s.snapshotExecutions()) != 1 || len(s.Errors()) != 0 {
		t.Fatal("unknown renew canceled, released or removed task")
	}
	after, err := admin.ZScore(ctx, claimLease, "task").Result()
	if err != nil || after <= before {
		t.Fatal("renew had not actually applied before response loss")
	}
	select {
	case <-nextRound:
		t.Fatal("renew replayed immediately after unknown result")
	case <-time.After(time.Second):
	}
	select {
	case <-nextRound:
	case <-time.After(3 * time.Second):
		t.Fatal("next round did not revalidate")
	}
	// 受控身份变化验证下一轮必须重新鉴权；不代表真实 Cancel/Reclaim 已实现。
	if err := admin.HSet(ctx, "taskq:meta:task", "attempt_token", "new-token").Err(); err != nil {
		t.Fatal(err)
	}
	close(allow)
	select {
	case <-handlerCtx.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("old identity not canceled after renewal rejection")
	}
	if !errors.Is(context.Cause(handlerCtx), errExecutionLost) || len(s.slots) != 1 || len(s.Errors()) != 0 {
		t.Fatal("normal rejection scope or slot lifetime incorrect")
	}
	stopStartLoop(t, s)
	close(release)
	awaitStartExecutions(t, s)
	if reports.Load() != 0 || renewCalls.Load() != 2 {
		t.Fatalf("reports=%d requests=%d", reports.Load(), renewCalls.Load())
	}
}
