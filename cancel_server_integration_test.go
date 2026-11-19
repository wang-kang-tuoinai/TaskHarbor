//go:build integration

package taskq

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/wang-kang-tuoinai/TaskHarbor/internal/redisstore"
)

func TestCancelServerRedisMatchesAttemptAndRetainsSlot(t *testing.T) {
	rdb := startSubmitRedis(t)
	ctx := context.Background()
	client := integrationClient(t, rdb)
	const id, fence, next = "task\uFFFD", "ordered-fence", "next-task"
	mustSubmit(t, client, id, "job", nil)
	mustSubmit(t, client, fence, "job", nil)
	s := integrationServer(t, rdb, WithConcurrency(2))

	type handlerEvent struct {
		id  string
		ctx context.Context
	}
	started := make(chan handlerEvent, 3)
	canceled := make(chan handlerEvent, 2)
	release := make(chan struct{})
	defer closeShutdownGate(release)
	if err := s.HandleFunc("job", func(ctx context.Context, task *Task) error {
		started <- handlerEvent{task.ID, ctx}
		if task.ID == next {
			return nil
		}
		select {
		case <-ctx.Done():
			canceled <- handlerEvent{task.ID, ctx}
		case <-release:
		}
		<-release
		return errors.New("handler exited after cancellation")
	}); err != nil {
		t.Fatal(err)
	}
	var canceledReports atomic.Int32
	s.reportClient.AddHook(observeRenewHook{process: func(next redis.ProcessHook) redis.ProcessHook {
		return func(ctx context.Context, cmd redis.Cmder) error {
			for _, arg := range cmd.Args() {
				if arg == "taskq:meta:"+id || arg == "taskq:meta:"+fence {
					canceledReports.Add(1)
					break
				}
			}
			return next(ctx, cmd)
		}
	}})
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	first := awaitReclaimSignal(t, started)
	second := awaitReclaimSignal(t, started)
	contexts := map[string]context.Context{first.id: first.ctx, second.id: second.ctx}
	if contexts[id] == nil || contexts[fence] == nil {
		t.Fatalf("wrong handlers: %q and %q", first.id, second.id)
	}
	channel := "cancel:" + s.ownerID
	token := submitHash(t, rdb, id)["attempt_token"]
	valid, err := json.Marshal(map[string]string{"task_id": id, "attempt_token": token})
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{
		"broken JSON", "{\"task_id\":\"ordered-fence\"}",
		"{\"task_id\":\"unknown\",\"attempt_token\":\"other\"}",
		"{\"task_id\":\"task\uFFFD\",\"attempt_token\":\"stale\"}",
		strings.Replace(string(valid), "\uFFFD", "\xff", 1),
	} {
		if n, err := rdb.Publish(ctx, channel, bad).Result(); err != nil || n != 1 {
			t.Fatalf("publish %q: subscribers=%d err=%v", bad, n, err)
		}
	}
	// 同一订阅按 Redis 发布次序消费。第二个 Handler 收到真实 Cancel 后，
	// 可以确定更早的坏消息与旧 token 已经过消费，且未影响第一个执行。
	requireCancel(t, client, fence, CancelCanceled, "canceled")
	event := awaitReclaimSignal(t, canceled)
	if event.id != fence || !errors.Is(context.Cause(event.ctx), errExecutionCanceled) || contexts[id].Err() != nil {
		t.Fatalf("unexpected local cancellation: id=%q cause=%v other=%v", event.id, context.Cause(event.ctx), contexts[id].Err())
	}

	mustSubmit(t, client, next, "job", nil)
	requireCancel(t, client, id, CancelCanceled, "canceled")
	event = awaitReclaimSignal(t, canceled)
	if event.id != id || !errors.Is(context.Cause(event.ctx), errExecutionCanceled) || len(s.slots) != 2 {
		t.Fatalf("id=%q cause=%v occupied slots=%d", event.id, context.Cause(event.ctx), len(s.slots))
	}
	requirePending(t, rdb, next)
	close(release)
	if last := awaitReclaimSignal(t, started); last.id != next {
		t.Fatalf("next execution=%q", last.id)
	}
	awaitReportState(t, rdb, next, "completed")
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if calls := canceledReports.Load(); calls != 0 {
		t.Fatalf("canceled handlers sent %d Redis report commands", calls)
	}
	for _, canceledID := range []string{id, fence} {
		if fields := submitHash(t, rdb, canceledID); fields["status"] != "canceled" || fields["last_error"] != "" {
			t.Fatalf("canceled handler overwrote task: %q", fields)
		}
	}
	waitForCancellationSubscribers(t, rdb, channel, 0)
	if len(s.Errors()) != 0 {
		t.Fatalf("normal cleanup produced fatal errors: %d", len(s.Errors()))
	}
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("shared client closed by Shutdown: %v", err)
	}
}

func TestCancelServerRedisDuringDrain(t *testing.T) {
	for _, force := range []bool{false, true} {
		t.Run(map[bool]string{false: "drain", true: "force"}[force], func(t *testing.T) {
			rdb := startSubmitRedis(t)
			client := integrationClient(t, rdb)
			ctx := context.Background()
			mustSubmit(t, client, "running", "job", nil)
			s := integrationServer(t, rdb, WithConcurrency(1))
			started, canceled, release := make(chan struct{}), make(chan error, 1), make(chan struct{})
			defer closeShutdownGate(release)
			if err := s.HandleFunc("job", func(ctx context.Context, _ *Task) error {
				close(started)
				<-ctx.Done()
				canceled <- context.Cause(ctx)
				<-release
				return ctx.Err()
			}); err != nil {
				t.Fatal(err)
			}
			if err := s.Start(); err != nil {
				t.Fatal(err)
			}
			awaitReclaimSignal(t, started)
			drain, cancel := context.WithCancel(ctx)
			defer cancel()
			finished := make(chan error, 1)
			go func() { finished <- s.Shutdown(drain) }()
			awaitReclaimSignal(t, s.claimDone)
			channel := "cancel:" + s.ownerID
			if n := rdb.PubSubNumSub(ctx, channel).Val()[channel]; n != 1 {
				t.Fatalf("draining server has %d subscriptions", n)
			}
			requireCancel(t, client, "running", CancelCanceled, "canceled")
			if err := awaitReclaimSignal(t, canceled); !errors.Is(err, errExecutionCanceled) {
				t.Fatalf("locally canceled while draining: %v", err)
			}
			if force {
				cancel()
				if err := awaitReclaimSignal(t, finished); !errors.Is(err, context.Canceled) {
					t.Fatalf("forced Shutdown = %v", err)
				}
				awaitReclaimSignal(t, s.subscriptionDone)
				if err := s.Shutdown(ctx); !errors.Is(err, ErrShutdownInProgress) {
					t.Fatalf("handler still alive but server considered stopped: %v", err)
				}
			}
			close(release)
			if !force {
				if err := awaitReclaimSignal(t, finished); err != nil {
					t.Fatal(err)
				}
			}
			awaitReclaimSignal(t, s.shutdownDone)
			waitForCancellationSubscribers(t, rdb, channel, 0)
			if len(s.Errors()) != 0 {
				t.Fatalf("stop produced fatal errors: %d", len(s.Errors()))
			}
			if err := rdb.Ping(ctx).Err(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func waitForCancellationSubscribers(t *testing.T, rdb *redis.Client, channel string, want int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for {
		counts, err := rdb.PubSubNumSub(ctx, channel).Result()
		if err != nil {
			t.Fatalf("waiting for %d subscribers on %s: %v", want, channel, err)
		}
		if counts[channel] == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
}

func TestCancelServerRedisUnexpectedSubscriptionClose(t *testing.T) {
	rdb := startSubmitRedis(t)
	client := integrationClient(t, rdb)
	ctx := context.Background()
	mustSubmit(t, client, "active", "job", nil)
	s := integrationServer(t, rdb, WithConcurrency(2), WithLeaseDuration(6*time.Second), WithRenewInterval(2*time.Second), WithReclaimInterval(2*time.Second))
	started := make(chan context.Context, 2)
	release := make(chan struct{})
	defer closeShutdownGate(release)
	if err := s.HandleFunc("job", func(ctx context.Context, _ *Task) error {
		started <- ctx
		<-release
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	handler := awaitReclaimSignal(t, started)
	lease := rdb.ZScore(ctx, claimLease, "active").Val()
	if err := s.subscription.Close(); err != nil {
		t.Fatal(err)
	}
	if err := awaitReclaimSignal(t, s.Errors()); !strings.Contains(err.Error(), "subscription closed unexpectedly") {
		t.Fatalf("unexpected fatal error: %v", err)
	}
	awaitReclaimSignal(t, s.claimDone)
	awaitReclaimSignal(t, s.subscriptionDone)
	mustSubmit(t, client, "unclaimed", "job", nil)
	deadline := time.Now().Add(5 * time.Second)
	for rdb.ZScore(ctx, claimLease, "active").Val() <= lease {
		if time.Now().After(deadline) {
			t.Fatal("healthy execution did not renew after subscription failure")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if handler.Err() != nil || s.reclaimCtx.Err() != nil {
		t.Fatalf("subscription failure canceled healthy work: handler=%v reclaim=%v", handler.Err(), s.reclaimCtx.Err())
	}
	requirePending(t, rdb, "unclaimed")
	close(release)
	awaitReportState(t, rdb, "active", "completed")
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestCancelServerRedisBeforeExecutionRegistration(t *testing.T) {
	rdb := startSubmitRedis(t)
	client := integrationClient(t, rdb)
	const id = "early-notice"
	ctx := context.Background()
	mustSubmit(t, client, "fence", "job", nil)
	s := integrationServer(t, rdb, WithConcurrency(2))
	fenceStarted, fenceCanceled, admitted := make(chan struct{}), make(chan struct{}), make(chan struct{})
	claimReturned, releaseClaim := make(chan struct{}), make(chan struct{})
	defer closeShutdownGate(releaseClaim)
	rdb.AddHook(observeRenewHook{process: func(next redis.ProcessHook) redis.ProcessHook {
		return func(ctx context.Context, cmd redis.Cmder) error {
			err := next(ctx, cmd)
			// 真实脚本授予执行权后，在响应交给 Start 循环前暂停。
			if value, ok := cmd.(*redis.Cmd); ok && err == nil {
				if fields, ok := value.Val().([]any); ok && len(fields) == 6 && fields[0] == id {
					close(claimReturned)
					<-releaseClaim
				}
			}
			return err
		}
	}})
	if err := s.HandleFunc("job", func(ctx context.Context, task *Task) error {
		if task.ID == id {
			close(admitted)
			return nil
		}
		close(fenceStarted)
		<-ctx.Done()
		close(fenceCanceled)
		return ctx.Err()
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	awaitReclaimSignal(t, fenceStarted)
	mustSubmit(t, client, id, "job", nil)
	awaitReclaimSignal(t, claimReturned)
	token := submitHash(t, rdb, id)["attempt_token"]
	requireCancel(t, client, id, CancelCanceled, "canceled")
	requireCancel(t, client, "fence", CancelCanceled, "canceled")
	awaitReclaimSignal(t, fenceCanceled)
	close(releaseClaim)
	awaitReclaimSignal(t, admitted) // 记录早到窗口的既定限制，以免将其描述为保证不启动。
	if applied, err := redisstore.Renew(ctx, rdb, redisstore.RenewParams{
		TaskID: id, AttemptToken: token, LeaseDurationMS: 30_000,
	}); err != nil || applied {
		t.Fatalf("canceled task renewed: %v, %v", applied, err)
	}
	if applied, err := redisstore.Complete(ctx, rdb, redisstore.CompleteParams{
		TaskID: id, AttemptToken: token, Result: []byte("late"),
	}); err != nil || applied {
		t.Fatalf("canceled task accepted result: %v, %v", applied, err)
	}
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if fields := submitHash(t, rdb, id); fields["status"] != "canceled" || fields["result"] != "" {
		t.Fatalf("late execution changed canceled task: %q", fields)
	}
}

func TestCancelServerRedisStopsInFlightReport(t *testing.T) {
	rdb := startSubmitRedis(t)
	client := integrationClient(t, rdb)
	const id = "reporting"
	ctx := context.Background()
	mustSubmit(t, client, id, "job", nil)
	s := integrationServer(t, rdb, WithConcurrency(1))
	reportStarted, reportCanceled := make(chan struct{}), make(chan error, 1)
	s.reportClient.AddHook(observeRenewHook{process: func(redis.ProcessHook) redis.ProcessHook {
		return func(ctx context.Context, _ redis.Cmder) error {
			close(reportStarted)
			<-ctx.Done()
			reportCanceled <- ctx.Err()
			return ctx.Err()
		}
	}})
	if err := s.HandleFunc("job", func(context.Context, *Task) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	awaitReclaimSignal(t, reportStarted)
	requireCancel(t, client, id, CancelCanceled, "canceled")
	if err := awaitReclaimSignal(t, reportCanceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("report was not canceled by notification: %v", err)
	}
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if fields := submitHash(t, rdb, id); fields["status"] != "canceled" || fields["last_error"] != "" || fields["result"] != "" {
		t.Fatalf("in-flight report changed cancellation: %q", fields)
	}
}

func TestCancelServerRedisDisconnectMissesOnlyMessagesDuringOutage(t *testing.T) {
	admin := startSubmitRedis(t)
	ctx := context.Background()
	client := integrationClient(t, admin)
	mustSubmit(t, client, "missed", "job", nil)

	reconnect, reconnectSeen := make(chan struct{}), make(chan struct{})
	defer closeShutdownGate(reconnect)
	var stall atomic.Bool
	rdb := redis.NewClient(&redis.Options{
		Addr: admin.Options().Addr, MaxRetries: -1, ContextTimeoutEnabled: true,
		Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
			if stall.CompareAndSwap(true, false) {
				close(reconnectSeen)
				<-reconnect
			}
			return (&net.Dialer{}).DialContext(ctx, network, addr)
		},
	})
	t.Cleanup(func() { _ = rdb.Close() })
	s := integrationServer(t, rdb, WithConcurrency(1))
	first, releaseFirst := make(chan context.Context, 1), make(chan struct{})
	defer closeShutdownGate(releaseFirst)
	second, secondCanceled := make(chan struct{}), make(chan error, 1)
	if err := s.HandleFunc("job", func(ctx context.Context, task *Task) error {
		if task.ID == "missed" {
			first <- ctx
			<-releaseFirst
			return nil
		}
		close(second)
		<-ctx.Done()
		secondCanceled <- context.Cause(ctx)
		return ctx.Err()
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	oldHandler := awaitReclaimSignal(t, first)
	channel := "cancel:" + s.ownerID
	// 临时实例上只有该 Server 的 Pub/Sub 连接；普通命令连接不受影响。
	stall.Store(true)
	if count, err := admin.ClientKillByFilter(ctx, "TYPE", "pubsub").Result(); err != nil || count != 1 {
		t.Fatalf("disconnect subscription: %d, %v", count, err)
	}
	awaitReclaimSignal(t, reconnectSeen)
	waitForCancellationSubscribers(t, admin, channel, 0)
	requireCancel(t, client, "missed", CancelCanceled, "canceled")
	close(reconnect)
	waitForCancellationSubscribers(t, admin, channel, 1)
	if oldHandler.Err() != nil || len(s.Errors()) != 0 {
		t.Fatalf("unexpected cancel or fatal error across reconnect: handler=%v errors=%d", oldHandler.Err(), len(s.Errors()))
	}
	close(releaseFirst)
	mustSubmit(t, client, "after-reconnect", "job", nil)
	awaitReclaimSignal(t, second)
	requireCancel(t, client, "after-reconnect", CancelCanceled, "canceled")
	if err := awaitReclaimSignal(t, secondCanceled); !errors.Is(err, errExecutionCanceled) {
		t.Fatalf("reconnected subscription did not deliver subsequent cancellation: %v", err)
	}
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if fields := submitHash(t, admin, "missed"); fields["status"] != "canceled" || fields["result"] != "" {
		t.Fatalf("missed notice did not preserve authoritative state: %q", fields)
	}
}
