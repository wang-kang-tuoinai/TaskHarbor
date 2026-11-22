//go:build integration

package taskq

import (
	"bytes"
	"context"
	"errors"
	"maps"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/wang-kang-tuoinai/TaskHarbor/internal/redisstore"
)

func TestGetTaskRedisLifecycle(t *testing.T) {
	rdb := startSubmitRedis(t)
	c := integrationClient(t, rdb)
	ctx := context.Background()

	const id = " task id "
	mustSubmit(t, c, id, "job", nil, WithSubmitMaxRetry(1))
	pending := requireTaskSnapshot(t, c, id, "pending", 0)
	if pending.Payload == nil || len(pending.Payload) != 0 || pending.Result != nil || pending.LastError != "" || pending.LastFailedAt != nil {
		t.Fatalf("initial pending task = %+v", pending)
	}
	mustClaimForCancel(t, rdb, id, "worker", "first")
	running := requireTaskSnapshot(t, c, id, "running", 1)
	if running.Result != nil || running.LastError != "" || running.LastFailedAt != nil {
		t.Fatalf("first running attempt = %+v", running)
	}
	applied, err := redisstore.ReportFailure(ctx, rdb, redisstore.FailureParams{
		TaskID: id, AttemptToken: "first", ErrorMessage: "first failure",
	})
	if err != nil || !applied {
		t.Fatalf("prepare failure: applied=%v err=%v", applied, err)
	}
	retrying := requireTaskSnapshot(t, c, id, "retry_wait", 1)
	if retrying.Result != nil || retrying.LastError != "first failure" || retrying.LastFailedAt == nil {
		t.Fatalf("retry_wait task = %+v", retrying)
	}
	// 令重试索引到期，让正常重试调度迁移任务，无需等待重试延迟。
	if err := rdb.ZAdd(ctx, "taskq:default:retry", redis.Z{Score: 1, Member: id}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := redisstore.ScheduleRetries(ctx, rdb); err != nil {
		t.Fatal(err)
	}
	requeued := requireTaskSnapshot(t, c, id, "pending", 1)
	if requeued.Result != nil || requeued.LastError != retrying.LastError || requeued.LastFailedAt == nil || !requeued.LastFailedAt.Equal(*retrying.LastFailedAt) {
		t.Fatalf("requeued task = %+v", requeued)
	}
	mustClaimForCancel(t, rdb, id, "worker", "second")
	result := []byte{0, 255, 128}
	applied, err = redisstore.Complete(ctx, rdb, redisstore.CompleteParams{TaskID: id, AttemptToken: "second", Result: result})
	if err != nil || !applied {
		t.Fatalf("prepare completion: applied=%v err=%v", applied, err)
	}
	completed := requireTaskSnapshot(t, c, id, "completed", 2)
	if !bytes.Equal(completed.Result, result) || completed.LastError != retrying.LastError || completed.LastFailedAt == nil || !completed.LastFailedAt.Equal(*retrying.LastFailedAt) || !completed.CreatedAt.Equal(pending.CreatedAt) {
		t.Fatalf("completed task = %+v", completed)
	}

	const expireID = "expired"
	mustSubmit(t, c, expireID, "job", nil)
	mustClaimForCancel(t, rdb, expireID, "worker", "lease")
	if err := rdb.ZAdd(ctx, claimLease, redis.Z{Score: 1, Member: expireID}).Err(); err != nil {
		t.Fatal(err)
	}
	requireTaskSnapshot(t, c, expireID, "running", 1)
	if err := redisstore.ReclaimLeases(ctx, rdb); err != nil {
		t.Fatal(err)
	}
	failed := requireTaskSnapshot(t, c, expireID, "failed", 1)
	if failed.Result != nil || failed.LastError != "lease expired" || failed.LastFailedAt == nil || !failed.FinishedAt.Equal(*failed.LastFailedAt) {
		t.Fatalf("failed task = %+v", failed)
	}

	const cancelID = "canceled before claim"
	mustSubmit(t, c, cancelID, "job", nil)
	requireCancel(t, c, cancelID, CancelCanceled, "canceled")
	canceled := requireTaskSnapshot(t, c, cancelID, "canceled", 0)
	if canceled.Result != nil || canceled.LastError != "" || canceled.LastFailedAt != nil {
		t.Fatalf("canceled task = %+v", canceled)
	}

	const cancelRetryID = "canceled while waiting to retry"
	mustSubmit(t, c, cancelRetryID, "job", nil, WithSubmitMaxRetry(1))
	mustClaimForCancel(t, rdb, cancelRetryID, "worker", "attempt")
	applied, err = redisstore.ReportFailure(ctx, rdb, redisstore.FailureParams{
		TaskID: cancelRetryID, AttemptToken: "attempt", ErrorMessage: "failure before cancel",
	})
	if err != nil || !applied {
		t.Fatalf("prepare failure before cancel: applied=%v err=%v", applied, err)
	}
	cancelRetrying := requireTaskSnapshot(t, c, cancelRetryID, "retry_wait", 1)
	if cancelRetrying.LastError != "failure before cancel" || cancelRetrying.LastFailedAt == nil {
		t.Fatalf("task waiting for retry before cancel = %+v", cancelRetrying)
	}
	requireCancel(t, c, cancelRetryID, CancelCanceled, "canceled")
	canceledAfterFailure := requireTaskSnapshot(t, c, cancelRetryID, "canceled", 1)
	if canceledAfterFailure.Result != nil || canceledAfterFailure.LastError != cancelRetrying.LastError ||
		canceledAfterFailure.LastFailedAt == nil || !canceledAfterFailure.LastFailedAt.Equal(*cancelRetrying.LastFailedAt) {
		t.Fatalf("canceled task after failure = %+v", canceledAfterFailure)
	}
}

func TestGetTaskRedisCompletionResultPresence(t *testing.T) {
	rdb := startSubmitRedis(t)
	c := integrationClient(t, rdb)
	ctx := context.Background()
	for _, tc := range []struct {
		name   string
		result []byte
	}{
		{name: "unset", result: nil},
		{name: "JSON null", result: []byte("null")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mustSubmit(t, c, tc.name, "job", nil)
			if task := requireTaskSnapshot(t, c, tc.name, "pending", 0); task.Result != nil {
				t.Fatalf("result before completion = %q, want nil", task.Result)
			}
			mustClaimForCancel(t, rdb, tc.name, "worker", "attempt")
			applied, err := redisstore.Complete(ctx, rdb, redisstore.CompleteParams{TaskID: tc.name, AttemptToken: "attempt", Result: tc.result})
			if err != nil || !applied {
				t.Fatalf("prepare completion: applied=%v err=%v", applied, err)
			}
			task := requireTaskSnapshot(t, c, tc.name, "completed", 1)
			if task.Result == nil || !bytes.Equal(task.Result, tc.result) {
				t.Fatalf("completed result = %#v, want present %q", task.Result, tc.result)
			}
		})
	}
}

func TestGetTaskRedisRejectsInvalidRecords(t *testing.T) {
	rdb := startSubmitRedis(t)
	c := integrationClient(t, rdb)
	ctx := context.Background()
	const id = "task"
	const key = "taskq:meta:" + id

	if task, err := c.GetTask(ctx, id); task != nil || !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("missing task = %+v, %v", task, err)
	}
	if err := rdb.Set(ctx, key, "wrong key type", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if task, err := c.GetTask(ctx, id); task != nil || !errors.Is(err, ErrStorageData) || !redis.HasErrorPrefix(err, "WRONGTYPE") {
		t.Fatalf("wrong key type = %+v, %v", task, err)
	}
	pending := map[string]string{
		"task_id": id, "task_type": "job", "queue": "default", "payload": "",
		"max_retry": "0", "execution_timeout_ms": "60000", "retry_delay_ms": "30000",
		"status": "pending", "attempts": "0", "created_at": "1234",
	}
	running := maps.Clone(pending)
	maps.Copy(running, map[string]string{
		"status": "running", "attempts": "1", "owner_id": "worker", "attempt_token": "token", "last_claimed_at": "1234",
	})
	completed := maps.Clone(running)
	delete(completed, "owner_id")
	delete(completed, "attempt_token")
	maps.Copy(completed, map[string]string{"status": "completed", "finished_at": "1235", "result": ""})
	retryWait := maps.Clone(completed)
	delete(retryWait, "result")
	delete(retryWait, "finished_at")
	maps.Copy(retryWait, map[string]string{"status": "retry_wait", "max_retry": "1", "last_error": "failure", "last_failed_at": "1235"})
	for _, tc := range []struct {
		name string
		base map[string]string
		set  map[string]string
		drop string
	}{
		{name: "only unknown fields", base: map[string]string{"future_field": "1"}},
		{name: "mismatched ID", base: pending, set: map[string]string{"task_id": "another"}},
		{name: "missing payload", base: pending, drop: "payload"},
		{name: "missing creation timestamp", base: pending, drop: "created_at"},
		{name: "invalid task type", base: pending, set: map[string]string{"task_type": "two words"}},
		{name: "unknown queue", base: pending, set: map[string]string{"queue": "other"}},
		{name: "unknown status", base: pending, set: map[string]string{"status": "unknown"}},
		{name: "negative retries", base: pending, set: map[string]string{"max_retry": "-1"}},
		{name: "attempts exceed budget", base: running, set: map[string]string{"attempts": "2"}},
		{name: "fractional timeout", base: pending, set: map[string]string{"execution_timeout_ms": "1.1"}},
		{name: "timeout duration overflow", base: pending, set: map[string]string{"execution_timeout_ms": "9223372036855"}},
		{name: "delay too short", base: pending, set: map[string]string{"retry_delay_ms": "14999"}},
		{name: "timestamp integer overflow", base: pending, set: map[string]string{"created_at": "9223372036854775808"}},
		{name: "empty optional timestamp", base: running, set: map[string]string{"last_claimed_at": ""}},
		{name: "running without claim time", base: running, drop: "last_claimed_at"},
		{name: "running without token", base: running, drop: "attempt_token"},
		{name: "running with empty owner", base: running, set: map[string]string{"owner_id": ""}},
		{name: "pending with residual empty identity", base: pending, set: map[string]string{"owner_id": ""}},
		{name: "pending with finish time", base: pending, set: map[string]string{"finished_at": "1235"}},
		{name: "pending with result", base: pending, set: map[string]string{"result": ""}},
		{name: "completed without finish time", base: completed, drop: "finished_at"},
		{name: "completed without result", base: completed, drop: "result"},
		{name: "failure error without timestamp", base: retryWait, drop: "last_failed_at"},
		{name: "retry_wait exhausted", base: retryWait, set: map[string]string{"max_retry": "0"}},
		{name: "failed with different failure and finish times", base: retryWait, set: map[string]string{
			"status": "failed", "max_retry": "0", "finished_at": "1236",
		}},
		{name: "retried pending without failure history", base: pending, set: map[string]string{
			"max_retry": "1", "attempts": "1", "last_claimed_at": "1235",
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := rdb.Del(ctx, key).Err(); err != nil {
				t.Fatal(err)
			}
			fields := maps.Clone(tc.base)
			maps.Copy(fields, tc.set)
			delete(fields, tc.drop)
			if err := rdb.HSet(ctx, key, fields).Err(); err != nil {
				t.Fatal(err)
			}
			if task, err := c.GetTask(ctx, id); task != nil || !errors.Is(err, ErrStorageData) || errors.Is(err, ErrTaskNotFound) {
				t.Fatalf("GetTask = %+v, %v, want storage data error", task, err)
			}
		})
	}
}

func TestGetTaskRedisConcurrentCancel(t *testing.T) {
	rdb := startSubmitRedis(t)
	c := integrationClient(t, rdb)
	const id = "task"
	mustSubmit(t, c, id, "job", nil)
	requireTaskSnapshot(t, c, id, "pending", 0)
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	start := make(chan struct{})
	var readers sync.WaitGroup
	for range 8 {
		readers.Go(func() {
			<-start
			for range 8 {
				task, err := c.GetTask(ctx, id)
				if err != nil || task == nil || task.TaskID != id || task.Attempts != 0 || task.LastClaimedAt != nil || task.Result != nil {
					t.Errorf("invalid snapshot during cancel: %+v, %v", task, err)
					return
				}
				if !(task.Status == "pending" && task.FinishedAt == nil || task.Status == "canceled" && task.FinishedAt != nil) {
					t.Errorf("incomplete snapshot during cancel: %+v", task)
					return
				}
			}
		})
	}
	close(start)
	result, err := c.Cancel(ctx, id)
	readers.Wait()
	if err != nil || result.Outcome != CancelCanceled {
		t.Fatalf("Cancel = %+v, %v", result, err)
	}
	requireTaskSnapshot(t, c, id, "canceled", 0)
}

func requireTaskSnapshot(t *testing.T, c *Client, id, status string, attempts int) *TaskInfo {
	t.Helper()
	task, err := c.GetTask(context.Background(), id)
	if err != nil || task == nil || task.TaskID != id || task.Status != status || task.Attempts != attempts {
		t.Fatalf("GetTask(%q) = %+v, %v; want %s with %d attempts", id, task, err, status, attempts)
	}
	wantFinished := status == "completed" || status == "failed" || status == "canceled"
	if task.CreatedAt.IsZero() || (task.LastClaimedAt != nil) != (attempts > 0) || (task.FinishedAt != nil) != wantFinished {
		t.Fatalf("unexpected timestamps in %+v", task)
	}
	return task
}
