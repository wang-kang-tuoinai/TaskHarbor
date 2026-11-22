package taskq

import (
	"context"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestGetTaskValidatesInputAndPreservesCallerContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const id = " task id "
	calls := 0
	c := newHookClient(t, func(got context.Context, cmd redis.Cmder) error {
		calls++
		if got != ctx {
			t.Error("GetTask replaced the caller context")
		}
		if want := []any{"hgetall", "taskq:meta:" + id}; !reflect.DeepEqual(cmd.Args(), want) {
			t.Fatalf("Redis command = %v, want %v", cmd.Args(), want)
		}
		cmd.(*redis.MapStringStringCmd).SetVal(map[string]string{})
		return nil
	})
	if task, err := c.GetTask(ctx, ""); task != nil || !errors.Is(err, ErrInvalidArgument) || calls != 0 {
		t.Fatalf("empty ID: task=%+v err=%v calls=%d", task, err, calls)
	}
	if task, err := c.GetTask(ctx, id); task != nil || !errors.Is(err, ErrTaskNotFound) || calls != 1 {
		t.Fatalf("missing task: task=%+v err=%v calls=%d", task, err, calls)
	}
}

func TestGetTaskPreservesRequestFailures(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, want := range []error{ctx.Err(), io.ErrUnexpectedEOF} {
		t.Run(want.Error(), func(t *testing.T) {
			c := newHookClient(t, func(_ context.Context, _ redis.Cmder) error { return want })
			task, err := c.GetTask(ctx, "task")
			if task != nil || !errors.Is(err, want) || errors.Is(err, ErrTaskNotFound) || errors.Is(err, ErrStorageData) {
				t.Fatalf("GetTask = %+v, %v, want %v", task, err, want)
			}
		})
	}
}

func TestGetTaskReturnsPendingSnapshot(t *testing.T) {
	const id = " pending "
	fields := map[string]string{
		"task_id": id, "task_type": "jobs:v2.Work", "queue": "default", "payload": "\x00\xff",
		"max_retry": "2", "execution_timeout_ms": "1501", "retry_delay_ms": "16002",
		"status": "pending", "attempts": "0", "created_at": "1234", "future_field": "ignored",
	}
	c := newHookClient(t, func(_ context.Context, cmd redis.Cmder) error {
		cmd.(*redis.MapStringStringCmd).SetVal(fields)
		return nil
	})
	want := &TaskInfo{
		TaskID: id, TaskType: "jobs:v2.Work", Queue: "default", Payload: []byte{0, 255},
		MaxRetry: 2, ExecutionTimeout: time.Second + 501*time.Millisecond, RetryDelay: 16*time.Second + 2*time.Millisecond,
		Status: "pending", CreatedAt: time.Date(1970, 1, 1, 0, 0, 1, 234_000_000, time.UTC),
	}
	if task, err := c.GetTask(context.Background(), id); err != nil || !reflect.DeepEqual(task, want) {
		t.Fatalf("GetTask = %+v, %v, want %+v", task, err, want)
	}
}
