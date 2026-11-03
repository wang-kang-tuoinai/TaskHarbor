package redisstore

import (
	"context"
	"errors"
	"io"
	"net"
	"reflect"
	"testing"

	"github.com/redis/go-redis/v9"
)

type claimHook struct {
	process func(context.Context, redis.Cmder) error
}

func (h claimHook) DialHook(next redis.DialHook) redis.DialHook     { return next }
func (h claimHook) ProcessHook(redis.ProcessHook) redis.ProcessHook { return h.process }
func (h claimHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func claimHookClient(t *testing.T, process func(context.Context, redis.Cmder) error) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1})
	t.Cleanup(func() { _ = rdb.Close() })
	rdb.AddHook(claimHook{process: process})
	return rdb
}

func TestClaimValidation(t *testing.T) {
	p := ClaimParams{OwnerID: "owner", AttemptToken: "token", LeaseDurationMS: 30000}
	if task, err := Claim(context.Background(), nil, p); task != nil || !errors.Is(err, ErrClaimArgument) {
		t.Fatalf("nil client: task=%+v err=%v", task, err)
	}
	rdb := claimHookClient(t, func(context.Context, redis.Cmder) error {
		t.Fatal("invalid input sent Redis command")
		return nil
	})
	for _, bad := range []ClaimParams{
		{"", "token", 1}, {"owner", "", 1}, {"owner", "token", 0},
		{"owner", "token", -1}, {"owner", "token", maxExactInteger + 1},
	} {
		if task, err := Claim(context.Background(), rdb, bad); task != nil || !errors.Is(err, ErrClaimArgument) {
			t.Fatalf("params=%+v task=%+v err=%v", bad, task, err)
		}
	}
}

// Redis 错误与网络错误需要保留各自的错误链，而不是一起转换为空队列。
type claimRedisError string

func (e claimRedisError) Error() string { return string(e) }
func (e claimRedisError) RedisError()   {}

func TestClaimErrorsAndFallback(t *testing.T) {
	p := ClaimParams{OwnerID: "owner", AttemptToken: "token", LeaseDurationMS: 30000}
	for _, cause := range []error{
		io.EOF, &net.OpError{Op: "read", Net: "tcp", Err: context.DeadlineExceeded},
		context.Canceled, claimRedisError("NOPERM forbidden"), claimRedisError("WRONGTYPE invalid key"),
		claimRedisError("ERR script failure"), claimRedisError("TASKQ_CLAIM_INCONSISTENT task id: missing record"),
	} {
		t.Run(cause.Error(), func(t *testing.T) {
			calls := 0
			rdb := claimHookClient(t, func(_ context.Context, cmd redis.Cmder) error {
				calls++
				if cmd.Name() != "evalsha" {
					t.Fatalf("unexpected replay: %s", cmd.Name())
				}
				return cause
			})
			task, err := Claim(context.Background(), rdb, p)
			if task != nil || !errors.Is(err, cause) || calls != 1 {
				t.Fatalf("task=%+v err=%v calls=%d", task, err, calls)
			}
			if errors.Is(err, ErrClaimInconsistent) != redis.HasErrorPrefix(cause, "TASKQ_CLAIM_INCONSISTENT") {
				t.Fatalf("wrong classification: %v", err)
			}
		})
	}
	var commands []string
	rdb := claimHookClient(t, func(_ context.Context, cmd redis.Cmder) error {
		commands = append(commands, cmd.Name())
		if cmd.Name() == "evalsha" {
			return claimRedisError("NOSCRIPT No matching script")
		}
		cmd.(*redis.Cmd).SetVal([]any{"id", "job", "", "default", "1", "60000"})
		return nil
	})
	if task, err := Claim(context.Background(), rdb, p); err != nil || task == nil || task.AttemptToken != "token" {
		t.Fatalf("task=%+v err=%v", task, err)
	}
	if !reflect.DeepEqual(commands, []string{"evalsha", "eval"}) || rdb.Options().MaxRetries != 0 {
		t.Fatalf("commands=%q options=%+v", commands, rdb.Options())
	}
}

func TestClaimResponse(t *testing.T) {
	p := ClaimParams{OwnerID: "owner", AttemptToken: "token", LeaseDurationMS: 1}
	valid := []any{"id", "job", "\x00\xff\r\n", "default", "2147483647", "86400000"}
	// 通过 Claim 验证解析失败也不重放请求，且不会返回可执行任务。
	responses := []any{nil, "scalar", []any{"id"}, append(append([]any{}, valid...), "extra")}
	for i := range valid {
		for _, bad := range []any{nil, int64(1)} {
			v := append([]any{}, valid...)
			v[i] = bad
			responses = append(responses, v)
		}
	}
	for i, badValues := range map[int][]string{
		0: {""}, 1: {""}, 3: {"other", ""},
		4: {"", "0", "-1", "1.5", "1e2", "2147483648", "999999999999999999999"},
		5: {"", "0", "-1", "1.5", "1e2", "86400001", "9223372036854775808"},
	} {
		for _, bad := range badValues {
			v := append([]any{}, valid...)
			v[i] = bad
			responses = append(responses, v)
		}
	}
	for _, response := range responses {
		calls := 0
		rdb := claimHookClient(t, func(_ context.Context, cmd redis.Cmder) error {
			calls++
			cmd.(*redis.Cmd).SetVal(response)
			return nil
		})
		if task, err := Claim(context.Background(), rdb, p); task != nil || !errors.Is(err, ErrClaimResponse) || calls != 1 {
			t.Fatalf("response=%#v task=%+v err=%v calls=%d", response, task, err, calls)
		}
	}
	for _, response := range []any{[]any{}, valid, []any{"id", "job", "", "default", "1", "1"}} {
		rdb := claimHookClient(t, func(_ context.Context, cmd redis.Cmder) error {
			cmd.(*redis.Cmd).SetVal(response)
			return nil
		})
		task, err := Claim(context.Background(), rdb, p)
		if err != nil {
			t.Fatal(err)
		}
		if len(response.([]any)) == 0 {
			if task != nil {
				t.Fatalf("empty response: %+v", task)
			}
		} else if task == nil || task.OwnerID != "owner" || task.AttemptToken != "token" || string(task.Payload) != response.([]any)[2] {
			t.Fatalf("task=%+v", task)
		}
	}
}
