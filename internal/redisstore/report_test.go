package redisstore

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestReportStoreBoundary(t *testing.T) {
	ctx := context.Background()
	for _, failure := range []bool{false, true} {
		call := func(rdb *redis.Client) (bool, error) {
			if failure {
				return ReportFailure(ctx, rdb, FailureParams{"task", "token", "message"})
			}
			return Complete(ctx, rdb, CompleteParams{"task", "token", nil})
		}
		for _, value := range []any{int64(0), int64(1), nil, "1", int64(2), []any{int64(1)}} {
			calls := 0
			rdb := claimHookClient(t, func(_ context.Context, cmd redis.Cmder) error { calls++; cmd.(*redis.Cmd).SetVal(value); return nil })
			applied, err := call(rdb)
			valid := value == int64(0) || value == int64(1)
			if calls != 1 || valid && err != nil || !valid && !errors.Is(err, ErrReportResponse) || applied != (value == int64(1)) {
				t.Fatalf("failure=%v value=%#v applied=%v err=%v", failure, value, applied, err)
			}
		}
		for _, cause := range []error{io.EOF, context.DeadlineExceeded, claimRedisError("NOPERM forbidden"), claimRedisError("TASKQ_REPORT_INCONSISTENT task task: missing lease")} {
			calls := 0
			rdb := claimHookClient(t, func(context.Context, redis.Cmder) error { calls++; return cause })
			applied, err := call(rdb)
			if applied || calls != 1 || !errors.Is(err, cause) || errors.Is(err, ErrReportInconsistent) != redis.HasErrorPrefix(cause, "TASKQ_REPORT_INCONSISTENT") {
				t.Fatalf("applied=%v calls=%d err=%v", applied, calls, err)
			}
		}
		var commands []string
		rdb := claimHookClient(t, func(_ context.Context, cmd redis.Cmder) error {
			commands = append(commands, cmd.Name())
			if cmd.Name() == "evalsha" {
				return claimRedisError("NOSCRIPT missing")
			}
			cmd.(*redis.Cmd).SetVal(int64(1))
			return nil
		})
		if applied, err := call(rdb); !applied || err != nil || strings.Join(commands, ",") != "evalsha,eval" {
			t.Fatalf("applied=%v err=%v commands=%v", applied, err, commands)
		}
		if applied, err := call(nil); applied || !errors.Is(err, ErrReportArgument) {
			t.Fatalf("nil client accepted %v %v", applied, err)
		}
	}
	rdb := claimHookClient(t, func(context.Context, redis.Cmder) error { t.Fatal("invalid input sent command"); return nil })
	for _, p := range []CompleteParams{{"", "token", nil}, {"task", "", nil}, {"task", "token", make([]byte, 1001)}} {
		if applied, err := Complete(ctx, rdb, p); applied || !errors.Is(err, ErrReportArgument) {
			t.Fatalf("complete accepted invalid params %v %v", applied, err)
		}
	}
	for _, p := range []FailureParams{{"", "token", "error"}, {"task", "", "error"}, {"task", "token", ""}, {"task", "token", strings.Repeat("a", 4097)}} {
		if applied, err := ReportFailure(ctx, rdb, p); applied || !errors.Is(err, ErrReportArgument) {
			t.Fatalf("failure accepted invalid params %v %v", applied, err)
		}
	}
}
