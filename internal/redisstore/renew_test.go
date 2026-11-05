package redisstore

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestRenewAppliedAndRejection(t *testing.T) {
	for _, response := range []int64{0, 1} {
		calls := 0
		rdb := claimHookClient(t, func(_ context.Context, cmd redis.Cmder) error {
			calls++
			cmd.(*redis.Cmd).SetVal(response)
			return nil
		})
		applied, err := Renew(context.Background(), rdb, RenewParams{TaskID: "task", AttemptToken: "token", LeaseDurationMS: 30000})
		if err != nil || applied != (response == 1) || calls != 1 {
			t.Fatalf("response=%v applied=%v err=%v calls=%d", response, applied, err, calls)
		}
	}
}

func TestRenewValidationResponsesAndErrors(t *testing.T) {
	p := RenewParams{TaskID: "id", AttemptToken: "token", LeaseDurationMS: 1}
	if applied, err := Renew(context.Background(), nil, p); applied || !errors.Is(err, ErrRenewArgument) {
		t.Fatalf("applied=%v err=%v", applied, err)
	}
	rdb := claimHookClient(t, func(context.Context, redis.Cmder) error { t.Fatal("invalid input sent command"); return nil })
	for _, bad := range []RenewParams{{"", "token", 1}, {"id", "", 1}, {"id", "token", 0}, {"id", "token", -1}, {"id", "token", maxExactInteger + 1}} {
		if applied, err := Renew(context.Background(), rdb, bad); applied || !errors.Is(err, ErrRenewArgument) {
			t.Fatalf("params=%+v applied=%v err=%v", bad, applied, err)
		}
	}
	for _, value := range []any{nil, "1", int64(-1), int64(2), []any{int64(1)}} {
		calls := 0
		client := claimHookClient(t, func(_ context.Context, cmd redis.Cmder) error { calls++; cmd.(*redis.Cmd).SetVal(value); return nil })
		if applied, err := Renew(context.Background(), client, p); applied || !errors.Is(err, ErrRenewResponse) || calls != 1 {
			t.Fatalf("response=%#v applied=%v err=%v calls=%d", value, applied, err, calls)
		}
	}
	for _, cause := range []error{io.EOF, context.DeadlineExceeded, claimRedisError("NOPERM forbidden"), claimRedisError("TASKQ_RENEW_INCONSISTENT task id: missing lease")} {
		calls := 0
		client := claimHookClient(t, func(context.Context, redis.Cmder) error { calls++; return cause })
		applied, err := Renew(context.Background(), client, p)
		if applied || !errors.Is(err, cause) || calls != 1 || errors.Is(err, ErrRenewInconsistent) != redis.HasErrorPrefix(cause, "TASKQ_RENEW_INCONSISTENT") {
			t.Fatalf("applied=%v err=%v calls=%d", applied, err, calls)
		}
	}
	commands := []string{}
	client := claimHookClient(t, func(_ context.Context, cmd redis.Cmder) error {
		commands = append(commands, cmd.Name())
		if cmd.Name() == "evalsha" {
			return claimRedisError("NOSCRIPT missing")
		}
		cmd.(*redis.Cmd).SetVal(int64(1))
		return nil
	})
	if applied, err := Renew(context.Background(), client, p); !applied || err != nil || len(commands) != 2 || commands[0] != "evalsha" || commands[1] != "eval" {
		t.Fatalf("applied=%v err=%v commands=%q", applied, err, commands)
	}
}
