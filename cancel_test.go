package taskq

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestCancelValidatesInputAndPreservesCallerContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	calls := 0
	c := newHookClient(t, func(got context.Context, cmd redis.Cmder) error {
		calls++
		if got != ctx {
			t.Errorf("Cancel replaced the caller context")
		}
		cmd.(*redis.Cmd).SetVal([]any{"not_found", "", ""})
		return nil
	})
	if result, err := c.Cancel(ctx, ""); result != (CancelResult{}) || !errors.Is(err, ErrInvalidArgument) || calls != 0 {
		t.Fatalf("empty ID: result=%+v err=%v calls=%d", result, err, calls)
	}
	if result, err := c.Cancel(ctx, " "); result != (CancelResult{Outcome: CancelNotFound}) || err != nil || calls != 1 {
		t.Fatalf("nonempty ID: result=%+v err=%v calls=%d", result, err, calls)
	}
}

func TestCancelRejectsInvalidStorageResponse(t *testing.T) {
	for _, value := range []any{
		int64(1), []any{"canceled", "canceled"}, []any{"canceled", "canceled", int64(1)},
		[]any{"unknown", "canceled", ""}, []any{"already_terminal", "pending", ""},
		[]any{"canceled", "failed", ""}, []any{"not_found", "canceled", ""}, []any{"not_found", "", "invalid diagnostic"},
	} {
		c := newHookClient(t, func(_ context.Context, cmd redis.Cmder) error {
			cmd.(*redis.Cmd).SetVal(value)
			return nil
		})
		result, err := c.Cancel(context.Background(), "task")
		if result != (CancelResult{}) || !errors.Is(err, ErrStorageData) {
			t.Fatalf("response %v: result=%+v err=%v", value, result, err)
		}
	}
}
