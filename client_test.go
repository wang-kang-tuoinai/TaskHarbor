package taskq

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

type submitHook struct {
	process func(context.Context, redis.Cmder) error
}

func (h submitHook) DialHook(next redis.DialHook) redis.DialHook     { return next }
func (h submitHook) ProcessHook(redis.ProcessHook) redis.ProcessHook { return h.process }
func (h submitHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func newHookClient(t *testing.T, process func(context.Context, redis.Cmder) error) *Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1})
	t.Cleanup(func() { _ = rdb.Close() })
	rdb.AddHook(submitHook{process: process})
	c, err := NewClient(rdb)
	if err != nil {
		t.Fatal(err)
	}
	if c.rdb != rdb {
		t.Fatal("client did not retain caller's Redis client")
	}
	return c
}

func TestNewClientAndSubmitValidation(t *testing.T) {
	if c, err := NewClient(nil); c != nil || !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("nil rdb: client=%v err=%v", c, err)
	}
	calls := 0
	c := newHookClient(t, func(context.Context, redis.Cmder) error {
		calls++
		return errors.New("unexpected Redis command")
	})
	cases := []struct {
		name    string
		id      string
		typ     string
		payload []byte
		opts    []SubmitOption
	}{
		{"empty-id", "", "job", nil, nil},
		{"empty-type", "id", "", nil, nil},
		{"long-type", "id", strings.Repeat("a", 129), nil, nil},
		{"unicode", "id", "任务", nil, nil},
		{"space", "id", "job ", nil, nil},
		{"slash", "id", "job/a", nil, nil},
		{"nul", "id", "job\x00", nil, nil},
		{"payload", "id", "job", make([]byte, MaxPayloadSize+1), nil},
		{"nil-option", "id", "job", nil, []SubmitOption{nil}},
		{"negative-retry", "id", "job", nil, []SubmitOption{WithSubmitMaxRetry(-1)}},
		{"excess-retry", "id", "job", nil, []SubmitOption{WithSubmitMaxRetry(MaxSubmitRetries + 1)}},
	}
	for _, d := range []time.Duration{0, -time.Millisecond, time.Millisecond - time.Nanosecond, time.Second + time.Nanosecond, MaxSubmitExecutionTimeout + time.Millisecond, time.Duration(1<<63 - 1)} {
		cases = append(cases, struct {
			name, id, typ string
			payload       []byte
			opts          []SubmitOption
		}{"timeout-" + d.String(), "id", "job", nil, []SubmitOption{WithSubmitExecutionTimeout(d)}})
	}
	for _, d := range []time.Duration{0, -time.Second, 14999 * time.Millisecond, 15*time.Second + time.Nanosecond, MaxSubmitRetryDelay + time.Millisecond, time.Duration(1<<63 - 1)} {
		cases = append(cases, struct {
			name, id, typ string
			payload       []byte
			opts          []SubmitOption
		}{"delay-" + d.String(), "id", "job", nil, []SubmitOption{WithSubmitRetryDelay(d)}})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := c.Submit(context.Background(), tc.id, tc.typ, tc.payload, tc.opts...)
			if result != nil || !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("result=%+v err=%v", result, err)
			}
		})
	}
	if calls != 0 {
		t.Fatalf("construction or invalid input sent %d Redis commands", calls)
	}
}

func TestSubmitNormalizationAndOptions(t *testing.T) {
	var args []any
	c := newHookClient(t, func(_ context.Context, cmd redis.Cmder) error {
		args = cmd.Args()
		cmd.(*redis.Cmd).SetVal([]any{"created", args[5]})
		return nil
	})
	type testCase struct {
		name, typ      string
		payload        []byte
		opts           []SubmitOption
		retry          int
		timeout, delay int64
	}
	cases := []testCase{
		{name: "defaults", typ: "Az09_-.:", timeout: 60000, delay: 30000},
		{name: "minima", typ: "a", opts: []SubmitOption{WithSubmitExecutionTimeout(time.Millisecond), WithSubmitRetryDelay(15 * time.Second)}, timeout: 1, delay: 15000},
		{name: "milliseconds", typ: "a", opts: []SubmitOption{WithSubmitExecutionTimeout(1500 * time.Millisecond), WithSubmitRetryDelay(15001 * time.Millisecond)}, timeout: 1500, delay: 15001},
		{name: "maxima", typ: strings.Repeat("a", 128), payload: make([]byte, MaxPayloadSize), opts: []SubmitOption{WithSubmitMaxRetry(MaxSubmitRetries), WithSubmitExecutionTimeout(MaxSubmitExecutionTimeout), WithSubmitRetryDelay(MaxSubmitRetryDelay)}, retry: MaxSubmitRetries, timeout: 86400000, delay: 604800000},
		{name: "ordered-options", typ: "a", opts: []SubmitOption{WithSubmitMaxRetry(-1), WithSubmitMaxRetry(2), WithSubmitExecutionTimeout(0), WithSubmitExecutionTimeout(time.Second)}, retry: 2, timeout: 1000, delay: 30000},
		{name: "independent-defaults", typ: "a", payload: []byte{}, timeout: 60000, delay: 30000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			id := " id\x00 "
			result, err := c.Submit(context.Background(), id, tc.typ, tc.payload, tc.opts...)
			if err != nil || result == nil || result.TaskID != id || !result.Created {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			want := []any{"taskq:meta:" + id, "taskq:default:pending", id, tc.typ, tc.payload, tc.retry, tc.timeout, tc.delay, "default"}
			if !reflect.DeepEqual(args[3:], want) {
				t.Fatal("keys or normalized arguments differ")
			}
		})
	}
}

func TestSubmitErrorsAndResults(t *testing.T) {
	backendErr := errors.New("backend unavailable")
	for _, tc := range []struct {
		name          string
		value         any
		backend, want error
		created       bool
	}{
		{name: "created", value: []any{"created", "id"}, created: true},
		{name: "duplicate", value: []any{"duplicate", "id"}},
		{name: "conflict", value: []any{"conflict", "id"}, want: ErrTaskIDConflict},
		{name: "incomplete", value: []any{"incomplete", "id"}, want: ErrStorageData},
		{name: "malformed", value: []any{"created"}, want: ErrStorageData},
		{name: "unknown", value: []any{"not_found", "id"}, want: ErrStorageData},
		{name: "redis-nil", backend: redis.Nil, want: redis.Nil},
		{name: "backend", backend: backendErr, want: backendErr},
		{name: "canceled", backend: context.Canceled, want: context.Canceled},
		{name: "deadline", backend: context.DeadlineExceeded, want: context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), struct{}{}, "marker")
			calls := 0
			c := newHookClient(t, func(got context.Context, cmd redis.Cmder) error {
				calls++
				if got != ctx {
					t.Fatal("context not forwarded")
				}
				cmd.(*redis.Cmd).SetVal(tc.value)
				cmd.SetErr(tc.backend)
				return tc.backend
			})
			result, err := c.Submit(ctx, "id", "job", nil)
			if tc.want != nil {
				if result != nil || !errors.Is(err, tc.want) {
					t.Fatalf("result=%+v err=%v want=%v", result, err, tc.want)
				}
			} else if err != nil || result == nil || result.TaskID != "id" || result.Created != tc.created {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			if calls != 1 {
				t.Fatalf("platform issued %d commands", calls)
			}
		})
	}
}
