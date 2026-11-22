//go:build integration

package taskq

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/wang-kang-tuoinai/TaskHarbor/internal/redisstore"
)

func TestCancelPendingRedis(t *testing.T) {
	rdb := startSubmitRedis(t)
	client := integrationClient(t, rdb)
	ctx := context.Background()
	id := " pending "
	mustSubmit(t, client, id, "job", []byte("initial"), WithSubmitMaxRetry(2))
	mustSubmit(t, client, "other", "job", nil)
	if err := rdb.RPush(ctx, "taskq:default:pending", id).Err(); err != nil {
		t.Fatal(err)
	}

	before := submitHash(t, rdb, id)
	beforeTime := rdb.Time(ctx).Val()
	requireCancel(t, client, id, CancelCanceled, "canceled")
	afterTime := rdb.Time(ctx).Val()
	requirePending(t, rdb, "other")
	after := submitHash(t, rdb, id)
	finished, err := strconv.ParseInt(after["finished_at"], 10, 64)
	if err != nil || finished < beforeTime.UnixMilli() || finished > afterTime.UnixMilli() {
		t.Fatalf("finished_at=%q outside Redis time interval [%v, %v]", after["finished_at"], beforeTime, afterTime)
	}
	before["status"] = "canceled"
	before["finished_at"] = after["finished_at"]
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("cancel changed unrelated fields: got %q want %q", after, before)
	}
	requireCancel(t, client, id, CancelAlreadyTerminal, "canceled")
	if got := submitHash(t, rdb, id); !reflect.DeepEqual(got, after) {
		t.Fatalf("repeated cancel changed terminal task: got %q want %q", got, after)
	}
	requireCancel(t, client, "unknown", CancelNotFound, "")
	if err := rdb.HGet(ctx, "taskq:meta:unknown", "status").Err(); err != redis.Nil {
		t.Fatalf("not_found created a task: %v", err)
	}
}

func TestCancelNotificationFailureRedis(t *testing.T) {
	for _, tc := range []struct {
		name       string
		id         string
		diagnostic string
		redisArgs  []string
	}{
		{"disabled publish", "task", "Unknown Redis command", []string{"--rename-command", "PUBLISH", ""}},
		{"invalid UTF-8", "task\xff", "not valid UTF-8", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rdb := startSubmitRedis(t, tc.redisArgs...)
			client := integrationClient(t, rdb)
			ctx := context.Background()
			const owner = "cancel-owner"
			var sub *redis.PubSub
			if tc.redisArgs == nil {
				sub = rdb.Subscribe(ctx, "cancel:"+owner)
				t.Cleanup(func() { _ = sub.Close() })
				if _, err := sub.Receive(ctx); err != nil {
					t.Fatal(err)
				}
			}
			mustSubmit(t, client, tc.id, "job", nil)
			mustClaimForCancel(t, rdb, tc.id, owner, "attempt")
			var output bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&output, nil)))
			defer slog.SetDefault(previous)

			requireCancel(t, client, tc.id, CancelCanceled, "canceled")
			requireCancel(t, client, tc.id, CancelAlreadyTerminal, "canceled")
			if fields := submitHash(t, rdb, tc.id); fields["status"] != "canceled" || fields["owner_id"] != "" || fields["attempt_token"] != "" {
				t.Fatalf("notification failure changed cancellation: %q", fields)
			}
			if rdb.SIsMember(ctx, "taskq:default:active", tc.id).Val() || rdb.ZScore(ctx, "taskq:default:lease", tc.id).Err() != redis.Nil {
				t.Fatal("execution index retained after cancellation with failed notification")
			}
			if !strings.Contains(output.String(), tc.diagnostic) {
				t.Errorf("missing diagnostic %q: %s", tc.diagnostic, &output)
			}
			if sub != nil {
				const fence = "after cancellation"
				if err := rdb.Publish(ctx, "cancel:"+owner, fence).Err(); err != nil {
					t.Fatal(err)
				}
				msg, err := sub.ReceiveTimeout(ctx, time.Second)
				if message, ok := msg.(*redis.Message); err != nil || !ok || message.Payload != fence {
					t.Fatalf("published a notification for invalid task ID: %v, %v", msg, err)
				}
			}
		})
	}
}

func requireCancel(t *testing.T, client *Client, id string, outcome CancelOutcome, status string) {
	t.Helper()
	result, err := client.Cancel(context.Background(), id)
	if err != nil || result != (CancelResult{Outcome: outcome, Status: status}) {
		t.Fatalf("Cancel(%q) = %+v, %v; want %q, %q", id, result, err, outcome, status)
	}
}

func TestCancelRunningRedis(t *testing.T) {
	rdb := startSubmitRedis(t)
	client := integrationClient(t, rdb)
	ctx := context.Background()
	id, token := "任务:\"\\\x00\n", "token:\"\\\x00\n"
	const owner = "cancel-owner"
	mustSubmit(t, client, id, "job", nil)
	sub := rdb.Subscribe(ctx, "cancel:"+owner)
	t.Cleanup(func() { _ = sub.Close() })
	if _, err := sub.Receive(ctx); err != nil {
		t.Fatal(err)
	}
	mustClaimForCancel(t, rdb, id, owner, token)
	if err := rdb.ZAdd(ctx, "taskq:default:lease", redis.Z{Score: 1, Member: id}).Err(); err != nil {
		t.Fatal(err)
	}
	// 过期且尝试预算已耗尽仍可取消。此分支不扫描异常 pending 残留。
	if err := rdb.RPush(ctx, "taskq:default:pending", id).Err(); err != nil {
		t.Fatal(err)
	}
	before := submitHash(t, rdb, id)
	requireCancel(t, client, id, CancelCanceled, "canceled")
	after := submitHash(t, rdb, id)
	delete(before, "owner_id")
	delete(before, "attempt_token")
	before["status"] = "canceled"
	before["finished_at"] = after["finished_at"]
	if after["finished_at"] == "" || !reflect.DeepEqual(after, before) {
		t.Fatalf("unexpected canceled hash: got %q want %q", after, before)
	}
	if rdb.SIsMember(ctx, "taskq:default:active", id).Val() || rdb.ZScore(ctx, "taskq:default:lease", id).Err() != redis.Nil {
		t.Fatal("running indexes retained after cancel")
	}
	requirePending(t, rdb, id)
	msg, err := sub.ReceiveTimeout(ctx, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]string
	if message, ok := msg.(*redis.Message); !ok || json.Unmarshal([]byte(message.Payload), &fields) != nil ||
		!reflect.DeepEqual(fields, map[string]string{"task_id": id, "attempt_token": token}) {
		t.Fatalf("invalid cancellation notice: %#v", msg)
	}
}

func mustClaimForCancel(t *testing.T, rdb *redis.Client, taskID, owner, token string) {
	t.Helper()
	claimed, err := redisstore.Claim(context.Background(), rdb, redisstore.ClaimParams{
		OwnerID: owner, AttemptToken: token, LeaseDurationMS: 30_000,
	})
	if err != nil || claimed == nil || claimed.TaskID != taskID {
		t.Fatalf("claim %q: got %+v, %v", taskID, claimed, err)
	}
}

func TestCancelRetryWaitRedis(t *testing.T) {
	rdb := startSubmitRedis(t)
	client := integrationClient(t, rdb)
	ctx := context.Background()
	const id, token = "retrying", "attempt"
	mustSubmit(t, client, id, "job", nil, WithSubmitMaxRetry(1))
	mustClaimForCancel(t, rdb, id, "worker", token)
	ok, err := redisstore.ReportFailure(ctx, rdb, redisstore.FailureParams{
		TaskID: id, AttemptToken: token, ErrorMessage: "first failure",
	})
	if err != nil || !ok {
		t.Fatalf("ReportFailure = %v, %v", ok, err)
	}
	before := submitHash(t, rdb, id)
	if before["status"] != "retry_wait" {
		t.Fatalf("unexpected retry state: %q", before)
	}
	requireCancel(t, client, id, CancelCanceled, "canceled")
	after := submitHash(t, rdb, id)
	before["status"] = "canceled"
	before["finished_at"] = after["finished_at"]
	if after["finished_at"] == "" || !reflect.DeepEqual(after, before) {
		t.Fatalf("retry cancellation changed unrelated fields: got %q want %q", after, before)
	}
	if err := rdb.ZScore(ctx, "taskq:default:retry", id).Err(); err != redis.Nil {
		t.Fatalf("retry member retained: %v", err)
	}
}

func TestCancelRejectsInconsistentRedisStateWithoutWrites(t *testing.T) {
	rdb := startSubmitRedis(t)
	client := integrationClient(t, rdb)
	ctx := context.Background()
	const id = "task"
	meta := "taskq:meta:" + id
	keys := []string{meta, claimPending, claimActive, claimLease, "taskq:default:retry"}
	cases := []struct {
		name, state string
		corrupt     func() error
	}{
		{"wrong hash type", "pending", func() error {
			if err := rdb.Del(ctx, meta).Err(); err != nil {
				return err
			}
			return rdb.Set(ctx, meta, "legacy", 0).Err()
		}},
		{"unknown status", "pending", func() error { return rdb.HSet(ctx, meta, "status", "unknown").Err() }},
		{"missing pending member", "pending", func() error { return rdb.LRem(ctx, claimPending, 0, id).Err() }},
		{"wrong pending type", "pending", func() error {
			if err := rdb.Del(ctx, claimPending).Err(); err != nil {
				return err
			}
			return rdb.Set(ctx, claimPending, "legacy", 0).Err()
		}},
		{"wrong active type", "pending", func() error { return rdb.Set(ctx, claimActive, "legacy", 0).Err() }},
		{"wrong lease type", "pending", func() error { return rdb.Set(ctx, claimLease, "legacy", 0).Err() }},
		{"wrong retry type", "pending", func() error { return rdb.Set(ctx, keys[4], "legacy", 0).Err() }},
		{"pending identity", "pending", func() error { return rdb.HSet(ctx, meta, "owner_id", "").Err() }},
		{"pending active conflict", "pending", func() error { return rdb.SAdd(ctx, claimActive, id).Err() }},
		{"pending lease conflict", "pending", func() error { return rdb.ZAdd(ctx, claimLease, redis.Z{Score: 1, Member: id}).Err() }},
		{"pending retry conflict", "pending", func() error { return rdb.ZAdd(ctx, keys[4], redis.Z{Score: 1, Member: id}).Err() }},
		{"missing owner", "running", func() error { return rdb.HDel(ctx, meta, "owner_id").Err() }},
		{"empty token", "running", func() error { return rdb.HSet(ctx, meta, "attempt_token", "").Err() }},
		{"missing active member", "running", func() error { return rdb.SRem(ctx, claimActive, id).Err() }},
		{"missing lease member", "running", func() error { return rdb.ZRem(ctx, claimLease, id).Err() }},
		{"running retry conflict", "running", func() error { return rdb.ZAdd(ctx, keys[4], redis.Z{Score: 1, Member: id}).Err() }},
		{"missing retry member", "retry_wait", func() error { return rdb.ZRem(ctx, keys[4], id).Err() }},
		{"retry empty identity", "retry_wait", func() error { return rdb.HSet(ctx, meta, "attempt_token", "").Err() }},
	}
	snapshot := func(t *testing.T) map[string]string {
		t.Helper()
		state := make(map[string]string)
		for _, key := range keys {
			dump, err := rdb.Dump(ctx, key).Result()
			if err == nil {
				state[key] = dump
			} else if err != redis.Nil {
				t.Fatal(err)
			}
		}
		return state
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := rdb.Del(ctx, keys...).Err(); err != nil {
				t.Fatal(err)
			}
			mustSubmit(t, client, id, "job", nil, WithSubmitMaxRetry(1))
			if tc.state != "pending" {
				mustClaimForCancel(t, rdb, id, "worker", "attempt")
			}
			if tc.state == "retry_wait" {
				ok, err := redisstore.ReportFailure(ctx, rdb, redisstore.FailureParams{
					TaskID: id, AttemptToken: "attempt", ErrorMessage: "first failure",
				})
				if err != nil || !ok {
					t.Fatalf("expected retry_wait: %v, %v", ok, err)
				}
			}
			if err := tc.corrupt(); err != nil {
				t.Fatal(err)
			}
			before := snapshot(t)
			result, err := client.Cancel(ctx, id)
			if result != (CancelResult{}) || !errors.Is(err, ErrStorageData) {
				t.Fatalf("Cancel: result=%+v error=%v", result, err)
			}
			if after := snapshot(t); !reflect.DeepEqual(before, after) {
				t.Fatalf("inconsistent cancellation changed task or indexes")
			}
		})
	}
}

func TestCancelCompetingRedisOperations(t *testing.T) {
	rdb := startSubmitRedis(t)
	c := integrationClient(t, rdb)
	ctx := context.Background()
	for _, tc := range []struct {
		name           string
		beforeClaim    bool
		operation      func(id, token string) (bool, error)
		stateIfApplied string
	}{
		{"claim", true, func(id, token string) (bool, error) {
			got, err := redisstore.Claim(ctx, rdb, redisstore.ClaimParams{OwnerID: "worker", AttemptToken: token, LeaseDurationMS: 30_000})
			return got != nil && got.TaskID == id, err
		}, ""},
		{"renew", false, func(id, token string) (bool, error) {
			return redisstore.Renew(ctx, rdb, redisstore.RenewParams{TaskID: id, AttemptToken: token, LeaseDurationMS: 30_000})
		}, ""},
		{"complete", false, func(id, token string) (bool, error) {
			return redisstore.Complete(ctx, rdb, redisstore.CompleteParams{TaskID: id, AttemptToken: token, Result: []byte("result")})
		}, "completed"},
		{"failure", false, func(id, token string) (bool, error) {
			return redisstore.ReportFailure(ctx, rdb, redisstore.FailureParams{TaskID: id, AttemptToken: token, ErrorMessage: "failure"})
		}, "failed"},
		{"reclaim", false, func(id, _ string) (bool, error) {
			err := redisstore.ReclaimLeases(ctx, rdb)
			return submitHash(t, rdb, id)["status"] == "failed", err
		}, "failed"},
	} {
		for _, cancelFirst := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/cancel-first=%v", tc.name, cancelFirst), func(t *testing.T) {
				const token = "attempt"
				id := t.Name()
				mustSubmit(t, c, id, "job", nil)
				if !tc.beforeClaim {
					mustClaimForCancel(t, rdb, id, "worker", token)
				}
				if tc.name == "reclaim" {
					if err := rdb.ZAdd(ctx, claimLease, redis.Z{Score: 1, Member: id}).Err(); err != nil {
						t.Fatal(err)
					}
				}
				if cancelFirst {
					requireCancel(t, c, id, CancelCanceled, "canceled")
				}
				applied, err := tc.operation(id, token)
				if err != nil || applied == cancelFirst {
					t.Fatalf("%s operation = %v, %v", tc.name, applied, err)
				}
				if cancelFirst || tc.stateIfApplied != "" {
					terminal := "canceled"
					if !cancelFirst {
						terminal = tc.stateIfApplied
					}
					before := submitHash(t, rdb, id)
					requireCancel(t, c, id, CancelAlreadyTerminal, terminal)
					if after := submitHash(t, rdb, id); !reflect.DeepEqual(before, after) {
						t.Fatalf("repeated Cancel changed terminal hash: before=%q after=%q", before, after)
					}
				} else {
					requireCancel(t, c, id, CancelCanceled, "canceled")
				}
			})
		}
	}
}

func TestCancelConcurrentRedisRequests(t *testing.T) {
	rdb := startSubmitRedis(t)
	c := integrationClient(t, rdb)
	mustSubmit(t, c, "same", "job", nil)
	type outcome struct {
		result CancelResult
		err    error
	}
	results, start := make(chan outcome, 8), make(chan struct{})
	for range cap(results) {
		go func() {
			<-start
			result, err := c.Cancel(context.Background(), "same")
			results <- outcome{result, err}
		}()
	}
	close(start)
	changed := 0
	for range cap(results) {
		got := awaitReclaimSignal(t, results)
		if got.err != nil || got.result.Status != "canceled" {
			t.Fatal(got)
		}
		switch got.result.Outcome {
		case CancelCanceled:
			changed++
		case CancelAlreadyTerminal:
		default:
			t.Fatalf("unexpected result: %+v", got)
		}
	}
	if changed != 1 {
		t.Fatalf("%d requests performed the migration", changed)
	}
}
