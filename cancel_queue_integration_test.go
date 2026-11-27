//go:build integration

package taskq

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestCancelRedisCustomQueue(t *testing.T) {
	rdb := startSubmitRedis(t)
	c := integrationClient(t, rdb)
	ctx := context.Background()
	const id, queue, owner, token = "send-email", "Mail_9-queue", "original-owner", "original-attempt"
	const meta = "taskq:meta:" + id
	const pending = "taskq:Mail_9-queue:pending"
	const active = "taskq:Mail_9-queue:active"
	const lease = "taskq:Mail_9-queue:lease"
	const retry = "taskq:Mail_9-queue:retry"

	for _, tc := range []struct {
		status  string
		indexes []string
	}{
		{"pending", []string{pending}},
		{"running", []string{active, lease}},
		{"retry_wait", []string{retry}},
		{"completed", nil},
		{"failed", nil},
		{"canceled", nil},
	} {
		t.Run(tc.status, func(t *testing.T) {
			if err := rdb.FlushDB(ctx).Err(); err != nil {
				t.Fatal(err)
			}
			mustSubmit(t, c, id, "job", nil, WithSubmitQueue(queue))
			createdAt := submitHash(t, rdb, id)["created_at"]
			attempts := 0
			if tc.status != "pending" && tc.status != "canceled" {
				attempts = 1
			}
			_, err := rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
				// 相邻队列故意留有同 ID 的残留，验证取消只检查、移除所属队列的索引。
				pipe.RPush(ctx, "taskq:default:pending", id)
				pipe.SAdd(ctx, "taskq:default:active", id)
				pipe.ZAdd(ctx, "taskq:default:lease", redis.Z{Score: 1, Member: id})
				pipe.ZAdd(ctx, "taskq:default:retry", redis.Z{Score: 2, Member: id})
				if tc.status != "pending" {
					pipe.LRem(ctx, pending, 0, id)
					pipe.HSet(ctx, meta, "status", tc.status, "attempts", attempts)
				}
				if attempts > 0 {
					pipe.HSet(ctx, meta, "last_claimed_at", createdAt)
				}
				switch tc.status {
				case "running":
					pipe.HSet(ctx, meta, "owner_id", owner, "attempt_token", token)
					pipe.SAdd(ctx, active, id)
					pipe.ZAdd(ctx, lease, redis.Z{Score: 1, Member: id})
				case "retry_wait":
					pipe.HSet(ctx, meta, "max_retry", 1, "last_error", "failure", "last_failed_at", createdAt)
					pipe.ZAdd(ctx, retry, redis.Z{Score: 2, Member: id})
				case "completed":
					pipe.HSet(ctx, meta, "result", `{"ok":true}`, "finished_at", createdAt)
				case "failed":
					pipe.HSet(ctx, meta, "last_error", "failure", "last_failed_at", createdAt, "finished_at", createdAt)
				case "canceled":
					pipe.HSet(ctx, meta, "finished_at", createdAt)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			var notices *redis.PubSub
			if tc.status == "running" {
				notices = rdb.Subscribe(ctx, "cancel:"+owner)
				t.Cleanup(func() { _ = notices.Close() })
				if _, err := notices.Receive(ctx); err != nil {
					t.Fatal(err)
				}
			}
			before := claimSnapshot(t, rdb)
			if len(tc.indexes) == 0 {
				requireCancel(t, c, id, CancelAlreadyTerminal, tc.status)
				if after := claimSnapshot(t, rdb); !reflect.DeepEqual(after, before) {
					t.Fatal("terminal cancellation changed Redis data")
				}
				return
			}

			requireCancel(t, c, id, CancelCanceled, "canceled")
			if got := requireTaskSnapshot(t, c, id, "canceled", attempts); got.Queue != queue {
				t.Fatalf("canceled task queue = %q, want %q", got.Queue, queue)
			}
			after := claimSnapshot(t, rdb)
			for _, key := range tc.indexes {
				if _, exists := after[key]; exists {
					t.Fatalf("cancel left the target in %q", key)
				}
				delete(before, key)
			}
			delete(before, meta)
			delete(after, meta)
			if !reflect.DeepEqual(after, before) {
				t.Fatal("cancel changed an unrelated index")
			}
			if notices != nil {
				value, err := notices.ReceiveTimeout(ctx, time.Second)
				message, ok := value.(*redis.Message)
				var notice map[string]string
				if err != nil || !ok || message.Channel != "cancel:"+owner || json.Unmarshal([]byte(message.Payload), &notice) != nil ||
					!reflect.DeepEqual(notice, map[string]string{"task_id": id, "attempt_token": token}) {
					t.Fatalf("unexpected original owner notice: %+v, %v", value, err)
				}
			}
		})
	}
}

func TestCancelRedisInvalidQueueDoesNotWrite(t *testing.T) {
	rdb := startSubmitRedis(t)
	c := integrationClient(t, rdb)
	ctx := context.Background()
	const id, meta = "task", "taskq:meta:task"
	for _, tc := range []struct {
		name, storedQueue string
		hasPendingMember  bool
	}{
		{"missing queue", "", false},
		{"empty queue", "", true},
		{"reserved queue", "meta", true},
		{"invalid character", "has:colon", true},
		{"embedded NUL", "has\x00nul", true},
		{"non-ASCII", "邮件", true},
		{"too long", strings.Repeat("a", 65), true},
		{"valid queue without membership", "Missing-member", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := rdb.FlushDB(ctx).Err(); err != nil {
				t.Fatal(err)
			}
			mustSubmit(t, c, id, "job", nil)
			if tc.name == "missing queue" {
				if err := rdb.HDel(ctx, meta, "queue").Err(); err != nil {
					t.Fatal(err)
				}
			} else if err := rdb.HSet(ctx, meta, "queue", tc.storedQueue).Err(); err != nil {
				t.Fatal(err)
			}
			if tc.hasPendingMember {
				if err := rdb.RPush(ctx, "taskq:"+tc.storedQueue+":pending", id).Err(); err != nil {
					t.Fatal(err)
				}
			}
			// 长字段可能将 Hash 转为哈希表，DUMP 的字段顺序会随 Redis 渐进式 rehash 改变。
			beforeHash := submitHash(t, rdb, id)
			before := claimSnapshot(t, rdb)
			delete(before, meta)
			if got, err := c.Cancel(ctx, id); got != (CancelResult{}) || !errors.Is(err, ErrStorageData) {
				t.Fatalf("Cancel = %+v, %v, want storage data error", got, err)
			}
			after := claimSnapshot(t, rdb)
			delete(after, meta)
			if !reflect.DeepEqual(after, before) || !reflect.DeepEqual(submitHash(t, rdb, id), beforeHash) {
				t.Fatal("failed cancellation changed Redis data")
			}
		})
	}
}
