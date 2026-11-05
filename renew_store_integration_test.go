//go:build integration

package taskq

import (
	"context"
	"errors"
	"io"
	"math"
	"net"
	"os"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/wang-kang-tuoinai/TaskHarbor/internal/redisstore"
)

func seedRenew(t *testing.T, rdb *redis.Client, id, token string) {
	t.Helper()
	ctx := context.Background()
	now, err := rdb.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	// 仅放入 Renew 所需字段，避免把无关配置缺失误判为本操作损坏。
	if err := rdb.HSet(ctx, "taskq:meta:"+id, "status", "running", "attempt_token", token).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.SAdd(ctx, claimActive, id).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.ZAdd(ctx, claimLease, redis.Z{Member: id, Score: float64(now.UnixMilli() + 5000)}).Err(); err != nil {
		t.Fatal(err)
	}
}

func TestRenewRedis(t *testing.T) {
	rdb := startSubmitRedis(t)
	ctx := context.Background()
	p := redisstore.RenewParams{TaskID: "id", AttemptToken: "token", LeaseDurationMS: 30000}
	seedRenew(t, rdb, "id", "token")
	seedRenew(t, rdb, "other", "other-token")
	hash := submitHash(t, rdb, "id")
	other, err := rdb.ZScore(ctx, claimLease, "other").Result()
	if err != nil {
		t.Fatal(err)
	}
	before, err := rdb.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	if err := rdb.ScriptFlush(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	if applied, err := redisstore.Renew(ctx, rdb, p); !applied || err != nil {
		t.Fatalf("applied=%v err=%v", applied, err)
	}
	after, err := rdb.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	score, err := rdb.ZScore(ctx, claimLease, "id").Result()
	if err != nil || score < float64(before.UnixMilli()+30000) || score > float64(after.UnixMilli()+30000) {
		t.Fatalf("score=%v err=%v", score, err)
	}
	if !reflect.DeepEqual(hash, submitHash(t, rdb, "id")) {
		t.Fatal("renew changed task hash")
	}
	if got, err := rdb.ZScore(ctx, claimLease, "other").Result(); err != nil || got != other {
		t.Fatalf("other score=%v err=%v", got, err)
	}
	if member, err := rdb.SIsMember(ctx, claimActive, "id").Result(); err != nil || !member {
		t.Fatalf("active=%v err=%v", member, err)
	}
}

func TestRenewRedisRejectionsAndConsistencyAreReadOnly(t *testing.T) {
	rdb := startSubmitRedis(t)
	ctx := context.Background()
	cases := []struct {
		name   string
		mutate func() error
		want   error
	}{
		{"missing", func() error { return rdb.Del(ctx, "taskq:meta:id").Err() }, nil},
		{"expired", func() error { return rdb.ZAdd(ctx, claimLease, redis.Z{Member: "id", Score: 1}).Err() }, nil},
		{"old-token", func() error { return rdb.HSet(ctx, "taskq:meta:id", "attempt_token", "new-token").Err() }, nil},
		{"missing-active", func() error { return rdb.SRem(ctx, claimActive, "id").Err() }, redisstore.ErrRenewInconsistent},
		{"missing-lease", func() error { return rdb.ZRem(ctx, claimLease, "id").Err() }, redisstore.ErrRenewInconsistent},
	}
	for _, key := range []string{"taskq:meta:id", claimActive, claimLease} {
		cases = append(cases, struct {
			name   string
			mutate func() error
			want   error
		}{"type-" + key, func() error { return rdb.Set(ctx, key, "bad", 0).Err() }, redisstore.ErrRenewInconsistent})
	}
	for _, field := range []string{"status", "attempt_token"} {
		cases = append(cases, struct {
			name   string
			mutate func() error
			want   error
		}{"missing-" + field, func() error { return rdb.HDel(ctx, "taskq:meta:id", field).Err() }, redisstore.ErrRenewInconsistent})
	}
	for field, values := range map[string][]string{"status": {"unknown", ""}, "attempt_token": {""}} {
		for _, value := range values {
			cases = append(cases, struct {
				name   string
				mutate func() error
				want   error
			}{field + "=" + value, func() error { return rdb.HSet(ctx, "taskq:meta:id", field, value).Err() }, redisstore.ErrRenewInconsistent})
		}
	}
	for _, score := range []float64{-1, 1.5, 1 << 53, math.Inf(1), math.Inf(-1)} {
		cases = append(cases, struct {
			name   string
			mutate func() error
			want   error
		}{"score-" + strconv.FormatFloat(score, 'g', -1, 64), func() error { return rdb.ZAdd(ctx, claimLease, redis.Z{Member: "id", Score: score}).Err() }, redisstore.ErrRenewInconsistent})
	}
	for _, status := range []string{"pending", "retry_wait", "completed", "failed", "canceled"} {
		cases = append(cases, struct {
			name   string
			mutate func() error
			want   error
		}{status, func() error {
			if err := rdb.HSet(ctx, "taskq:meta:id", "status", status).Err(); err != nil {
				return err
			}
			if err := rdb.HDel(ctx, "taskq:meta:id", "attempt_token").Err(); err != nil {
				return err
			}
			return rdb.Del(ctx, claimActive, claimLease).Err()
		}, nil})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := rdb.FlushDB(ctx).Err(); err != nil {
				t.Fatal(err)
			}
			seedRenew(t, rdb, "id", "token")
			if err := tc.mutate(); err != nil {
				t.Fatal(err)
			}
			before := claimSnapshot(t, rdb)
			applied, err := redisstore.Renew(ctx, rdb, redisstore.RenewParams{TaskID: "id", AttemptToken: "token", LeaseDurationMS: 30000})
			if applied || tc.want == nil && err != nil || tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("applied=%v err=%v want=%v", applied, err, tc.want)
			}
			if !reflect.DeepEqual(before, claimSnapshot(t, rdb)) {
				t.Fatal("rejection or consistency error changed data")
			}
		})
	}
	t.Run("addition-overflow", func(t *testing.T) {
		if err := rdb.FlushDB(ctx).Err(); err != nil {
			t.Fatal(err)
		}
		seedRenew(t, rdb, "id", "token")
		before := claimSnapshot(t, rdb)
		applied, err := redisstore.Renew(ctx, rdb, redisstore.RenewParams{TaskID: "id", AttemptToken: "token", LeaseDurationMS: 1<<53 - 1})
		if applied || err == nil || !reflect.DeepEqual(before, claimSnapshot(t, rdb)) {
			t.Fatalf("applied=%v err=%v", applied, err)
		}
	})
}

func TestRenewRedisExactExpiryBoundary(t *testing.T) {
	rdb := startSubmitRedis(t)
	ctx := context.Background()
	source, err := os.ReadFile("internal/redisstore/scripts/renew_task.lua")
	if err != nil {
		t.Fatal(err)
	}
	// 在真实 Redis 中运行同一源码，仅替换时间来源，使相等边界不依赖跨毫秒调度。
	prefix := `local backing = redis
local redis = {error_reply=backing.error_reply, call=function(command, ...)
    if command == 'TIME' then return {'100', '0'} end
    return backing.call(command, ...)
end}
`
	for _, score := range []float64{100000, 100001} {
		seedRenew(t, rdb, "id", "token")
		if err := rdb.ZAdd(ctx, claimLease, redis.Z{Member: "id", Score: score}).Err(); err != nil {
			t.Fatal(err)
		}
		before := claimSnapshot(t, rdb)
		applied, err := rdb.Eval(ctx, prefix+string(source), []string{"taskq:meta:id", claimActive, claimLease}, "id", "token", 1000).Int64()
		if err != nil {
			t.Fatal(err)
		}
		if score == 100000 {
			if applied != 0 || !reflect.DeepEqual(before, claimSnapshot(t, rdb)) {
				t.Fatal("now == score revived lease")
			}
		} else if got, err := rdb.ZScore(ctx, claimLease, "id").Result(); applied != 1 || err != nil || got != 101000 {
			t.Fatalf("applied=%d score=%v err=%v", applied, got, err)
		}
	}
}

func TestRenewRedisConcurrentTransition(t *testing.T) {
	rdb := startSubmitRedis(t)
	ctx := context.Background()
	for _, status := range []string{"running", "canceled"} {
		t.Run(status, func(t *testing.T) {
			if err := rdb.FlushDB(ctx).Err(); err != nil {
				t.Fatal(err)
			}
			seedRenew(t, rdb, "id", "old")
			now, err := rdb.Time(ctx).Result()
			if err != nil {
				t.Fatal(err)
			}
			future := now.UnixMilli() + 60000
			var wg sync.WaitGroup
			for range 20 {
				wg.Go(func() {
					if _, err := redisstore.Renew(ctx, rdb, redisstore.RenewParams{TaskID: "id", AttemptToken: "old", LeaseDurationMS: 30000}); err != nil {
						t.Error(err)
					}
				})
			}
			// 受控原子迁移用于构造前置状态，不代表真实 Cancel/Reclaim 已实现。
			if err := rdb.Eval(ctx, `redis.call('HSET',KEYS[1],'status',ARGV[1],'attempt_token','new')
if ARGV[1]=='running' then redis.call('ZADD',KEYS[3],ARGV[3],ARGV[2])
else redis.call('HDEL',KEYS[1],'attempt_token');redis.call('SREM',KEYS[2],ARGV[2]);redis.call('ZREM',KEYS[3],ARGV[2]) end
return 1`, []string{"taskq:meta:id", claimActive, claimLease}, status, "id", future).Err(); err != nil {
				t.Fatal(err)
			}
			wg.Wait()
			before := claimSnapshot(t, rdb)
			if applied, err := redisstore.Renew(ctx, rdb, redisstore.RenewParams{TaskID: "id", AttemptToken: "old", LeaseDurationMS: 30000}); applied || err != nil {
				t.Fatalf("old applied=%v err=%v", applied, err)
			}
			if !reflect.DeepEqual(before, claimSnapshot(t, rdb)) {
				t.Fatal("old renew changed migrated data")
			}
			if status == "running" {
				if got, err := rdb.ZScore(ctx, claimLease, "id").Result(); err != nil || got != float64(future) {
					t.Fatalf("new score=%v err=%v", got, err)
				}
			}
		})
	}
}

func TestRenewRedisLostReply(t *testing.T) {
	admin := startSubmitRedis(t)
	ctx := context.Background()
	p := redisstore.RenewParams{TaskID: "id", AttemptToken: "token", LeaseDurationMS: 60000}
	if _, err := redisstore.Renew(ctx, admin, p); err != nil {
		t.Fatal(err)
	}
	seedRenew(t, admin, "id", "token")
	hash := submitHash(t, admin, "id")
	before, err := admin.ZScore(ctx, claimLease, "id").Result()
	if err != nil {
		t.Fatal(err)
	}
	var armed, dropped atomic.Bool
	rdb := redis.NewClient(&redis.Options{Addr: admin.Options().Addr, MaxRetries: -1, PoolSize: 1, ReadTimeout: time.Second, WriteTimeout: time.Second, Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return &dropSubmitReplyConn{Conn: conn, armed: &armed, dropped: &dropped, expected: []byte(":1\r\n")}, nil
	}})
	t.Cleanup(func() { _ = rdb.Close() })
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	armed.Store(true)
	applied, err := redisstore.Renew(ctx, rdb, p)
	if applied || !dropped.Load() || !(errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed)) {
		t.Fatalf("applied=%v dropped=%v err=%v", applied, dropped.Load(), err)
	}
	after, err := admin.ZScore(ctx, claimLease, "id").Result()
	if err != nil || after <= before || !reflect.DeepEqual(hash, submitHash(t, admin, "id")) {
		t.Fatalf("score before=%v after=%v err=%v", before, after, err)
	}
}
