//go:build integration

package taskq

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// 每个顶层测试启动独立 Redis，禁用持久化；不连接或清空调用方已有的实例。
// TASKQ_REDIS_SERVER 可指定可执行文件，否则从 PATH 查找 redis-server。
func startSubmitRedis(t *testing.T) *redis.Client {
	t.Helper()
	binary := os.Getenv("TASKQ_REDIS_SERVER")
	if binary == "" {
		var err error
		binary, err = exec.LookPath("redis-server")
		if err != nil {
			t.Fatal("integration tests require redis-server or TASKQ_REDIS_SERVER: ", err)
		}
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	logPath := filepath.Join(dir, "redis.log")
	cmd := exec.Command(binary, "--bind", "127.0.0.1", "--port", strconv.Itoa(port), "--save", "", "--appendonly", "no", "--dir", dir, "--logfile", logPath)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	rdb := redis.NewClient(&redis.Options{Addr: net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), MaxRetries: -1, DialTimeout: 100 * time.Millisecond, ReadTimeout: time.Second, WriteTimeout: time.Second})
	t.Cleanup(func() { _ = rdb.Close() })
	deadline := time.Now().Add(5 * time.Second)
	for {
		if err := rdb.Ping(context.Background()).Err(); err == nil {
			break
		}
		if time.Now().After(deadline) {
			log, _ := os.ReadFile(logPath)
			t.Fatalf("Redis did not start: %s", log)
		}
		time.Sleep(20 * time.Millisecond)
	}
	info, err := rdb.Info(context.Background(), "server").Result()
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(info, "\n") {
		if strings.HasPrefix(line, "redis_version:") {
			t.Log(strings.TrimSpace(line))
		}
	}
	return rdb
}

func integrationClient(t *testing.T, rdb *redis.Client) *Client {
	t.Helper()
	c, err := NewClient(rdb)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func mustSubmit(t *testing.T, c *Client, id, typ string, payload []byte, opts ...SubmitOption) *SubmitResult {
	t.Helper()
	result, err := c.Submit(context.Background(), id, typ, payload, opts...)
	if err != nil || result == nil {
		t.Fatalf("submit: result=%+v err=%v", result, err)
	}
	return result
}

func submitHash(t *testing.T, rdb *redis.Client, id string) map[string]string {
	t.Helper()
	fields, err := rdb.HGetAll(context.Background(), "taskq:meta:"+id).Result()
	if err != nil {
		t.Fatal(err)
	}
	return fields
}

func requirePending(t *testing.T, rdb *redis.Client, want ...string) {
	t.Helper()
	got, err := rdb.LRange(context.Background(), "taskq:default:pending", 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("pending=%q want=%q", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("pending=%q want=%q", got, want)
		}
	}
}

func TestSubmitRedis(t *testing.T) {
	rdb := startSubmitRedis(t)
	c := integrationClient(t, rdb)
	ctx := context.Background()
	run := func(name string, fn func(*testing.T)) {
		t.Run(name, func(t *testing.T) {
			if err := rdb.FlushDB(ctx).Err(); err != nil {
				t.Fatal(err)
			}
			fn(t)
		})
	}
	run("create-binary-and-milliseconds", func(t *testing.T) {
		id := " id\x00\r\n "
		payload := []byte{0, 255, 128, '\r', '\n', '{', '}'}
		before, err := rdb.Time(ctx).Result()
		if err != nil {
			t.Fatal(err)
		}
		if result := mustSubmit(t, c, id, "A0_-.:", payload, WithSubmitMaxRetry(3), WithSubmitExecutionTimeout(1500*time.Millisecond), WithSubmitRetryDelay(15001*time.Millisecond)); !result.Created || result.TaskID != id {
			t.Fatalf("result=%+v", result)
		}
		after, err := rdb.Time(ctx).Result()
		if err != nil {
			t.Fatal(err)
		}
		fields := submitHash(t, rdb, id)
		created, err := strconv.ParseInt(fields["created_at"], 10, 64)
		if err != nil || created < before.UnixMilli() || created > after.UnixMilli() {
			t.Fatalf("created_at=%q err=%v", fields["created_at"], err)
		}
		delete(fields, "created_at")
		want := map[string]string{"task_id": id, "task_type": "A0_-.:", "payload": string(payload), "max_retry": "3", "execution_timeout_ms": "1500", "retry_delay_ms": "15001", "queue": "default", "status": "pending", "attempts": "0"}
		if !reflect.DeepEqual(fields, want) {
			t.Fatalf("fields=%q want=%q", fields, want)
		}
		requirePending(t, rdb, id)
		if result := mustSubmit(t, c, id, "A0_-.:", payload, WithSubmitMaxRetry(3), WithSubmitExecutionTimeout(1500*time.Millisecond), WithSubmitRetryDelay(15001*time.Millisecond)); result.Created {
			t.Fatal("binary duplicate created")
		}
	})
	run("defaults-and-independent-IDs", func(t *testing.T) {
		if !mustSubmit(t, c, "id", "job", nil).Created {
			t.Fatal("not created")
		}
		before := submitHash(t, rdb, "id")
		if before["payload"] != "" || before["max_retry"] != "0" || before["execution_timeout_ms"] != "60000" || before["retry_delay_ms"] != "30000" {
			t.Fatalf("defaults=%q", before)
		}
		if mustSubmit(t, c, "id", "job", []byte{}, WithSubmitMaxRetry(0), WithSubmitExecutionTimeout(time.Minute), WithSubmitRetryDelay(30*time.Second)).Created {
			t.Fatal("equivalent defaults created")
		}
		if !reflect.DeepEqual(before, submitHash(t, rdb, "id")) {
			t.Fatal("duplicate changed hash")
		}
		if !mustSubmit(t, c, "other", "job", nil).Created {
			t.Fatal("distinct ID not created")
		}
		requirePending(t, rdb, "id", "other")
	})
	run("accepted-limits", func(t *testing.T) {
		payload := bytes.Repeat([]byte{255}, MaxPayloadSize)
		mustSubmit(t, c, "max", "job", payload, WithSubmitMaxRetry(MaxSubmitRetries), WithSubmitExecutionTimeout(MaxSubmitExecutionTimeout), WithSubmitRetryDelay(MaxSubmitRetryDelay))
		fields := submitHash(t, rdb, "max")
		if fields["payload"] != string(payload) || fields["max_retry"] != "2147483646" || fields["execution_timeout_ms"] != "86400000" || fields["retry_delay_ms"] != "604800000" {
			t.Fatal("limits stored incorrectly")
		}
		mustSubmit(t, c, "min", "job", nil, WithSubmitExecutionTimeout(time.Millisecond), WithSubmitRetryDelay(15*time.Second))
		fields = submitHash(t, rdb, "min")
		if fields["execution_timeout_ms"] != "1" || fields["retry_delay_ms"] != "15000" {
			t.Fatal("minima stored incorrectly")
		}
	})
	run("configuration-conflicts", func(t *testing.T) {
		mustSubmit(t, c, "id", "job", []byte("payload"))
		before := submitHash(t, rdb, "id")
		for _, tc := range []struct {
			typ     string
			payload []byte
			opts    []SubmitOption
		}{
			{"Job", []byte("payload"), nil},
			{"job", []byte("different"), nil},
			{"job", []byte("payload"), []SubmitOption{WithSubmitMaxRetry(1)}},
			{"job", []byte("payload"), []SubmitOption{WithSubmitExecutionTimeout(time.Second)}},
			{"job", []byte("payload"), []SubmitOption{WithSubmitRetryDelay(45 * time.Second)}},
		} {
			result, err := c.Submit(ctx, "id", tc.typ, tc.payload, tc.opts...)
			if result != nil || !errors.Is(err, ErrTaskIDConflict) {
				t.Fatalf("result=%+v err=%v", result, err)
			}
		}
		if !reflect.DeepEqual(before, submitHash(t, rdb, "id")) {
			t.Fatal("conflict changed hash")
		}
		requirePending(t, rdb, "id")
	})
	run("concurrent-same-configuration", func(t *testing.T) {
		var wg sync.WaitGroup
		var created atomic.Int32
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				result, err := c.Submit(ctx, "id", "job", nil)
				if err != nil {
					t.Error(err)
					return
				}
				if result.Created {
					created.Add(1)
				}
			}()
		}
		wg.Wait()
		if created.Load() != 1 {
			t.Fatalf("created=%d", created.Load())
		}
		requirePending(t, rdb, "id")
	})
	run("concurrent-different-configuration", func(t *testing.T) {
		var wg sync.WaitGroup
		var created, conflicts atomic.Int32
		for _, typ := range []string{"a", "b"} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				result, err := c.Submit(ctx, "id", typ, nil)
				if errors.Is(err, ErrTaskIDConflict) {
					conflicts.Add(1)
				} else if err == nil && result != nil && result.Created {
					created.Add(1)
				} else {
					t.Errorf("result=%+v err=%v", result, err)
				}
			}()
		}
		wg.Wait()
		if created.Load() != 1 || conflicts.Load() != 1 {
			t.Fatalf("created=%d conflict=%d", created.Load(), conflicts.Load())
		}
		requirePending(t, rdb, "id")
	})
	for _, status := range []string{"running", "retry_wait", "completed", "failed", "canceled"} {
		run("preserve-"+status, func(t *testing.T) {
			mustSubmit(t, c, "id", "job", nil)
			if err := rdb.Del(ctx, "taskq:default:pending").Err(); err != nil {
				t.Fatal(err)
			}
			if err := rdb.HSet(ctx, "taskq:meta:id", "status", status, "attempts", "3", "owner_id", "owner", "attempt_token", "token", "result", "result", "finished_at", "123").Err(); err != nil {
				t.Fatal(err)
			}
			before := submitHash(t, rdb, "id")
			if mustSubmit(t, c, "id", "job", nil).Created {
				t.Fatal("existing task recreated")
			}
			if !reflect.DeepEqual(before, submitHash(t, rdb, "id")) {
				t.Fatal("runtime data changed")
			}
			requirePending(t, rdb)
			if err := rdb.Set(ctx, "taskq:default:pending", "wrong-type", 0).Err(); err != nil {
				t.Fatal(err)
			}
			if mustSubmit(t, c, "id", "job", nil).Created {
				t.Fatal("duplicate checked or changed pending")
			}
			if got, err := rdb.Get(ctx, "taskq:default:pending").Result(); err != nil || got != "wrong-type" {
				t.Fatalf("pending=%q err=%v", got, err)
			}
		})
	}
	for _, field := range []string{"task_type", "payload", "max_retry", "execution_timeout_ms", "retry_delay_ms", "queue"} {
		run("missing-"+field, func(t *testing.T) {
			mustSubmit(t, c, "id", "job", nil)
			if err := rdb.HDel(ctx, "taskq:meta:id", field).Err(); err != nil {
				t.Fatal(err)
			}
			before := submitHash(t, rdb, "id")
			for _, typ := range []string{"job", "different"} {
				result, err := c.Submit(ctx, "id", typ, nil)
				if result != nil || !errors.Is(err, ErrStorageData) || errors.Is(err, ErrTaskIDConflict) {
					t.Fatalf("result=%+v err=%v", result, err)
				}
			}
			if !reflect.DeepEqual(before, submitHash(t, rdb, "id")) {
				t.Fatal("incomplete record repaired")
			}
			requirePending(t, rdb, "id")
		})
	}
	run("queue-conflict", func(t *testing.T) {
		mustSubmit(t, c, "id", "job", nil)
		if err := rdb.HSet(ctx, "taskq:meta:id", "queue", "other").Err(); err != nil {
			t.Fatal(err)
		}
		result, err := c.Submit(ctx, "id", "job", nil)
		if result != nil || !errors.Is(err, ErrTaskIDConflict) {
			t.Fatalf("result=%+v err=%v", result, err)
		}
		if submitHash(t, rdb, "id")["queue"] != "other" {
			t.Fatal("queue overwritten")
		}
	})
	run("wrong-pending-type", func(t *testing.T) {
		if err := rdb.Set(ctx, "taskq:default:pending", "sentinel", 0).Err(); err != nil {
			t.Fatal(err)
		}
		result, err := c.Submit(ctx, "id", "job", nil)
		if result != nil || err == nil || !redis.HasErrorPrefix(err, "WRONGTYPE") {
			t.Fatalf("result=%+v err=%v", result, err)
		}
		if n, err := rdb.Exists(ctx, "taskq:meta:id").Result(); err != nil || n != 0 {
			t.Fatalf("orphan hash exists=%d err=%v", n, err)
		}
		if got, err := rdb.Get(ctx, "taskq:default:pending").Result(); err != nil || got != "sentinel" {
			t.Fatalf("pending=%q err=%v", got, err)
		}
	})
	run("wrong-meta-type", func(t *testing.T) {
		if err := rdb.Set(ctx, "taskq:meta:id", "sentinel", 0).Err(); err != nil {
			t.Fatal(err)
		}
		result, err := c.Submit(ctx, "id", "job", nil)
		// Redis 5 的脚本错误会以 ERR 包装 HMGET 的 WRONGTYPE，仍须保留 Redis 错误链。
		var redisErr redis.Error
		if result != nil || !errors.As(err, &redisErr) || !strings.Contains(redisErr.Error(), "WRONGTYPE") {
			t.Fatalf("result=%+v err=%v", result, err)
		}
		if got, err := rdb.Get(ctx, "taskq:meta:id").Result(); err != nil || got != "sentinel" {
			t.Fatalf("meta=%q err=%v", got, err)
		}
		requirePending(t, rdb)
	})
	run("script-cache-miss", func(t *testing.T) {
		mustSubmit(t, c, "id", "job", nil)
		if err := rdb.ScriptFlush(ctx).Err(); err != nil {
			t.Fatal(err)
		}
		if mustSubmit(t, c, "id", "job", nil).Created {
			t.Fatal("cache miss recreated duplicate")
		}
		if err := rdb.ScriptFlush(ctx).Err(); err != nil {
			t.Fatal(err)
		}
		if !mustSubmit(t, c, "new", "job", nil).Created {
			t.Fatal("cache miss failed to create")
		}
		requirePending(t, rdb, "id", "new")
	})
}

// 丢弃服务端已经完整返回的创建确认，再断开连接；故障发生在真实 Lua 写入之后。
type dropSubmitReplyConn struct {
	net.Conn
	armed    *atomic.Bool
	dropped  *atomic.Bool
	expected []byte
}

func (c *dropSubmitReplyConn) Read(p []byte) (int, error) {
	if !c.armed.CompareAndSwap(true, false) {
		return c.Conn.Read(p)
	}
	reply := make([]byte, len(c.expected))
	_, err := io.ReadFull(c.Conn, reply)
	if err != nil {
		_ = c.Conn.Close()
		return 0, err
	}
	if !bytes.Equal(reply, c.expected) {
		_ = c.Conn.Close()
		return 0, fmt.Errorf("unexpected reply at fault point: %q", reply)
	}
	c.dropped.Store(true)
	_ = c.Conn.Close()
	// go-redis 后续设置 deadline 时也可能观察到 net.ErrClosed；两者均为断连错误。
	return 0, io.EOF
}

func TestSubmitRedisLostReply(t *testing.T) {
	admin := startSubmitRedis(t)
	ctx := context.Background()
	// 预热脚本，使故障点直接命中 EVALSHA 的创建响应。
	mustSubmit(t, integrationClient(t, admin), "warm", "job", nil)
	for _, retries := range []int{-1, 1} {
		t.Run(fmt.Sprintf("max-retries-%d", retries), func(t *testing.T) {
			id := fmt.Sprintf("lost-%d", retries)
			var armed, dropped atomic.Bool
			expected := []byte(fmt.Sprintf("*2\r\n$7\r\ncreated\r\n$%d\r\n%s\r\n", len(id), id))
			rdb := redis.NewClient(&redis.Options{
				Addr: admin.Options().Addr, MaxRetries: retries, PoolSize: 1, MinRetryBackoff: time.Millisecond, MaxRetryBackoff: time.Millisecond,
				Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
					conn, err := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, network, addr)
					if err != nil {
						return nil, err
					}
					return &dropSubmitReplyConn{Conn: conn, armed: &armed, dropped: &dropped, expected: expected}, nil
				},
			})
			t.Cleanup(func() { _ = rdb.Close() })
			if err := rdb.Ping(ctx).Err(); err != nil {
				t.Fatal(err)
			}
			c := integrationClient(t, rdb)
			armed.Store(true)
			result, err := c.Submit(ctx, id, "job", nil)
			if !dropped.Load() {
				t.Fatalf("fault not injected: result=%+v err=%v", result, err)
			}
			if retries < 0 {
				if result != nil || !(errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed)) {
					t.Fatalf("lost reply result=%+v err=%v", result, err)
				}
				result, err = c.Submit(ctx, id, "job", nil)
			}
			if err != nil || result == nil || result.Created || result.TaskID != id {
				t.Fatalf("retry confirmation=%+v err=%v", result, err)
			}
			fields := submitHash(t, admin, id)
			if fields["status"] != "pending" || fields["attempts"] != "0" {
				t.Fatalf("fields=%q", fields)
			}
			pending, err := admin.LRange(ctx, "taskq:default:pending", 0, -1).Result()
			if err != nil {
				t.Fatal(err)
			}
			count := 0
			for _, entry := range pending {
				if entry == id {
					count++
				}
			}
			if count != 1 {
				t.Fatalf("task queued %d times", count)
			}
		})
	}
}
