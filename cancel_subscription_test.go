package taskq

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/redis/go-redis/v9"
)

// 使用真实 RESP 握手和可控的内存连接，假时钟测试不会依赖本机端口或 Redis 进程。
func subscriptionPipe(reply func(string) string) func(context.Context, string, string) (net.Conn, error) {
	return func(context.Context, string, string) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer server.Close()
			reader := bufio.NewReader(server)
			for {
				cmd, err := readRedisTCPCommand(reader)
				if err != nil {
					return
				}
				var response string
				switch strings.ToLower(cmd[0]) {
				case "hello":
					response = "-ERR unknown command 'hello'\r\n"
				case "subscribe":
					response = reply(cmd[1])
				case "ping":
					response = "+PONG\r\n"
				default:
					response = "-ERR unexpected command\r\n"
				}
				if _, err := io.WriteString(server, response); err != nil {
					return
				}
			}
		}()
		return client, nil
	}
}

func TestShutdownWhileCancellationSubscriptionIsStarting(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		subscribed, ack := make(chan struct{}), make(chan struct{})
		dialCtx := make(chan context.Context, 1)
		pipe := subscriptionPipe(func(channel string) string {
			close(subscribed)
			<-ack
			return cancelSubscribeAck(channel)
		})
		rdb := redis.NewClient(&redis.Options{
			Addr: "127.0.0.1:1", MaxRetries: -1, ContextTimeoutEnabled: true, Protocol: 2, DisableIdentity: true,
			Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
				dialCtx <- ctx
				return pipe(ctx, network, addr)
			},
		})
		defer rdb.Close()
		s, err := NewServer(rdb)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.HandleFunc("job", func(context.Context, *Task) error { return nil }); err != nil {
			t.Fatal(err)
		}
		worker := s.(*server)
		started := make(chan error, 1)
		go func() { started <- s.Start() }()
		synctest.Wait()
		defer closeShutdownGate(ack)
		select {
		case <-subscribed:
		default:
			t.Fatal("Start did not send SUBSCRIBE")
		}

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		if err := s.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Shutdown during network wait = %v", err)
		}
		if err := (<-dialCtx).Err(); !errors.Is(err, context.Canceled) {
			t.Fatalf("startup context was not canceled: %v", err)
		}
		if err := s.Shutdown(context.Background()); !errors.Is(err, ErrShutdownInProgress) {
			t.Fatalf("marked stopped with acknowledgment blocked: %v", err)
		}
		close(ack)
		if err := <-started; err == nil {
			t.Fatal("late acknowledgment resurrected server")
		}
		<-worker.shutdownDone
		if err := s.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := s.Start(); err == nil {
			t.Fatal("restarted server after startup cancellation")
		}
		for _, done := range []chan struct{}{worker.claimDone, worker.renewDone, worker.reclaimDone, worker.subscriptionDone} {
			select {
			case <-done:
				t.Fatal("background loop started after shutdown")
			default:
			}
		}
	})
}

func cancelSubscribeAck(channel string) string {
	return fmt.Sprintf("*3\r\n$9\r\nsubscribe\r\n$%d\r\n%s\r\n:1\r\n", len(channel), channel)
}

func TestStartWaitsForCancellationSubscription(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		subscribed, ack := make(chan struct{}), make(chan struct{})
		rdb := redis.NewClient(&redis.Options{
			Addr: "127.0.0.1:1", MaxRetries: -1, ContextTimeoutEnabled: true, Protocol: 2, DisableIdentity: true,
			Dialer: subscriptionPipe(func(channel string) string {
				close(subscribed)
				<-ack
				return cancelSubscribeAck(channel)
			}),
		})
		defer rdb.Close()
		claimed := make(chan struct{}, 1)
		s, err := NewServer(rdb)
		if err != nil {
			t.Fatal(err)
		}
		s.(*server).claimClient.AddHook(submitHook{process: func(_ context.Context, cmd redis.Cmder) error {
			claimed <- struct{}{}
			cmd.(*redis.Cmd).SetVal([]any{})
			return nil
		}})
		defer s.Shutdown(context.Background())
		if err := s.HandleFunc("job", func(context.Context, *Task) error { return nil }); err != nil {
			t.Fatal(err)
		}
		started := make(chan error, 1)
		go func() { started <- s.Start() }()
		synctest.Wait()
		select {
		case <-subscribed:
		default:
			t.Error("Start did not request a cancellation subscription")
		}
		select {
		case <-claimed:
			t.Error("claimed before subscription confirmation")
		default:
		}
		select {
		case <-started:
			t.Error("Start returned before subscription confirmation")
		default:
		}
		close(ack)
		synctest.Wait()
		if t.Failed() {
			return
		}
		if err := <-started; err != nil {
			t.Fatal(err)
		}
		select {
		case <-claimed:
		default:
			t.Fatal("Claim did not start after subscription confirmation")
		}
	})
}

func readRedisTCPCommand(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	count, err := strconv.Atoi(strings.TrimSpace(line[1:]))
	if err != nil {
		return nil, err
	}
	parts := make([]string, 0, count)
	for range count {
		line, err = reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		n, err := strconv.Atoi(strings.TrimSpace(line[1:]))
		if err != nil {
			return nil, err
		}
		data := make([]byte, n+2)
		if _, err := io.ReadFull(reader, data); err != nil {
			return nil, err
		}
		parts = append(parts, string(data[:n]))
	}
	return parts, nil
}
