package taskq

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
	"unicode/utf8"

	"github.com/redis/go-redis/v9"
)

func TestReportingRetriesKeepResultAndEligibility(t *testing.T) {
	for _, failure := range []bool{false, true} {
		t.Run(map[bool]string{false: "complete", true: "failure"}[failure], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := newTestServer(t, WithConcurrency(1))
				key := executionKey{"task", "token"}
				started := time.Now()
				var calls []time.Duration
				var callsMu sync.Mutex
				var arguments [][]any
				s.reportClient.AddHook(submitHook{process: func(ctx context.Context, cmd redis.Cmder) error {
					callsMu.Lock()
					defer callsMu.Unlock()
					calls = append(calls, time.Since(started))
					args := cmd.Args()
					arguments = append(arguments, append([]any(nil), args[len(args)-3:]...))
					deadline, ok := ctx.Deadline()
					if !ok || deadline.Sub(time.Now()) != 3*time.Second {
						t.Errorf("request deadline=%v", deadline)
					}
					if _, ok := s.getExecution(key); !ok {
						t.Error("normal reporting lost renew eligibility")
					}
					switch len(calls) {
					case 1:
						return &net.DNSError{Err: "lookup timeout", IsTimeout: true}
					case 2:
						return &net.DNSError{Err: "lookup unavailable", IsTemporary: true}
					}
					cmd.(*redis.Cmd).SetVal(int64(1))
					return nil
				}})
				if err := s.HandleFunc("job", func(_ context.Context, task *Task) error {
					if failure {
						return errors.New("fixed error")
					}
					return task.SetResult("fixed result")
				}); err != nil {
					t.Fatal(err)
				}
				s.slots <- struct{}{}
				done, err := s.acceptExecution(context.Background(), &Task{ID: "task", Type: "job"}, key, 50*time.Millisecond)
				if err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
				callsMu.Lock()
				initialCalls := len(calls)
				callsMu.Unlock()
				if len(s.slots) != 0 || initialCalls != 1 {
					t.Fatalf("slot=%d calls=%d", len(s.slots), initialCalls)
				}
				time.Sleep(300 * time.Millisecond)
				synctest.Wait()
				<-done
				if !reflect.DeepEqual(calls, []time.Duration{0, 100 * time.Millisecond, 300 * time.Millisecond}) {
					t.Fatalf("request times=%v", calls)
				}
				if !reflect.DeepEqual(arguments[0], arguments[1]) || !reflect.DeepEqual(arguments[1], arguments[2]) {
					t.Fatalf("changed arguments=%v", arguments)
				}
				if len(s.snapshotExecutions()) != 0 || len(s.Errors()) != 0 {
					t.Fatal("report cleanup or transient failure scope incorrect")
				}
			})
		})
	}
}

type reportingMessageError struct {
	text  string
	calls int
}

func (e *reportingMessageError) Error() string { e.calls++; return e.text }

func TestReportingFailureMessage(t *testing.T) {
	for _, message := range []string{"", strings.Repeat("a", 4096), strings.Repeat("中", 2000)} {
		t.Run(errorMessageCaseName(message), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := newTestServer(t)
				cause := &reportingMessageError{text: message}
				calls := 0
				var first string
				s.reportClient.AddHook(submitHook{process: func(_ context.Context, cmd redis.Cmder) error {
					calls++
					args := cmd.Args()
					got := args[len(args)-1].(string)
					if calls == 1 {
						first = got
					} else if got != first {
						t.Error("failure message changed on retry")
					}
					if got == "" || len(got) > 4096 || !utf8.ValidString(got) {
						t.Errorf("invalid normalized message bytes=%d", len(got))
					}
					if message == "" && got != "Handler 返回了空错误信息" {
						t.Errorf("empty diagnostic=%q", got)
					}
					if len(message) == 4096 && got != message {
						t.Error("boundary message changed")
					}
					if len(message) > 4096 && (!strings.HasSuffix(got, "...[truncated]") || !strings.HasPrefix(message, strings.TrimSuffix(got, "...[truncated]"))) {
						t.Error("missing UTF-8 truncation marker or prefix")
					}
					return io.EOF
				}})
				if err := s.HandleFunc("job", func(context.Context, *Task) error { return cause }); err != nil {
					t.Fatal(err)
				}
				s.slots <- struct{}{}
				done, err := s.acceptExecution(context.Background(), &Task{Type: "job"}, executionKey{"task", "token"}, time.Second)
				if err != nil {
					t.Fatal(err)
				}
				<-done
				if calls != 3 || cause.calls != 1 || len(s.Errors()) != 0 {
					t.Fatalf("calls=%d error evaluations=%d fatal=%d", calls, cause.calls, len(s.Errors()))
				}
			})
		})
	}
}

func errorMessageCaseName(message string) string {
	if message == "" {
		return "empty"
	}
	if len(message) == 4096 {
		return "boundary"
	}
	return "UTF8-truncated"
}

func TestExecutionSelectedReportPropagatesVisibleParentCause(t *testing.T) {
	for _, tc := range []struct {
		name   string
		result executionResult
	}{
		{"result", executionResult{result: []byte(`"ok"`)}},
		{"timeout", executionResult{err: context.DeadlineExceeded}},
		{"unknown-handler", executionResult{err: errors.New("no handler registered")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestServer(t)
			parent, cancelParent := context.WithCancelCause(context.Background())
			defer cancelParent(nil)
			handlerCtx, cancelHandler := context.WithCancelCause(parent)
			defer cancelHandler(nil)
			reportCtx, cancelReports := context.WithCancel(context.Background())
			defer cancelReports()
			record := &execution{reportCtx: reportCtx, cancelReports: cancelReports}
			s.onComplete = func(context.Context, *execution, executionKey, []byte) { t.Error("completed after parent revoked") }
			s.onFailure = func(context.Context, *execution, executionKey, error) {
				t.Error("reported failure after parent revoked")
			}

			// 经过已选定分支的共同入口，不依赖 select 选中哪个就绪信号。
			// 此时还没有上报的 AfterFunc，关闭 reportCtx 必须由调用边界同步完成。
			cancelParent(fmt.Errorf("parent revoked: %w", errExecutionCanceled))
			if tc.name == "timeout" {
				cancelHandler(context.DeadlineExceeded)
			}
			s.reportExecutionResult(handlerCtx, record, executionKey{"task", "token"}, tc.result)
			if reportCtx.Err() != context.Canceled {
				t.Fatal("visible parent cause did not synchronously stop reporting")
			}
		})
	}
}

func TestReportingUnregistersParentCancellationAfterCompletion(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newTestServer(t)
		parent, cancelParent := context.WithCancelCause(context.Background())
		defer cancelParent(nil)
		handlerCtx, cancelHandler := context.WithCancelCause(parent)
		defer cancelHandler(nil)
		reportCtx, cancelReports := context.WithCancel(context.Background())
		defer cancelReports()
		record := &execution{reportCtx: reportCtx, cancelReports: cancelReports}
		calls := 0
		s.reportClient.AddHook(submitHook{process: func(_ context.Context, cmd redis.Cmder) error {
			calls++
			cmd.(*redis.Cmd).SetVal(int64(1))
			return nil
		}})
		s.complete(handlerCtx, record, executionKey{"task", "token"}, nil)
		if calls != 1 || handlerCtx.Err() != nil {
			t.Fatalf("completion did not finish with a live handler context: calls=%d, cause=%v", calls, context.Cause(handlerCtx))
		}

		// 保持上报 context 可观察。父取消确实传播给 Handler，但已结束的上报不再订阅它。
		cancelParent(errExecutionLost)
		synctest.Wait()
		if context.Cause(handlerCtx) != errExecutionLost || reportCtx.Err() != nil {
			t.Fatal("completed report retained its parent cancellation callback")
		}
	})
}

func TestReportingCancellationStopsNextRequest(t *testing.T) {
	for _, mode := range []string{"lost", "canceled", "inconsistent", "parent", "abort", "timeout-then-lost", "inflight-lost"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := newTestServer(t, WithConcurrency(1))
				key := executionKey{"task", "token"}
				parent, cancelParent := context.WithCancelCause(context.Background())
				defer cancelParent(nil)
				release := make(chan struct{})
				inflight := make(chan struct{})
				allowReply := make(chan struct{})
				var handlerCtx context.Context
				calls := 0
				s.reportClient.AddHook(submitHook{process: func(ctx context.Context, _ redis.Cmder) error {
					calls++
					if mode == "inflight-lost" {
						close(inflight)
						<-allowReply
						return ctx.Err()
					}
					return io.EOF
				}})
				if err := s.HandleFunc("job", func(ctx context.Context, _ *Task) error {
					if mode == "timeout-then-lost" {
						handlerCtx = ctx
						<-release
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				s.slots <- struct{}{}
				done, err := s.acceptExecution(parent, &Task{Type: "job"}, key, time.Second)
				if err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
				record, ok := s.getExecution(key)
				if !ok {
					t.Fatal("missing managed execution")
				}
				if mode == "timeout-then-lost" {
					time.Sleep(time.Second)
					synctest.Wait()
					// 本地超时已取消子 context。父取消不能覆盖首个原因，须显式撤销。
					cancelParent(errExecutionLost)
					synctest.Wait()
					if context.Cause(handlerCtx) != context.DeadlineExceeded || record.reportCtx.Err() != nil {
						t.Fatal("late parent cancellation replaced the first handler cause or stopped the timeout report")
					}
				}
				if mode == "inflight-lost" {
					<-inflight
				}
				switch mode {
				case "abort":
					close(s.abort)
				case "parent":
					cancelParent(errExecutionCanceled)
				case "canceled":
					s.revokeExecution(key, record, errExecutionCanceled)
				case "inconsistent":
					s.revokeExecution(key, record, errExecutionInconsistent)
				case "timeout-then-lost":
					record.cancel(fmt.Errorf("late revocation: %w", errExecutionLost))
					record.cancel(errExecutionLost)
					if context.Cause(handlerCtx) != context.DeadlineExceeded || record.reportCtx.Err() != context.Canceled {
						t.Fatal("explicit revocation did not stop reporting independently of the first handler cause")
					}
				default:
					s.revokeExecution(key, record, errExecutionLost)
				}
				if mode == "inflight-lost" {
					close(allowReply)
				}
				synctest.Wait()
				select {
				case <-done:
				default:
					t.Fatal("cancellation did not interrupt reporting wait")
				}
				if calls != 1 || len(s.Errors()) != 0 {
					t.Fatalf("requests=%d fatals=%d", calls, len(s.Errors()))
				}
				if mode == "timeout-then-lost" && len(s.slots) != 1 {
					t.Fatal("live handler slot released during timeout report")
				}
				close(release)
				synctest.Wait()
				if len(s.slots) != 0 || len(s.snapshotExecutions()) != 0 {
					t.Fatal("report resources leaked")
				}
			})
		})
	}
}

func TestReportingOutcomesAndDiagnostics(t *testing.T) {
	for _, mode := range []string{"success", "rejected", "unknown-rejected", "exhausted", "permission", "invalid", "inconsistent", "dns-not-found", "address", "invalid-address"} {
		t.Run(mode, func(t *testing.T) {
			var output bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug})))
			defer slog.SetDefault(previous)
			synctest.Test(t, func(t *testing.T) {
				s := newTestServer(t)
				calls := 0
				s.reportClient.AddHook(submitHook{process: func(_ context.Context, cmd redis.Cmder) error {
					calls++
					switch mode {
					case "exhausted":
						return io.EOF
					case "permission":
						return startRedisError("NOPERM forbidden")
					case "dns-not-found":
						return &net.DNSError{Err: "no such host", Name: "invalid.example", IsNotFound: true}
					case "address":
						return &net.AddrError{Err: "missing port", Addr: "invalid"}
					case "invalid-address":
						return fmt.Errorf("dial Redis: %w", net.InvalidAddrError("invalid address"))
					case "invalid":
						cmd.(*redis.Cmd).SetVal("bad")
						return nil
					case "inconsistent":
						return startRedisError("TASKQ_REPORT_INCONSISTENT task task: missing lease")
					case "unknown-rejected":
						if calls == 1 {
							return io.EOF
						}
					}
					value := int64(0)
					if mode == "success" {
						value = 1
					}
					cmd.(*redis.Cmd).SetVal(value)
					return nil
				}})
				if err := s.HandleFunc("job", func(_ context.Context, task *Task) error { return task.SetResult("private-business-result") }); err != nil {
					t.Fatal(err)
				}
				s.slots <- struct{}{}
				done, err := s.acceptExecution(context.Background(), &Task{Type: "job", Payload: []byte("private-payload")}, executionKey{"task", "token"}, time.Second)
				if err != nil {
					t.Fatal(err)
				}
				<-done
				wantCalls := 1
				wantLevel := "DEBUG"
				fatal := mode == "permission" || mode == "invalid" || mode == "inconsistent" || mode == "dns-not-found" || mode == "address" || mode == "invalid-address"
				if mode == "unknown-rejected" {
					wantCalls = 2
					wantLevel = "WARN"
				}
				if mode == "exhausted" {
					wantCalls = 3
					wantLevel = "WARN"
				}
				if fatal {
					wantLevel = "ERROR"
				}
				if calls != wantCalls || (s.claimCtx.Err() != nil) != fatal || (len(s.Errors()) == 1) != fatal {
					t.Fatalf("calls=%d stopped=%v fatals=%d", calls, s.claimCtx.Err(), len(s.Errors()))
				}
				if mode == "success" {
					if output.Len() != 0 {
						t.Fatalf("unexpected success log %s", output.String())
					}
					return
				}
				decoder := json.NewDecoder(&output)
				var entry map[string]any
				if err := decoder.Decode(&entry); err != nil {
					t.Fatal(err)
				}
				if entry["level"] != wantLevel || entry["operation"] != "complete" || entry["task_id"] != "task" || entry["attempts"] != float64(calls) {
					t.Fatalf("log=%v", entry)
				}
				var extra map[string]any
				if decoder.Decode(&extra) != io.EOF {
					t.Fatal("duplicate diagnostic logs")
				}
				for _, secret := range []string{"private-business-result", "private-payload"} {
					if strings.Contains(fmt.Sprint(entry), secret) {
						t.Fatal("business data logged")
					}
				}
				if len(s.snapshotExecutions()) != 0 || len(s.slots) != 0 {
					t.Fatal("report did not clean up")
				}
			})
		})
	}
}

func TestReportingRequestBudgets(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newTestServer(t, WithConcurrency(1))
		began := time.Now()
		var calls []time.Duration
		s.reportClient.AddHook(submitHook{process: func(ctx context.Context, _ redis.Cmder) error {
			calls = append(calls, time.Since(began))
			<-ctx.Done()
			return ctx.Err()
		}})
		if err := s.HandleFunc("job", func(context.Context, *Task) error { return nil }); err != nil {
			t.Fatal(err)
		}
		s.slots <- struct{}{}
		done, err := s.acceptExecution(context.Background(), &Task{Type: "job"}, executionKey{"task", "token"}, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		<-done
		if time.Since(began) != 9300*time.Millisecond || !reflect.DeepEqual(calls, []time.Duration{0, 3100 * time.Millisecond, 6300 * time.Millisecond}) {
			t.Fatalf("elapsed=%v calls=%v", time.Since(began), calls)
		}
		if len(s.Errors()) != 0 || s.claimCtx.Err() != nil || len(s.snapshotExecutions()) != 0 || len(s.slots) != 0 {
			t.Fatal("network exhaustion leaked or stopped claiming")
		}
	})
}

func TestReportingAfterGlobalRenewFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newTestServer(t)
		release := make(chan struct{})
		calls := 0
		s.reportClient.AddHook(submitHook{process: func(ctx context.Context, cmd redis.Cmder) error {
			calls++
			if ctx.Err() != nil {
				t.Fatal("report inherited stopped loop context")
			}
			cmd.(*redis.Cmd).SetVal(int64(1))
			return nil
		}})
		if err := s.HandleFunc("job", func(context.Context, *Task) error { <-release; return nil }); err != nil {
			t.Fatal(err)
		}
		s.slots <- struct{}{}
		done, err := s.acceptExecution(context.Background(), &Task{Type: "job"}, executionKey{"task", "token"}, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		s.cancelRenew()
		s.reportFatal(errors.New("global Renew fault"))
		close(release)
		<-done
		if calls != 1 {
			t.Fatalf("report calls=%d", calls)
		}
	})
}

func TestReportingAndBackgroundFatalNotifyOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		claimReady := make(chan struct{})
		renewReady := make(chan struct{})
		reportReady := make(chan struct{})
		allowFatal := make(chan struct{})
		s := newStartServer(t, func(_ context.Context, cmd redis.Cmder) error {
			if isRenewCommand(cmd) {
				close(renewReady)
				<-allowFatal
				return startRedisError("ERR renew fatal")
			}
			close(claimReady)
			<-allowFatal
			return startRedisError("NOPERM claim fatal")
		}, WithConcurrency(2), WithLeaseDuration(6*time.Second), WithRenewInterval(2*time.Second))
		s.onComplete = s.complete
		s.onFailure = s.reportFailure
		if err := s.HandleFunc("job", func(context.Context, *Task) error { return nil }); err != nil {
			t.Fatal(err)
		}
		s.reportClient.AddHook(submitHook{process: func(context.Context, redis.Cmder) error {
			close(reportReady)
			<-allowFatal
			return startRedisError("NOPERM report fatal")
		}})
		s.slots <- struct{}{}
		done, err := s.acceptExecution(context.Background(), &Task{Type: "job"}, executionKey{"task", "token"}, 10*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Start(); err != nil {
			t.Fatal(err)
		}
		<-claimReady
		<-reportReady
		<-renewReady
		close(allowFatal)
		<-done
		<-s.claimDone
		<-s.renewDone
		if len(s.Errors()) != 1 {
			t.Fatalf("fatal notifications=%d", len(s.Errors()))
		}
		<-s.Errors()
		select {
		case <-s.Errors():
			t.Fatal("extra fatal notification or closed channel")
		default:
		}
	})
}
