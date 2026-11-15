//go:build recovery

package recovery

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	taskq "github.com/wang-kang-tuoinai/TaskHarbor"
)

type journal struct {
	mu     sync.Mutex
	file   *os.File
	events []event
	err    error
}

func (j *journal) record(e event) {
	if e.At.IsZero() {
		e.At = time.Now().UTC()
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	j.events = append(j.events, e)
	if err := json.NewEncoder(j.file).Encode(e); err != nil && j.err == nil {
		j.err = err
	}
}

func (j *journal) all() []event {
	j.mu.Lock()
	defer j.mu.Unlock()
	return append([]event(nil), j.events...)
}

type childWorker struct {
	command  *exec.Cmd
	done     chan struct{}
	ready    chan struct{}
	stopping atomic.Bool
}

func launchWorker(ctx context.Context, config workerConfig, directory string, events *journal) (*childWorker, error) {
	binary, err := os.Executable()
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(config)
	if err != nil {
		return nil, err
	}
	stdout, err := os.Create(filepath.Join(directory, fmt.Sprintf("worker-%d.jsonl", config.Number)))
	if err != nil {
		return nil, err
	}
	stderr, err := os.Create(filepath.Join(directory, fmt.Sprintf("worker-%d.stderr.jsonl", config.Number)))
	if err != nil {
		stdout.Close()
		return nil, err
	}
	command := exec.CommandContext(ctx, binary, "-test.run=^TestRecoveryWorker$", "-test.timeout=12m")
	command.Env = append(os.Environ(), workerEnvironment+"="+string(encoded))
	command.Stderr = stderr
	pipe, err := command.StdoutPipe()
	if err != nil {
		stdout.Close()
		stderr.Close()
		return nil, err
	}
	if err := command.Start(); err != nil {
		stdout.Close()
		stderr.Close()
		return nil, err
	}
	child := &childWorker{command: command, done: make(chan struct{}), ready: make(chan struct{})}
	go func() {
		defer close(child.done)
		defer stdout.Close()
		defer stderr.Close()
		scanner := bufio.NewScanner(io.TeeReader(pipe, stdout))
		scanner.Buffer(make([]byte, 4096), 1<<20)
		for scanner.Scan() {
			var e event
			if err := json.Unmarshal(scanner.Bytes(), &e); err != nil {
				events.record(event{Kind: "fatal", Worker: config.Number, Detail: "invalid worker event: " + scanner.Text()})
				continue
			}
			events.record(e)
			if e.Kind == "worker_ready" {
				close(child.ready)
			}
		}
		if err := scanner.Err(); err != nil {
			events.record(event{Kind: "fatal", Worker: config.Number, Detail: "worker log read: " + err.Error()})
		}
		err := command.Wait()
		kind := "worker_exit"
		if !child.stopping.Load() {
			kind = "unexpected_worker_exit"
		}
		events.record(event{Kind: kind, Worker: config.Number, PID: command.Process.Pid, Detail: fmt.Sprint(err)})
	}()
	return child, nil
}

func (w *childWorker) kill() error {
	if !w.stopping.CompareAndSwap(false, true) {
		return nil
	}
	err := w.command.Process.Kill()
	if errors.Is(err, os.ErrProcessDone) {
		return nil
	}
	return err
}

func saveJSON(path string, value any) error {
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(encoded, '\n'), 0600)
}

type drillReport struct {
	Scenario        string         `json:"scenario"`
	Started         time.Time      `json:"started"`
	Finished        time.Time      `json:"finished"`
	Confirmed       int            `json:"submit_confirmed"`
	Completed       int            `json:"completed"`
	Attempts        map[int]int    `json:"attempts_distribution"`
	MaximumAttempts int            `json:"maximum_attempts"`
	HandlerStarts   int            `json:"observed_handler_starts"`
	Fatal           []event        `json:"unexpected_fatal_errors"`
	Problems        []string       `json:"problems"`
	Passed          bool           `json:"passed"`
	Evidence        map[string]any `json:"fault_evidence"`
	LogLimit        string         `json:"log_limit"`
}

func runDrill(t *testing.T, scenarios []string) {
	t.Helper()
	runID := time.Now().UTC().Format("20060102t150405") + "-" + strconv.Itoa(os.Getpid())
	directory, err := filepath.Abs(filepath.Join("..", "..", ".scratch", "recovery", runID))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(directory, 0700); err != nil {
		t.Fatal(err)
	}
	network := "taskharbor-drill-" + runID
	if _, err := dockerCommand("network", "create", "--label", "taskharbor.recovery="+runID, network); err != nil {
		t.Fatal(err)
	}
	retained := false
	defer func() {
		if retained {
			t.Logf("retained diagnostic Docker network %s", network)
			return
		}
		if _, err := dockerCommand("network", "rm", network); err != nil {
			t.Error(err)
		}
	}()
	code, err := exec.Command("git", "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatal(err)
	}
	status, err := exec.Command("git", "status", "--porcelain").Output()
	if err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob("*_test.go")
	if err != nil {
		t.Fatal(err)
	}
	checksums := make(map[string]string, len(files))
	for _, name := range files {
		content, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(content)
		checksums[name] = hex.EncodeToString(digest[:])
	}
	dockerVersion, err := dockerCommand("version", "--format", "{{json .}}")
	if err != nil {
		t.Fatal(err)
	}
	metadata := map[string]any{"run_id": runID, "code_version": strings.TrimSpace(string(code)), "go": runtime.Version(),
		"workspace_status": string(status), "test_file_sha256": checksums, "docker": json.RawMessage(dockerVersion),
		"platform": runtime.GOOS + "/" + runtime.GOARCH, "redis_image": redisImage,
		"tasks_per_scenario": 1000, "workers": 3, "concurrency_per_worker": 10,
		"lease_seconds": 30, "renew_seconds": 6, "reclaim_seconds": 10,
		"execution_timeout_seconds": 60, "retry_delay_seconds": 15, "max_retry": 10,
		"deadline_seconds": 600, "dial_timeout_seconds": 1, "read_timeout_seconds": 2,
		"write_timeout_seconds": 2, "pool_timeout_seconds": 2, "pool_size": 32,
		"max_retries": -1, "context_timeout_enabled": true, "proxy": "test-only RESP2 TCP proxy"}
	if err := saveJSON(filepath.Join(directory, "environment.json"), metadata); err != nil {
		t.Fatal(err)
	}
	t.Logf("evidence directory %s", directory)
	for _, scenario := range scenarios {
		if !t.Run(scenario, func(t *testing.T) {
			retained = runScenario(t, network, runID, directory, scenario) || retained
		}) {
			return // 失败证据独立保留，基线失败时不继续故障演练。
		}
	}
}

func runScenario(t *testing.T, network, runID, parentDirectory, scenario string) (keepContainer bool) {
	t.Helper()
	directory := filepath.Join(parentDirectory, scenario)
	if err := os.MkdirAll(directory, 0700); err != nil {
		t.Fatal(err)
	}
	instance, err := startDockerRedis(network, "taskharbor-"+runID+"-"+scenario)
	if instance != nil {
		defer func() {
			logs, logErr := dockerCommand("logs", instance.id)
			if logErr != nil {
				t.Error(logErr)
			}
			if err := os.WriteFile(filepath.Join(directory, "redis.log"), []byte(logs), 0600); err != nil {
				t.Error(err)
			}
			if instance.client != nil {
				if err := instance.client.Close(); err != nil {
					t.Error(err)
				}
			}
			if keepContainer {
				t.Logf("state export failed; retained diagnostic Redis %s on network %s", instance.id, network)
				return
			}
			if _, err := dockerCommand("rm", "--force", "--volumes", instance.id); err != nil {
				t.Error(err)
			}
		}()
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := saveJSON(filepath.Join(directory, "redis.json"), map[string]string{
		"container_id": instance.id, "version": instance.version, "image": redisImage, "address": instance.address,
	}); err != nil {
		t.Fatal(err)
	}
	manifest := make([]expectedTask, 1000)
	for i := range manifest {
		manifest[i] = expectedTask{ID: fmt.Sprintf("%s-%s-%04d", runID, scenario, i), Value: i + 1,
			Long: i%10 == 0, Fail: scenario == "failures" && i%10 == 1, Result: fmt.Sprintf(`{"square":%d}`, (i+1)*(i+1))}
	}
	if err := saveJSON(filepath.Join(directory, "manifest.json"), manifest); err != nil {
		t.Fatal(err)
	}
	file, err := os.Create(filepath.Join(directory, "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	events := &journal{file: file}
	proxy, err := newProxy(instance.address, events.record)
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	processContext, cancelProcesses := context.WithCancel(context.Background())
	defer cancelProcesses()
	var workers []*childWorker
	// 报告先导出快照，再强制清理进程。清理后合并已经排空的日志，避免丢失致命通知。
	report := drillReport{Scenario: scenario, Attempts: make(map[int]int), Evidence: make(map[string]any),
		LogLimit: "Handler logs may be lost on forced termination; attempts in Redis are authoritative."}
	var final snapshot
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		state, err := readSnapshot(ctx, instance.client, manifest)
		if err != nil {
			report.Problems = append(report.Problems, "final snapshot: "+err.Error())
			keepContainer = true
		} else {
			final = state
			if err := saveJSON(filepath.Join(directory, "final-state.json"), final); err != nil {
				report.Problems = append(report.Problems, err.Error())
				keepContainer = true
			}
		}
		if keepContainer {
			report.Evidence["retained_resources"] = map[string]string{"container": instance.id, "network": network,
				"reason": "failure-state export unsuccessful; Redis preserved for diagnosis"}
		}
		for _, child := range workers {
			if err := child.kill(); err != nil {
				report.Problems = append(report.Problems, "process cleanup: "+err.Error())
			}
		}
		for _, child := range workers {
			select {
			case <-child.done:
			case <-time.After(5 * time.Second):
				report.Problems = append(report.Problems, "worker cleanup did not finish")
			}
		}
		if err := verifyWorkerDiagnostics(directory, len(workers)); err != nil {
			report.Problems = append(report.Problems, err.Error())
		}
		for _, e := range events.all() {
			switch e.Kind {
			case "handler_start":
				report.HandlerStarts++
			case "submit_confirmed":
				report.Confirmed++
			case "fatal", "unexpected_worker_exit":
				report.Fatal = append(report.Fatal, e)
			}
		}
		for _, task := range manifest {
			fields := final.Tasks[task.ID]
			if fields["status"] == "completed" {
				report.Completed++
			}
			if attempts, err := strconv.Atoi(fields["attempts"]); err == nil {
				report.Attempts[attempts]++
				report.MaximumAttempts = max(report.MaximumAttempts, attempts)
			}
		}
		if report.Confirmed != len(manifest) {
			report.Problems = append(report.Problems, fmt.Sprintf("submit confirmation count=%d want=%d", report.Confirmed, len(manifest)))
		}
		if err := verifyConverged(manifest, final, scenario); err != nil {
			report.Problems = append(report.Problems, err.Error())
		}
		if len(report.Fatal) != 0 {
			report.Problems = append(report.Problems, "unexpected fatal error or worker exit")
		}
		events.mu.Lock()
		if events.err != nil {
			report.Problems = append(report.Problems, "event export: "+events.err.Error())
		}
		events.mu.Unlock()
		report.Finished = time.Now().UTC()
		report.Passed = len(report.Problems) == 0
		if err := saveJSON(filepath.Join(directory, "report.json"), report); err != nil {
			t.Error(err)
		}
		if !report.Passed {
			t.Errorf("scenario failed; see %s/report.json", directory)
		} else {
			t.Logf("completed=%d confirmed=%d attempts=%v handler starts=%d", report.Completed, report.Confirmed, report.Attempts, report.HandlerStarts)
		}
	}()
	for number := range 3 {
		address := instance.address
		if number == 0 {
			address = proxy.Addr()
		}
		child, err := launchWorker(processContext, workerConfig{Address: address, Number: number}, directory, events)
		if err != nil {
			report.Problems = append(report.Problems, err.Error())
			return
		}
		workers = append(workers, child)
		select {
		case <-child.ready:
		case <-child.done:
			report.Problems = append(report.Problems, "worker exited before ready")
			return
		case <-time.After(15 * time.Second):
			report.Problems = append(report.Problems, "worker did not become ready")
			return
		}
	}
	report.Started = time.Now().UTC()
	ctx, cancel := context.WithDeadline(context.Background(), report.Started.Add(10*time.Minute))
	defer cancel()
	submitted := make(chan error, 1)
	go func() {
		err := submitManifest(ctx, instance.client, manifest, events)
		if err != nil {
			events.record(event{Kind: "fatal", Worker: -1, Detail: "submission failed: " + err.Error()})
		}
		submitted <- err
	}()
	defer func() {
		cancel()
		if err := <-submitted; err != nil {
			report.Problems = append(report.Problems, "submission: "+err.Error())
		}
	}()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	fault := &faultControl{Scenario: scenario}
	report.Evidence["fault"] = fault
	for {
		select {
		case <-ctx.Done():
			report.Problems = append(report.Problems, "10 minute deadline reached")
			return
		case <-ticker.C:
		}
		for _, e := range events.all() {
			if e.Kind == "fatal" || e.Kind == "unexpected_worker_exit" {
				report.Problems = append(report.Problems, fmt.Sprintf("worker %d: %s", e.Worker, e.Detail))
				return
			}
		}
		state, err := readSnapshot(ctx, instance.client, manifest)
		if err != nil {
			report.Problems = append(report.Problems, err.Error())
			return
		}
		final = state
		if err := fault.advance(ctx, state, events, workers, proxy, instance.client); err != nil {
			report.Problems = append(report.Problems, err.Error())
			return
		}
		completed := 0
		for _, task := range manifest {
			if state.Tasks[task.ID]["status"] == "completed" {
				completed++
			}
		}
		if err := saveJSON(filepath.Join(directory, "progress.json"), map[string]any{"completed": completed, "elapsed_seconds": time.Since(report.Started).Seconds()}); err != nil {
			report.Problems = append(report.Problems, err.Error())
			return
		}
		if completed == len(manifest) {
			if err := fault.verify(events.all()); err != nil {
				report.Problems = append(report.Problems, err.Error())
			}
			return
		}
	}
}

func submitManifest(ctx context.Context, rdb *redis.Client, manifest []expectedTask, events *journal) error {
	client, err := taskq.NewClient(rdb)
	if err != nil {
		return err
	}
	for _, task := range manifest {
		payload, err := json.Marshal(workload{Value: task.Value, Long: task.Long, Fail: task.Fail})
		if err != nil {
			return err
		}
		for {
			_, err := client.Submit(ctx, task.ID, "drill", payload, taskq.WithSubmitMaxRetry(10),
				taskq.WithSubmitExecutionTimeout(60*time.Second), taskq.WithSubmitRetryDelay(15*time.Second))
			if err == nil {
				events.record(event{Kind: "submit_confirmed", Worker: -1, TaskID: task.ID})
				break
			}
			if errors.Is(err, taskq.ErrInvalidArgument) || errors.Is(err, taskq.ErrTaskIDConflict) || errors.Is(err, taskq.ErrStorageData) {
				return err
			}
			events.record(event{Kind: "submit_unconfirmed", Worker: -1, TaskID: task.ID, Detail: err.Error()})
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
	return nil
}
