package recovery

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

type expectedTask struct {
	ID     string `json:"id"`
	Value  int    `json:"value"`
	Long   bool   `json:"long"`
	Fail   bool   `json:"fail_first_two_attempts"`
	Result string `json:"expected_result"`
}

type snapshot struct {
	RedisTimeMS int64                        `json:"redis_time_ms"`
	Tasks       map[string]map[string]string `json:"tasks"`
	Pending     []string                     `json:"pending"`
	Active      []string                     `json:"active"`
	Lease       []string                     `json:"lease_with_scores"`
	Retry       []string                     `json:"retry_with_scores"`
}

// 同一格式贯穿 Worker、代理和父进程，原始日志另外逐进程保留。
type event struct {
	At      time.Time `json:"at"`
	Kind    string    `json:"kind"`
	Worker  int       `json:"worker"`
	PID     int       `json:"pid,omitempty"`
	TaskID  string    `json:"task_id,omitempty"`
	Attempt int       `json:"attempt,omitempty"`
	Long    bool      `json:"long,omitempty"`
	Detail  string    `json:"detail,omitempty"`
	Timeout bool      `json:"timeout,omitempty"`
}

func verifyConverged(manifest []expectedTask, state snapshot, scenario string) error {
	var problems []error
	ids := make(map[string]bool, len(manifest))
	for _, task := range manifest {
		ids[task.ID] = true
		fields, exists := state.Tasks[task.ID]
		if !exists || len(fields) == 0 {
			problems = append(problems, fmt.Errorf("%s missing", task.ID))
			continue
		}
		if fields["task_id"] != task.ID || fields["status"] != "completed" {
			problems = append(problems, fmt.Errorf("%s identity or status invalid: %q", task.ID, fields["status"]))
		}
		if result, exists := fields["result"]; !exists || result != task.Result {
			problems = append(problems, fmt.Errorf("%s result mismatch: %q", task.ID, result))
		}
		for _, key := range []string{"owner_id", "attempt_token"} {
			if _, exists := fields[key]; exists {
				problems = append(problems, fmt.Errorf("%s identity field remains: %s", task.ID, key))
			}
		}
		attempts, err := strconv.Atoi(fields["attempts"])
		if err != nil || attempts < 1 || attempts > 11 {
			problems = append(problems, fmt.Errorf("%s attempts invalid: %q", task.ID, fields["attempts"]))
		} else if scenario == "baseline" && attempts != 1 {
			problems = append(problems, fmt.Errorf("%s baseline unexpectedly used %d attempts", task.ID, attempts))
		} else if scenario == "failures" && task.Fail && attempts < 3 {
			problems = append(problems, fmt.Errorf("%s failure rule skipped attempts: %d", task.ID, attempts))
		}
	}
	for _, index := range []struct {
		name    string
		members []string
		stride  int
	}{{"pending", state.Pending, 1}, {"active", state.Active, 1}, {"lease", state.Lease, 2}, {"retry", state.Retry, 2}} {
		for i := 0; i < len(index.members); i += index.stride {
			if ids[index.members[i]] {
				problems = append(problems, fmt.Errorf("%s remains in %s", index.members[i], index.name))
			}
		}
	}
	return errors.Join(problems...)
}

func verifyWorkerDiagnostics(directory string, workers int) error {
	for number := range workers {
		path := filepath.Join(directory, fmt.Sprintf("worker-%d.stderr.jsonl", number))
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		// 子进程由父进程强制清理，不能只依赖 race 检测器的最终退出码。
		if bytes.Contains(data, []byte("WARNING: DATA RACE")) {
			return fmt.Errorf("worker %d reported a data race; see %s", number, path)
		}
	}
	return nil
}
