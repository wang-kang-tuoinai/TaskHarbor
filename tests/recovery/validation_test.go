package recovery

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAcceptanceRejectsIncompleteOrIncorrectEvidence(t *testing.T) {
	manifest := []expectedTask{{ID: "run-0000", Value: 3, Result: `{"square":9}`}}
	valid := func() snapshot {
		return snapshot{Tasks: map[string]map[string]string{
			"run-0000": {"task_id": "run-0000", "status": "completed", "result": `{"square":9}`, "attempts": "1"},
		}, Pending: []string{}, Active: []string{}, Lease: []string{}, Retry: []string{}}
	}
	for _, tc := range []struct {
		name, want string
		change     func(*snapshot)
	}{
		{"valid", "", func(*snapshot) {}},
		{"missing", "missing", func(s *snapshot) { delete(s.Tasks, "run-0000") }},
		{"wrong-result", "result", func(s *snapshot) { s.Tasks["run-0000"]["result"] = `{"square":8}` }},
		{"not-completed", "status", func(s *snapshot) { s.Tasks["run-0000"]["status"] = "failed" }},
		{"identity-residue", "identity", func(s *snapshot) { s.Tasks["run-0000"]["owner_id"] = "" }},
		{"pending-residue", "pending", func(s *snapshot) { s.Pending = []string{"run-0000"} }},
		{"lease-residue", "lease", func(s *snapshot) { s.Lease = []string{"run-0000", "123"} }},
		{"exhausted", "attempts", func(s *snapshot) { s.Tasks["run-0000"]["attempts"] = "12" }},
		{"baseline-retried", "baseline", func(s *snapshot) { s.Tasks["run-0000"]["attempts"] = "2" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := valid()
			tc.change(&state)
			err := verifyConverged(manifest, state, "baseline")
			if tc.want == "" && err != nil || tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)) {
				t.Fatalf("expected %q, got %v", tc.want, err)
			}
		})
	}
}

func TestAcceptanceRejectsWorkerRaceDiagnostics(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "worker-0.stderr.jsonl")
	if err := os.WriteFile(path, []byte("ordinary worker diagnostic\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := verifyWorkerDiagnostics(directory, 1); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("WARNING: DATA RACE\nRead at 0x123\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := verifyWorkerDiagnostics(directory, 1); err == nil {
		t.Fatal("worker race warning was ignored when the child was forcibly cleaned up")
	}
}
