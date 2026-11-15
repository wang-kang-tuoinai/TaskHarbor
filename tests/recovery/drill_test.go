//go:build recovery

package recovery

import (
	"fmt"
	"testing"
	"time"
)

func TestMultiProcessRecovery(t *testing.T) {
	runDrill(t, []string{"baseline", "worker-crash", "short-network", "long-network", "failures"})
}

func TestPlannedFailureCoverageNeedsRetryAndCompletionEvidence(t *testing.T) {
	fault := faultControl{Scenario: "failures", FailurePaths: make(map[string]map[string]bool)}
	var events []event
	for i := range 100 {
		id := fmt.Sprintf("task-%04d", i)
		fault.FailurePaths[id] = map[string]bool{"retry_wait/1": true, "retry_wait/2": true, "completed/3": true}
		events = append(events, event{Kind: "handler_return", TaskID: id, Attempt: 1, Detail: "planned failure"})
	}
	if err := fault.verify(events); err != nil {
		t.Fatal(err)
	}
	delete(fault.FailurePaths["task-0000"], "retry_wait/1")
	delete(fault.FailurePaths["task-0000"], "retry_wait/2")
	if err := fault.verify(events); err == nil {
		t.Fatal("completed task without observed retry was accepted as failure-path coverage")
	}
}

func TestLongNetworkCoverageRequiresRealOldRequestRejection(t *testing.T) {
	fault := faultControl{Scenario: "long-network", Target: "task", Attempt: 1,
		Started: time.Now().Add(-time.Minute), Ended: time.Now(), Reclaimed: true}
	events := []event{{Kind: "handler_start", TaskID: "task", Attempt: 2, Worker: 1}}
	if err := fault.verify(events); err == nil {
		t.Fatal("long outage passed without a real delayed request and rejection")
	}
}

func TestNetworkCoverageRequiresIsolationAndRecoveryEvidence(t *testing.T) {
	fault := faultControl{Scenario: "short-network", Target: "task", Attempt: 1, Started: time.Now(), Reclaimed: true}
	events := []event{{Kind: "handler_start", TaskID: "task", Attempt: 2, Worker: 1}}
	if err := fault.verify(events); err == nil {
		t.Fatal("network scenario passed with only crash recovery evidence")
	}
}
