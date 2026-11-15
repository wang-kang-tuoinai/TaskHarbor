//go:build recovery

package recovery

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type targetObservation struct {
	RedisTimeMS int64             `json:"redis_time_ms"`
	Fields      map[string]string `json:"fields"`
	LeaseScore  string            `json:"lease_score,omitempty"`
}

type faultControl struct {
	Scenario     string                     `json:"scenario"`
	Target       string                     `json:"target,omitempty"`
	Token        string                     `json:"old_token,omitempty"`
	Attempt      int                        `json:"old_attempt,omitempty"`
	Started      time.Time                  `json:"started,omitempty"`
	Ended        time.Time                  `json:"ended,omitempty"`
	LeaseAtFault string                     `json:"lease_at_fault,omitempty"`
	Reclaimed    bool                       `json:"lease_reclaim_observed"`
	Delayed      *heldCommand               `json:"actual_delayed_request,omitempty"`
	Unchanged    bool                       `json:"new_result_unchanged_after_delayed_request"`
	Observations []targetObservation        `json:"target_observations,omitempty"`
	FailurePaths map[string]map[string]bool `json:"planned_failure_paths,omitempty"`
}

func targetState(state snapshot, id string) targetObservation {
	observation := targetObservation{RedisTimeMS: state.RedisTimeMS, Fields: state.Tasks[id]}
	for i := 0; i < len(state.Lease); i += 2 {
		if state.Lease[i] == id {
			observation.LeaseScore = state.Lease[i+1]
			break
		}
	}
	return observation
}

func (f *faultControl) advance(ctx context.Context, state snapshot, events *journal, workers []*childWorker, proxy *networkProxy, rdb *redis.Client) error {
	if f.Scenario == "baseline" {
		return nil
	}
	if f.Scenario == "failures" {
		if f.FailurePaths == nil {
			f.FailurePaths = make(map[string]map[string]bool)
		}
		for id, fields := range state.Tasks {
			if fields["last_error"] == "drill planned failure in attempt <=2" || f.FailurePaths[id] != nil {
				if f.FailurePaths[id] == nil {
					f.FailurePaths[id] = make(map[string]bool)
				}
				f.FailurePaths[id][fields["status"]+"/"+fields["attempts"]] = true
			}
		}
		return nil
	}
	if f.Target == "" {
		for _, e := range events.all() {
			fields := state.Tasks[e.TaskID]
			if e.Kind != "handler_start" || e.Worker != 0 || !e.Long || fields["status"] != "running" || fields["attempts"] != strconv.Itoa(e.Attempt) {
				continue
			}
			f.Target, f.Token, f.Attempt = e.TaskID, fields["attempt_token"], e.Attempt
			f.LeaseAtFault = targetState(state, f.Target).LeaseScore
			if f.Token == "" || f.LeaseAtFault == "" {
				return errors.New("fault target has no current execution identity or lease")
			}
			switch f.Scenario {
			case "worker-crash":
				f.Started = time.Now().UTC()
				events.record(event{Kind: "fault_started", Worker: 0, TaskID: f.Target, Attempt: f.Attempt, Detail: "forced process termination"})
				if err := workers[0].kill(); err != nil {
					return err
				}
				f.Ended = time.Now().UTC()
			case "short-network":
				proxy.Isolate("cut", f.Target)
				f.Started = time.Now().UTC()
				events.record(event{Kind: "fault_started", Worker: 0, TaskID: f.Target, Attempt: f.Attempt, Detail: "cut existing and new connections"})
			case "long-network":
				// 先由 Handler 启动事件布置屏障，再让真实成功请求触发隔离。
				// 这样捕获的旧请求确实来自平台，且从未被 Redis 执行。
				proxy.ArmLongOutage(f.Target)
				events.record(event{Kind: "outage_armed", Worker: 0, TaskID: f.Target, Attempt: f.Attempt})
			}
			break
		}
	}
	if f.Scenario == "long-network" && f.Target != "" && f.Started.IsZero() {
		for _, e := range events.all() {
			if e.Kind == "fault_started" && e.TaskID == f.Target {
				f.Started = e.At
				f.LeaseAtFault = targetState(state, f.Target).LeaseScore
				break
			}
		}
	}
	if f.Target != "" {
		observation := targetState(state, f.Target)
		f.Observations = append(f.Observations, observation)
		if observation.Fields["last_error"] == "lease expired" {
			failedAt, err := strconv.ParseInt(observation.Fields["last_failed_at"], 10, 64)
			lease, leaseErr := strconv.ParseInt(f.LeaseAtFault, 10, 64)
			if err == nil && leaseErr == nil && failedAt >= lease && observation.RedisTimeMS >= lease {
				f.Reclaimed = true
			}
		}
	}
	duration := 5 * time.Second
	if f.Scenario == "long-network" {
		duration = 45 * time.Second
	}
	if (f.Scenario == "short-network" || f.Scenario == "long-network") && !f.Started.IsZero() && f.Ended.IsZero() && time.Since(f.Started) >= duration {
		proxy.Restore()
		f.Ended = time.Now().UTC()
		events.record(event{Kind: "network_restored", Worker: 0, TaskID: f.Target, Detail: "target path restored"})
	}
	if f.Scenario == "long-network" && !f.Ended.IsZero() && f.Delayed == nil {
		fields := state.Tasks[f.Target]
		attempts, err := strconv.Atoi(fields["attempts"])
		if err == nil && attempts > f.Attempt && fields["status"] == "completed" {
			request, cancel := context.WithTimeout(ctx, 3*time.Second)
			held, err := proxy.DeliverHeld(request)
			cancel()
			f.Delayed = &held
			if err != nil {
				return err
			}
			after, err := readSnapshot(ctx, rdb, []expectedTask{{ID: f.Target}})
			if err != nil {
				return err
			}
			f.Unchanged = reflect.DeepEqual(fields, after.Tasks[f.Target])
			if !f.Unchanged {
				return errors.New("delayed old report changed the completed task")
			}
			events.record(event{Kind: "old_report_rejected", Worker: 0, TaskID: f.Target, Detail: held.Reply})
		}
	}
	return nil
}

func (f *faultControl) verify(events []event) error {
	if f.Scenario == "baseline" {
		return nil
	}
	if f.Scenario == "failures" {
		if len(f.FailurePaths) != 100 {
			return fmt.Errorf("observed %d planned-failure paths, expected 100", len(f.FailurePaths))
		}
		for id, states := range f.FailurePaths {
			retried, completed, returnedFailure := false, false, false
			for state := range states {
				retried = retried || strings.HasPrefix(state, "retry_wait/")
				completed = completed || strings.HasPrefix(state, "completed/")
			}
			for _, e := range events {
				returnedFailure = returnedFailure || e.Kind == "handler_return" && e.TaskID == id && e.Attempt <= 2 && e.Detail == "planned failure"
			}
			if !retried || !completed || !returnedFailure {
				return fmt.Errorf("planned failure %s lacks observed failure/retry/completion evidence", id)
			}
		}
		return nil
	}
	if f.Scenario == "short-network" || f.Scenario == "long-network" {
		duration := 5 * time.Second
		if f.Scenario == "long-network" {
			duration = 45 * time.Second
		}
		if f.Started.IsZero() || f.Ended.IsZero() || f.Ended.Sub(f.Started) < duration {
			return errors.New("network isolation or restoration was not observed")
		}
		blocked, networkError, resumed, healthy, timeout := false, false, false, false, false
		for _, e := range events {
			inFault := !e.At.Before(f.Started) && !e.At.After(f.Ended)
			blocked = blocked || inFault && e.Kind == "proxy_blocked"
			networkError = networkError || inFault && e.Kind == "redis_network_error" && e.Worker == 0
			timeout = timeout || inFault && e.Kind == "redis_network_error" && e.Worker == 0 && e.Timeout
			resumed = resumed || e.At.After(f.Ended) && e.Kind == "handler_start" && e.Worker == 0
			healthy = healthy || inFault && e.Kind == "handler_return" && e.Worker != 0
		}
		if !blocked || !networkError || !resumed || !healthy {
			return fmt.Errorf("network coverage incomplete: blocked=%v error=%v resumed=%v healthy=%v", blocked, networkError, resumed, healthy)
		}
		if f.Scenario == "short-network" {
			return nil
		}
		if !timeout || f.Delayed == nil || f.Delayed.Token != f.Token || f.Delayed.Reply != ":0\r\n" || !f.Unchanged {
			return errors.New("long outage missing real timeout or actual old report rejection")
		}
	}
	if f.Target == "" || f.Started.IsZero() || !f.Reclaimed {
		return errors.New("fault did not cover a real lease expiry and reclaim")
	}
	for _, e := range events {
		if e.Kind == "handler_start" && e.TaskID == f.Target && e.Attempt > f.Attempt && (f.Scenario != "worker-crash" || e.Worker != 0) {
			return nil
		}
	}
	return fmt.Errorf("task %s was not restarted by another worker", f.Target)
}
