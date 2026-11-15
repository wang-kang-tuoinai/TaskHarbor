//go:build recovery

package recovery

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// 固定本地已核验的镜像摘要，避免浮动标签使不同轮次更换 Redis。
const redisImage = "redis@sha256:b2b95679e3b46fb51864949ed25ea976fc3a6bcc00a40a1bc00d568cb2822e50"

func dockerCommand(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		return string(output), fmt.Errorf("docker %s: %w: %s", strings.Join(args, " "), err, output)
	}
	return strings.TrimSpace(string(output)), nil
}

type dockerRedis struct {
	id      string
	client  *redis.Client
	address string
	version string
}

func startDockerRedis(network, name string) (*dockerRedis, error) {
	id, err := dockerCommand("run", "--pull=never", "--detach", "--name", name,
		"--label", "taskharbor.recovery="+name, "--network", network,
		"--publish", "127.0.0.1::6379", redisImage, "redis-server", "--save", "", "--appendonly", "no")
	if err != nil {
		return nil, err
	}
	instance := &dockerRedis{id: id}
	port, err := dockerCommand("port", id, "6379/tcp")
	if err != nil {
		return instance, err
	}
	instance.address = port
	instance.client = redis.NewClient(&redis.Options{Addr: port, MaxRetries: -1, ContextTimeoutEnabled: true,
		Protocol: 2, DisableIdentity: true, DialTimeout: time.Second, ReadTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for {
		if err := instance.client.Ping(ctx).Err(); err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return instance, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	info, err := instance.client.Info(ctx, "server").Result()
	if err != nil {
		return instance, err
	}
	for _, line := range strings.Split(info, "\n") {
		if strings.HasPrefix(line, "redis_version:") {
			instance.version = strings.TrimSpace(strings.TrimPrefix(line, "redis_version:"))
		}
	}
	if instance.version == "" {
		return instance, errors.New("Redis version missing from INFO")
	}
	return instance, nil
}

// EVAL_RO 将观察器限制为只读；每次结果来自一个一致的 Redis 快照。
const snapshotSource = `
local now = redis.call('TIME')
local tasks = {}
for i = 5, #KEYS do
    tasks[#tasks + 1] = {string.sub(KEYS[i], 12), redis.call('HGETALL', KEYS[i])}
end
return {now, redis.call('LRANGE', KEYS[1], 0, -1), redis.call('SMEMBERS', KEYS[2]),
    redis.call('ZRANGE', KEYS[3], 0, -1, 'WITHSCORES'),
    redis.call('ZRANGE', KEYS[4], 0, -1, 'WITHSCORES'), tasks}
`

func readSnapshot(ctx context.Context, rdb *redis.Client, manifest []expectedTask) (snapshot, error) {
	keys := []string{"taskq:default:pending", "taskq:default:active", "taskq:default:lease", "taskq:default:retry"}
	for _, task := range manifest {
		keys = append(keys, "taskq:meta:"+task.ID)
	}
	rows, err := rdb.EvalRO(ctx, snapshotSource, keys).Slice()
	if err != nil {
		return snapshot{}, err
	}
	if len(rows) != 6 {
		return snapshot{}, errors.New("observer snapshot has an invalid shape")
	}
	stringsFrom := func(value any) ([]string, error) {
		values, ok := value.([]any)
		if !ok {
			return nil, errors.New("observer expected an array")
		}
		result := make([]string, len(values))
		for i, item := range values {
			text, ok := item.(string)
			if !ok {
				return nil, errors.New("observer expected string elements")
			}
			result[i] = text
		}
		return result, nil
	}
	var state snapshot
	clock, err := stringsFrom(rows[0])
	if err != nil || len(clock) != 2 {
		return state, errors.New("observer received an invalid Redis TIME")
	}
	seconds, err := strconv.ParseInt(clock[0], 10, 64)
	if err != nil {
		return state, err
	}
	microseconds, err := strconv.ParseInt(clock[1], 10, 64)
	if err != nil {
		return state, err
	}
	state.RedisTimeMS = seconds*1000 + microseconds/1000
	for i, target := range []*[]string{&state.Pending, &state.Active, &state.Lease, &state.Retry} {
		*target, err = stringsFrom(rows[i+1])
		if err != nil {
			return state, err
		}
	}
	tasks, ok := rows[5].([]any)
	if !ok {
		return state, errors.New("observer tasks are not an array")
	}
	state.Tasks = make(map[string]map[string]string, len(tasks))
	for _, row := range tasks {
		pair, ok := row.([]any)
		if !ok || len(pair) != 2 {
			return state, errors.New("observer task row invalid")
		}
		id, ok := pair[0].(string)
		if !ok {
			return state, errors.New("observer task ID invalid")
		}
		fields, err := stringsFrom(pair[1])
		if err != nil || len(fields)%2 != 0 {
			return state, errors.New("observer hash invalid")
		}
		hash := make(map[string]string, len(fields)/2)
		for i := 0; i < len(fields); i += 2 {
			hash[fields[i]] = fields[i+1]
		}
		state.Tasks[id] = hash
	}
	return state, nil
}
