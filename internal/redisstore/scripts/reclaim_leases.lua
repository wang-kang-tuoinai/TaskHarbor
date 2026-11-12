local batch = {}
local retry_type_checked = false
for i = 1, #candidates, 2 do
    local id = candidates[i]
    local task, reason = read_candidate(id, candidates[i + 1], 'running')
    if not task then return inconsistent(reason, id) end
    if not task.owner or task.owner == '' or not task.token or task.token == '' or
        redis.call('SISMEMBER', KEYS[2], id) ~= 1 then
        return inconsistent('missing execution identity or active member', id)
    end
    if task.can_retry then
        if not retry_type_checked then
            if not key_is(KEYS[3], 'zset') then return inconsistent('retry index must be a zset', id) end
            retry_type_checked = true
        end
        local delay = unsigned_integer(redis.call('HGET', task.meta, 'retry_delay_ms'), 604800000)
        if not delay or delay < 15000 then return inconsistent('invalid retry delay', id) end
        if now_ms > max_exact_ms - delay then return inconsistent('retry timestamp exceeds exact integer range', id) end
        task.retry_at = string.format('%.0f', now_ms + delay)
    end
    task.id = id
    batch[#batch + 1] = task
end

-- Lua 出错不回滚；整批通过校验后才开始迁移。
local timestamp = string.format('%.0f', now_ms)
for _, task in ipairs(batch) do
    redis.call('SREM', KEYS[2], task.id)
    redis.call('ZREM', KEYS[1], task.id)
    redis.call('HDEL', task.meta, 'owner_id', 'attempt_token')
    redis.call('HSET', task.meta, 'last_error', 'lease expired', 'last_failed_at', timestamp)
    if task.retry_at then
        redis.call('HSET', task.meta, 'status', 'retry_wait')
        redis.call('HDEL', task.meta, 'finished_at')
        redis.call('ZADD', KEYS[3], task.retry_at, task.id)
    else
        redis.call('HSET', task.meta, 'status', 'failed', 'finished_at', timestamp)
    end
end
return #batch
