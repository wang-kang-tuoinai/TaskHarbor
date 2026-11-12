if not key_is(KEYS[3], 'zset') then return inconsistent('lease index must be a zset') end
if not key_is(KEYS[4], 'list') then return inconsistent('pending index must be a list') end

for i = 1, #candidates, 2 do
    local id = candidates[i]
    local task, reason = read_candidate(id, candidates[i + 1], 'retry_wait')
    if not task then return inconsistent(reason, id) end
    if not task.can_retry then return inconsistent('attempt budget exhausted', id) end
    if task.owner ~= false or task.token ~= false or redis.call('SISMEMBER', KEYS[2], id) == 1 or
        redis.call('ZSCORE', KEYS[3], id) ~= false then
        return inconsistent('execution identity or index remains', id)
    end
end

-- 合法源状态加上原子移除 retry 防止并发重复入队，避免扫描 pending 全表。
for i = 1, #candidates, 2 do
    local id = candidates[i]
    redis.call('ZREM', KEYS[1], id)
    redis.call('HSET', 'taskq:meta:' .. id, 'status', 'pending')
    redis.call('RPUSH', KEYS[4], id)
end
return #candidates / 2
