local max_exact_ms = 9007199254740991

local function inconsistent(reason, task_id)
    if task_id then reason = 'task ' .. task_id .. ': ' .. reason end
    return redis.error_reply('TASKQ_RECLAIM_INCONSISTENT ' .. reason)
end

local function key_is(key, expected)
    local kind = redis.call('TYPE', key).ok
    return kind == 'none' or kind == expected
end

local function unsigned_integer(value, maximum)
    if value == false or not string.match(value, '^%d+$') then return nil end
    local number = tonumber(value)
    if not number or number > maximum then return nil end
    return number
end

if not key_is(KEYS[1], 'zset') then return inconsistent('source index must be a zset') end
local now = redis.call('TIME')
local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
-- 下限包含非法负分值以便明确报错；WITHSCORES 保留原子选择时的当前分值。
local candidates = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', string.format('%.0f', now_ms),
    'WITHSCORES', 'LIMIT', 0, ARGV[1])
if #candidates == 0 then return 0 end
if not key_is(KEYS[2], 'set') then return inconsistent('active index must be a set') end

local function read_candidate(id, raw_score, expected_status)
    local score = tonumber(raw_score)
    if not score or score < 0 or score > max_exact_ms or score ~= math.floor(score) then
        return nil, 'invalid source score'
    end
    local meta = 'taskq:meta:' .. id
    if redis.call('TYPE', meta).ok ~= 'hash' then return nil, 'missing or invalid task hash' end
    local fields = redis.call('HMGET', meta, 'task_id', 'queue', 'status', 'attempts', 'max_retry',
        'owner_id', 'attempt_token')
    if id == '' or fields[1] ~= id or fields[2] ~= 'default' or fields[3] ~= expected_status then
        return nil, 'invalid task identity, queue or status'
    end
    local attempts = unsigned_integer(fields[4], 2147483647)
    local max_retry = unsigned_integer(fields[5], 2147483646)
    if not attempts or attempts == 0 or not max_retry or attempts > max_retry + 1 then
        return nil, 'invalid attempt budget'
    end
    return {meta=meta, owner=fields[6], token=fields[7], can_retry=attempts < max_retry + 1}
end
