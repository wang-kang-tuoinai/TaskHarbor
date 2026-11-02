-- Claim 不具备请求幂等性；所有可预见检查必须在第一笔写入之前完成。
if #KEYS ~= 3 or #ARGV ~= 3 then
    return redis.error_reply('ERR invalid claim keys or arguments')
end

local pending_type = redis.call('TYPE', KEYS[1]).ok
if pending_type ~= 'none' and pending_type ~= 'list' then
    return redis.error_reply('WRONGTYPE pending key must be a list')
end
local task_id = redis.call('LINDEX', KEYS[1], 0)
if task_id == false then
    return {}
end

local function inconsistent(reason)
    return redis.error_reply('TASKQ_CLAIM_INCONSISTENT task ' .. task_id .. ': ' .. reason)
end

local active_type = redis.call('TYPE', KEYS[2]).ok
local lease_type = redis.call('TYPE', KEYS[3]).ok
if active_type ~= 'none' and active_type ~= 'set' then
    return redis.error_reply('WRONGTYPE active key must be a set')
end
if lease_type ~= 'none' and lease_type ~= 'zset' then
    return redis.error_reply('WRONGTYPE lease key must be a zset')
end
local meta = 'taskq:meta:' .. task_id
local meta_type = redis.call('TYPE', meta).ok
if meta_type == 'none' then
    return inconsistent('missing task hash')
end
if meta_type ~= 'hash' then
    return redis.error_reply('WRONGTYPE task meta must be a hash')
end

local fields = redis.call('HMGET', meta, 'task_id', 'task_type', 'payload', 'queue',
    'status', 'attempts', 'max_retry', 'execution_timeout_ms', 'owner_id', 'attempt_token')
for i = 1, 8 do
    -- false 表示缺字段；空 payload 合法，不能以字符串长度判断缺失。
    if fields[i] == false then
        return inconsistent('missing required field ' .. i)
    end
end
if task_id == '' or fields[1] ~= task_id or fields[2] == '' or fields[4] ~= 'default' or fields[5] ~= 'pending' then
    return inconsistent('invalid task identity, queue or status')
end

local function unsigned_integer(value, maximum)
    if not string.match(value, '^%d+$') then
        return nil
    end
    local number = tonumber(value)
    if not number or number > maximum then
        return nil
    end
    return number
end

-- 与 Submit 上限一致，attempts+1 同时适用于 Lua 和 32 位 Go int。
local attempts = unsigned_integer(fields[6], 2147483647)
local max_retry = unsigned_integer(fields[7], 2147483646)
local timeout = unsigned_integer(fields[8], 86400000)
if not attempts or not max_retry or not timeout or timeout == 0 then
    return inconsistent('invalid attempts, max_retry or execution timeout')
end
if attempts >= max_retry + 1 then
    return inconsistent('attempt budget exhausted')
end
-- 当前执行身份必须不存在；空值残留也不自动修复。
if fields[9] ~= false or fields[10] ~= false or
    redis.call('SISMEMBER', KEYS[2], task_id) == 1 or redis.call('ZSCORE', KEYS[3], task_id) ~= false then
    return inconsistent('execution identity or index remains')
end

local now = redis.call('TIME')
local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
local lease_ms = tonumber(ARGV[3]) -- Go 边界已校验正整数及精确表示范围。
if now_ms > 9007199254740991 - lease_ms then
    return redis.error_reply('ERR claim lease timestamp exceeds exact integer range')
end
local claimed_at = string.format('%.0f', now_ms)
local expires_at = string.format('%.0f', now_ms + lease_ms)
local next_attempts = string.format('%.0f', attempts + 1)

-- Lua 不回滚运行时错误；到此已检查本次迁移涉及的 key 类型和数据条件。
redis.call('LPOP', KEYS[1])
redis.call('SADD', KEYS[2], task_id)
redis.call('HSET', meta, 'status', 'running', 'owner_id', ARGV[1], 'attempt_token', ARGV[2],
    'attempts', next_attempts, 'last_claimed_at', claimed_at)
redis.call('ZADD', KEYS[3], expires_at, task_id)
return {task_id, fields[2], fields[3], fields[4], next_attempts, fields[8]}
