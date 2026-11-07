local function unsigned_integer(value, maximum)
    if value == false or not string.match(value, '^%d+$') then return nil end
    local number = tonumber(value)
    if not number or number > maximum then return nil end
    return number
end
local fields = redis.call('HMGET', meta, 'attempts', 'max_retry', 'retry_delay_ms')
local attempts = unsigned_integer(fields[1], 2147483647)
local max_retry = unsigned_integer(fields[2], 2147483646)
if not attempts or attempts == 0 or not max_retry or attempts > max_retry + 1 then
    return inconsistent('invalid attempt budget')
end
local retry_at
if attempts < max_retry + 1 then
    -- 与 Submit 的已保存配置范围一致；仅在本次需要重试时检查等待配置和目标类型。
    local delay = unsigned_integer(fields[3], 604800000)
    if not delay or delay < 15000 then return inconsistent('invalid retry delay') end
    local retry_type = redis.call('TYPE', KEYS[4]).ok
    if retry_type ~= 'none' and retry_type ~= 'zset' then return inconsistent('retry must be a zset') end
    if now_ms > 9007199254740991 - delay then return inconsistent('retry timestamp exceeds exact integer range') end
    retry_at = string.format('%.0f', now_ms + delay)
end

-- Lua 运行时错误不回滚，到此已经检查迁移需要的所有类型、额度和时间计算。
redis.call('HSET', meta, 'last_error', ARGV[3], 'last_failed_at', timestamp)
if retry_at then
    redis.call('HSET', meta, 'status', 'retry_wait')
    redis.call('HDEL', meta, 'finished_at')
    redis.call('ZADD', KEYS[4], retry_at, ARGV[1])
else
    redis.call('HSET', meta, 'status', 'failed', 'finished_at', timestamp)
end
redis.call('HDEL', meta, 'owner_id', 'attempt_token')
redis.call('SREM', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[3], ARGV[1])
return 1
