-- 参数由 Client 校验并规范化；这里在写前检查序列化结构。
if #KEYS ~= 2 or #ARGV ~= 7 then
    return redis.error_reply('ERR invalid submit keys or arguments')
end

local task_id = ARGV[1]
local fields = {'task_type', 'payload', 'max_retry', 'execution_timeout_ms', 'retry_delay_ms', 'queue'}

if redis.call('EXISTS', KEYS[1]) == 1 then
    -- HMGET 同时在写前拒绝错误的 meta 类型。false 才是缺失，空字符串有效。
    local stored = redis.call('HMGET', KEYS[1], unpack(fields))
    local any_missing = false
    local mismatch = false
    for i = 1, #fields do
        if stored[i] == false then
            any_missing = true
        elseif stored[i] ~= ARGV[i + 1] then
            mismatch = true
        end
    end
    -- 数据不完整优先于内容冲突，不能补字段、重置运行状态或重新入队。
    if any_missing then
        return {'incomplete', task_id}
    end
    if mismatch then
        return {'conflict', task_id}
    end
    return {'duplicate', task_id}
end

-- Lua 运行时错误不回滚；先拒绝可预见的 RPUSH 类型错误，避免留下孤立 Hash。
local pending_type = redis.call('TYPE', KEYS[2]).ok
if pending_type ~= 'none' and pending_type ~= 'list' then
    return redis.error_reply('WRONGTYPE pending key must be a list')
end

local now = redis.call('TIME')
local created_at = string.format('%.0f', tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000))
redis.call('HSET', KEYS[1],
    'task_id', task_id,
    'task_type', ARGV[2],
    'payload', ARGV[3],
    'max_retry', ARGV[4],
    'execution_timeout_ms', ARGV[5],
    'retry_delay_ms', ARGV[6],
    'queue', ARGV[7],
    'status', 'pending',
    'attempts', '0',
    'created_at', created_at)
redis.call('RPUSH', KEYS[2], task_id)
return {'created', task_id}
