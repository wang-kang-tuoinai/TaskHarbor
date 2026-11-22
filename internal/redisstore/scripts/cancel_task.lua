local meta, pending, active, lease, retry = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5]
local id = ARGV[1]

local function inconsistent(reason)
    return redis.error_reply('TASKQ_CANCEL_INCONSISTENT ' .. reason)
end

local meta_type = redis.call('TYPE', meta).ok
if meta_type == 'none' then
    return {'not_found', '', ''}
end
if meta_type ~= 'hash' then
    return inconsistent('task meta must be a hash')
end
local status = redis.call('HGET', meta, 'status')
if status == 'completed' or status == 'failed' or status == 'canceled' then
    return {'already_terminal', status, ''}
end
if status ~= 'pending' and status ~= 'running' and status ~= 'retry_wait' then
    return inconsistent('invalid task status')
end
local fields = redis.call('HMGET', meta, 'task_id', 'queue', 'owner_id', 'attempt_token')
if fields[1] ~= id or fields[2] ~= 'default' then
    return inconsistent('invalid task identity or queue')
end

local active_type = redis.call('TYPE', active).ok
local lease_type = redis.call('TYPE', lease).ok
local retry_type = redis.call('TYPE', retry).ok
if (active_type ~= 'none' and active_type ~= 'set') or
    (lease_type ~= 'none' and lease_type ~= 'zset') or
    (retry_type ~= 'none' and retry_type ~= 'zset') then
    return inconsistent('invalid active, lease or retry index type')
end
local in_active = redis.call('SISMEMBER', active, id) == 1
local in_lease = redis.call('ZSCORE', lease, id) ~= false
local in_retry = redis.call('ZSCORE', retry, id) ~= false
local owner, token = fields[3], fields[4]
local notification, diagnostic = nil, ''
if status == 'running' then
    if not owner or owner == '' or not token or token == '' or not in_active or not in_lease or in_retry then
        return inconsistent('invalid running execution identity or index membership')
    end
    if ARGV[2] == '1' then
        notification = cjson.encode({task_id = id, attempt_token = token})
    else
        -- Submit 保留原始 ID 字节；不要发布可能被 JSON 消费端替换字符的 ID。
        diagnostic = 'task ID is not valid UTF-8'
    end
else
    if owner ~= false or token ~= false or in_active or in_lease then
        return inconsistent('unexpected execution identity or index member')
    end
    if status == 'retry_wait' then
        if not in_retry then
            return inconsistent('missing retry member')
        end
    else
        if in_retry or redis.call('TYPE', pending).ok ~= 'list' then
            return inconsistent('unexpected retry member or invalid pending index')
        end
        -- Redis 5 没有 LPOS；写前确认成员存在，写入时一次删除所有目标项。
        local found = false
        for _, member in ipairs(redis.call('LRANGE', pending, 0, -1)) do
            if member == id then
                found = true
                break
            end
        end
        if not found then
            return inconsistent('missing pending member')
        end
    end
end
local now = redis.call('TIME')
local finished_at = string.format('%.0f', tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000))

-- Lua 不回滚运行时错误。进入写入阶段前已检查本次迁移所需数据并准备通知。
if status == 'running' then
    redis.call('SREM', active, id)
    redis.call('ZREM', lease, id)
    redis.call('HDEL', meta, 'owner_id', 'attempt_token')
elseif status == 'retry_wait' then
    redis.call('ZREM', retry, id)
else
    redis.call('LREM', pending, 0, id)
end
redis.call('HSET', meta, 'status', 'canceled', 'finished_at', finished_at)
if notification then
    -- 发布失败不改变已完成的权威迁移，也不应导致调用方补发。
    local published = redis.pcall('PUBLISH', 'cancel:' .. owner, notification)
    if type(published) == 'table' and published.err then
        diagnostic = published.err
    end
end
return {'canceled', 'canceled', diagnostic}
