if #KEYS ~= 3 or #ARGV ~= 3 then
    return redis.error_reply('ERR invalid renew keys or arguments')
end

local function inconsistent(reason)
    return redis.error_reply('TASKQ_RENEW_INCONSISTENT task ' .. ARGV[1] .. ': ' .. reason)
end

local meta_type = redis.call('TYPE', KEYS[1]).ok
if meta_type == 'none' then
    return 0
end
if meta_type ~= 'hash' then
    return inconsistent('meta must be a hash')
end
local status = redis.call('HGET', KEYS[1], 'status')
local valid_status = {pending=true, running=true, retry_wait=true, completed=true, failed=true, canceled=true}
if status == false or not valid_status[status] then
    return inconsistent('missing or invalid status')
end
-- 正常迁移会清理身份和索引；非 running 请求到此正常拒绝，不做全量对账。
if status ~= 'running' then
    return 0
end
local token = redis.call('HGET', KEYS[1], 'attempt_token')
if token == false or token == '' then
    return inconsistent('missing attempt token')
end
if token ~= ARGV[2] then
    return 0
end

local active_type = redis.call('TYPE', KEYS[2]).ok
local lease_type = redis.call('TYPE', KEYS[3]).ok
if active_type ~= 'none' and active_type ~= 'set' then
    return inconsistent('active must be a set')
end
if lease_type ~= 'none' and lease_type ~= 'zset' then
    return inconsistent('lease must be a zset')
end
if redis.call('SISMEMBER', KEYS[2], ARGV[1]) ~= 1 then
    return inconsistent('missing active member')
end
local stored_score = redis.call('ZSCORE', KEYS[3], ARGV[1])
local score = tonumber(stored_score)
if stored_score == false or not score or score < 0 or score > 9007199254740991 or score ~= math.floor(score) then
    return inconsistent('missing or invalid lease score')
end

local now = redis.call('TIME')
local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
if now_ms >= score then
    return 0
end
local lease_ms = tonumber(ARGV[3]) -- Go 已检查请求数值范围；这里只检查与 Redis 时间相加。
if now_ms > 9007199254740991 - lease_ms then
    return redis.error_reply('ERR renew lease timestamp exceeds exact integer range')
end
-- 唯一写入在所有可预见检查之后；不复制详情时间、不修改执行身份或尝试次数。
redis.call('ZADD', KEYS[3], string.format('%.0f', now_ms + lease_ms), ARGV[1])
return 1
