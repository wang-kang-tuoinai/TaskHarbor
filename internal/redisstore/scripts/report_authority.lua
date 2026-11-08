-- 两种结果上报共用执行权检查；所有拒绝都发生在首次写入之前。
local function inconsistent(reason)
    return redis.error_reply('TASKQ_REPORT_INCONSISTENT task ' .. ARGV[1] .. ': ' .. reason)
end
local meta = KEYS[1]
local meta_type = redis.call('TYPE', meta).ok
if meta_type == 'none' then return 0 end
if meta_type ~= 'hash' then return inconsistent('meta must be a hash') end
local status = redis.call('HGET', meta, 'status')
local valid_status = {pending=true, running=true, retry_wait=true, completed=true, failed=true, canceled=true}
if status == false or not valid_status[status] then return inconsistent('missing or invalid status') end
if status ~= 'running' then return 0 end
local token = redis.call('HGET', meta, 'attempt_token')
if token == false or token == '' then return inconsistent('missing attempt token') end
if token ~= ARGV[2] then return 0 end
local active_type = redis.call('TYPE', KEYS[2]).ok
local lease_type = redis.call('TYPE', KEYS[3]).ok
if active_type ~= 'none' and active_type ~= 'set' then return inconsistent('active must be a set') end
if lease_type ~= 'none' and lease_type ~= 'zset' then return inconsistent('lease must be a zset') end
if redis.call('SISMEMBER', KEYS[2], ARGV[1]) ~= 1 then return inconsistent('missing active member') end
local stored_score = redis.call('ZSCORE', KEYS[3], ARGV[1])
local score = tonumber(stored_score)
if not score or score ~= score or score < 0 or score > 9007199254740991 or score ~= math.floor(score) then
    return inconsistent('missing or invalid lease score')
end
local now = redis.call('TIME')
local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
if now_ms >= score then return 0 end
local timestamp = string.format('%.0f', now_ms)
