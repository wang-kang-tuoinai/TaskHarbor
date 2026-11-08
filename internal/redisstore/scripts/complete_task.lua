-- 执行权由共用前缀校验；只改变本次成功迁移需要的字段与索引。
redis.call('HSET', meta, 'status', 'completed', 'result', ARGV[3], 'finished_at', timestamp)
redis.call('HDEL', meta, 'owner_id', 'attempt_token')
redis.call('SREM', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[3], ARGV[1])
return 1
