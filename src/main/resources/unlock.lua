
-- 比较线程标识与锁中的标识是否一致
if(redis.call('get', KEYS[1]) == ARGV[1]) then
    -- 一致，删除锁
    return redis.call('del', KEYS[1])
end
-- 不一致，返回 0
return 0