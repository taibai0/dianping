-- 获取锁中的值
local value = redis.call('get', KEYS[1])
-- 比较锁中的值与线程 ID
if value == ARGV[1] then
    -- 释放锁
    return redis.call('del', KEYS[1])
end
return 0