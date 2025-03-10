package com.hmdp.utils;

import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Slf4j
public class SimpleRedisLock implements ILock{
    private final StringRedisTemplate stringRedisTemplate;
    private final String name; //锁的名称

    public SimpleRedisLock(StringRedisTemplate stringRedisTemplate, String name) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.name = name;
    }

    private static final String KEY_PREFIX = "lock:";
    private static final String ID_PREFIX= UUID.randomUUID().toString()+"-";
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT ;
    static {
        UNLOCK_SCRIPT=new DefaultRedisScript<>();
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("unlock.lua"));
        UNLOCK_SCRIPT.setResultType(Long.class);
    }

    @Override
    public boolean tryLock(long timeoutSec) {
        //获取线程标识
        String threadId = ID_PREFIX+Thread.currentThread().getId();
        //获取锁
        Boolean success = stringRedisTemplate.opsForValue().setIfAbsent(KEY_PREFIX + name, threadId, timeoutSec, TimeUnit.SECONDS);
        log.info("Lock Key: {}", KEY_PREFIX + name);
        log.info("Lock result: {}", success);
        return Boolean.TRUE.equals(success);  //自动拆箱可能会产生空指针
    }

//    @Override
////    public void unlock() {
////        //获取线程标识
////        String threadId = ID_PREFIX+Thread.currentThread().getId();
////        //释放锁中的标识
////        String id = stringRedisTemplate.opsForValue().get(KEY_PREFIX + name);
////        //释放锁
////        if(threadId.equals(id)){
////            //释放锁
////            stringRedisTemplate.delete(KEY_PREFIX + name);
////        }
////    }
    @Override
    public void unlock(){
        //调用lua脚本
        stringRedisTemplate.execute(
                UNLOCK_SCRIPT,
                Collections.singletonList(KEY_PREFIX+name),
                String.valueOf(ID_PREFIX+Thread.currentThread().getId()));
    }
}
