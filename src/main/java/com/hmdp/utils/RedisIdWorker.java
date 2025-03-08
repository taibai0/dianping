package com.hmdp.utils;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component
public class RedisIdWorker {
    /**
     * 开始时间戳
     */
    private static final long BEGIN_TIMESTAMP =1741046400L;
    private static final int COUNT_BITS =32;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 使用redis生成全局唯一id
     * @param keyPrefix
     * @return
     */
    public long nextId(String keyPrefix){
        //1.生成时间戳
        LocalDateTime now = LocalDateTime.now();
        long nowSecond=now.toEpochSecond(ZoneOffset.UTC);
        long timestamp = nowSecond-BEGIN_TIMESTAMP;

        //2.生成序列号
        //自增长
        String date = now.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        long count = stringRedisTemplate.opsForValue().increment("icr:" + keyPrefix + ":" + date);
        //3.拼接并返回
        return timestamp<<COUNT_BITS | count;
    }

//    public static void main(String[] args) {
//        LocalDateTime time = LocalDateTime.of(2025,3,4,0,0,0);
//        long second=time.toEpochSecond(ZoneOffset.UTC);
//        System.out.println(second);
//    }
}
