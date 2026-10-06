package com.hmdp.utils;

import cn.hutool.core.lang.UUID;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

import javax.annotation.Resource;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class SimpleRedisLock implements ILock {

    private StringRedisTemplate stringRedisTemplate;
    private String name;
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;
    static {
        UNLOCK_SCRIPT=new DefaultRedisScript<>();
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("unlock.lua"));
        UNLOCK_SCRIPT.setResultType(Long.class);
    }

    public static final String KEY_PREFIX ="lock:";
    public static final String ID_PREFIX = UUID.randomUUID().toString(true)+"-";


    public SimpleRedisLock(StringRedisTemplate stringRedisTemplate, String name) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.name = name;
    }


    @Override
    public boolean tryLock(long timeoutSec) {
        //h获取线程标识
        long threadId = Thread.currentThread().getId();
        String value=ID_PREFIX+threadId;
        String key=KEY_PREFIX+name;
        Boolean success = stringRedisTemplate.opsForValue().setIfAbsent(key, value, timeoutSec, TimeUnit.SECONDS);
        //避免自动拆箱的空指针风险
        return Boolean.TRUE.equals(success);
    }

    @Override
    public void unlock() {
        //调用lua脚本
        stringRedisTemplate.execute(
          UNLOCK_SCRIPT,
                Collections.singletonList(KEY_PREFIX+name),
                ID_PREFIX+Thread.currentThread().getId()
        );

    }

//    @Override
//    public void unlock() {
//        //获取线程标识
//        String threadId = ID_PREFIX+Thread.currentThread().getId();
//        //获取锁标识
//        String id = stringRedisTemplate.opsForValue().get(KEY_PREFIX + name);
//        //判断是否一致
//        if(threadId.equals(id)){
//            stringRedisTemplate.delete(KEY_PREFIX+name);
//        }
//    }
}
