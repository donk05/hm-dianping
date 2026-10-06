package com.hmdp.utils;

import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.util.deparser.UpsertDeParser;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component
@Slf4j
public class RedisIdWorker {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    /**
     * 开始时间戳
     */
    public static final long BEGIN_TIMESTAMP =1640995200L;

    /**
     * 序列号位数
     * @param keyPredix
     * @return
     */
    public static final int COUNT_BITS=32;
    public long nextId(String keyPredix){
        //1,生成时间戳
        LocalDateTime now = LocalDateTime.now();
        long nowSecond = now.toEpochSecond(ZoneOffset.UTC);
        long timetamp = nowSecond - BEGIN_TIMESTAMP;

        //2.生成序列号
        //2.1获取当前日期，精确到天作为序列号的key
                                                            //冒号在redis分层级的，可以累加计算
        String data = now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        long count = stringRedisTemplate.opsForValue().increment("icr:" + keyPredix + ":" + data);

        //3。拼接并返回
        return timetamp<<COUNT_BITS | count;
    }

//    public static void main(String[] args) {
//        LocalDateTime localDateTime = LocalDateTime.of(2022, 1, 1, 0, 0, 0);
//        long second = localDateTime.toEpochSecond(ZoneOffset.UTC);
//        System.out.println(second);
//
//    }
}
