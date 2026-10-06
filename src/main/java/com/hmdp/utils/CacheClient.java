package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
@Component
public class CacheClient {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    //1.
    public void set(String key, Object value, Long time , TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value),time,unit);
    }
    //2.
    public void setWIthLogicalExpire(String key, Object value, Long time , TimeUnit unit){
        //设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        redisData.setData(value);
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));

    }

    //3.
    public <R,ID> R queryWithPassTrough(String keyPrefix , ID id, Class<R> type , Function<ID,R> dbFallback,Long time,TimeUnit unit){

        String key=keyPrefix+id;
        //从redis查缓存
        String Json = stringRedisTemplate.opsForValue().get(key);
        //判断是否存在
        if (StrUtil.isNotBlank(Json)) {
            //1.存在，直接返回
            return JSONUtil.toBean(Json, type);
        }
        //判断命中的是否是空值（缓存穿透）
        if ( Json != null) {
            return null;

        }


        //2.不存在，根据id查询数据库
        R r =dbFallback.apply(id);

        //2.1不存在，返回错误
        if (r == null) {
            //把空值写入redis(缓存穿透的空值解决办法)
            stringRedisTemplate.opsForValue().set(key,"",RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);

            return null;
        }

        //2.2存在，写入redis
        //过期时间，线程安全问题的兜底方案
        this.set(key, r, time, unit);
        //3返回
        return r;

    }

    //4.
    //创建线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR= Executors.newFixedThreadPool(10);

    //逻辑时间解决方案
    public <R,ID> R queryWithLogicalExpire( String keyPrifix ,ID id,Class<R> type,Function<ID,R> dbfallback,Long time,TimeUnit unit){
        String key=keyPrifix+id;
        //从redis查缓存
        String json = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
        //判断是否存在
        if (StrUtil.isBlank(json)) {
            //1.未命中，直接返回
            return null;
        }

        //2.命中，把json反序列化为对象，判断过期时间
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        JSONObject shopJsonObject = (JSONObject) redisData.getData();
        R r = JSONUtil.toBean(shopJsonObject, type);
        //2.1未过期，返回店铺信息
        if(expireTime.isAfter(LocalDateTime.now())){
            return r;
        }

        //2.2过期，缓存重建
        //3.缓存重建
        //3.1获取互斥锁
        String lockKey=RedisConstants.LOCK_SHOP_KEY+id;
        boolean lock = tryLock(lockKey);

        //3.1.1获取成功，开启独立线程，实现缓存重建
        if(lock){
//

            // 开启独立线程
            CACHE_REBUILD_EXECUTOR.submit(()->{
                try {
                    //重建缓存
                    //1.查询数据库
                    R r1 = dbfallback.apply(id);
                    //2.写入redis
                    this.setWIthLogicalExpire(key,r1,time,unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    //释放锁
                    unLock(lockKey);
                }

            });
        }
        //3.1.2失败，返回过期的 店铺信息
        return r;

    }
    //获取互斥锁方法
    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }
    //释放锁
    private void unLock(String key){
        stringRedisTemplate.delete(key);
    }
}
