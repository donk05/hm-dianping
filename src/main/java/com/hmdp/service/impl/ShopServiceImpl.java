package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import com.sun.xml.internal.bind.v2.TODO;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private CacheClient cacheClient;
    @Override
    public Result queryById(Long id) {
        //调用缓存穿透
        //Shop shop = queryWithPassTrough(id);
//        Shop shop =cacheClient
//                .queryWithPassTrough(RedisConstants.CACHE_SHOP_KEY,id,Shop.class,id2->getById(id2),RedisConstants.CACHE_SHOP_TTL,TimeUnit.MINUTES);

//        互斥锁解决缓存击穿
//        Shop shop = queryWithMutex(id);

        //逻辑过期解决缓存击穿
        //Shop shop = queryWithLogicalExpire(id);
        Shop shop=cacheClient.queryWithLogicalExpire(RedisConstants.CACHE_SHOP_KEY,id, Shop.class,this::getById,RedisConstants.CACHE_SHOP_TTL,TimeUnit.MINUTES);


        if (shop == null) {
            return Result.fail("店铺不存在");
        }
        //3返回
        return Result.ok(shop);
    }

    //创建线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR= Executors.newFixedThreadPool(10);

/*
    //逻辑时间解决方案
    public Shop queryWithLogicalExpire(Long id){

        //从redis查缓存
        String shopJson = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
        //判断是否存在
        if (StrUtil.isBlank(shopJson)) {
            //1.未命中，直接返回
            return null;
        }

        //2.命中，把json反序列化为对象，判断过期时间
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        JSONObject shopJsonObject = (JSONObject) redisData.getData();
        Shop shop = JSONUtil.toBean(shopJsonObject, Shop.class);
        //2.1未过期，返回店铺信息
        if(expireTime.isAfter(LocalDateTime.now())){
            return shop;
        }

        //2.2过期，缓存重建
        //3.缓存重建
        //3.1获取互斥锁
        String lockKey=RedisConstants.LOCK_SHOP_KEY+id;
        boolean lock = tryLock(lockKey);

        //3.1.1获取成功，开启独立线程，实现缓存重建
        if(lock){
//            //此时获取锁成功，需要再次驾车呢redis缓存是否存在
//            //从redis查缓存
//            shopJson = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
//            //判断是否存在
//            if (StrUtil.isNotBlank(shopJson)) {
//                //1.存在，直接返回
//                shop = JSONUtil.toBean(shopJson, Shop.class);
//                return shop;
//            }

            // 开启独立线程
            CACHE_REBUILD_EXECUTOR.submit(()->{
                try {
                    //重建缓存
                    this.saveShoptoRedis(id,20L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    //释放锁
                    unLock(lockKey);
                }

            });
        }
        //3.1.2失败，返回过期的 店铺信息
        return shop;

    }
*/

    //互斥锁解决方案
    /*public Shop queryWithMutex(Long id){

        //从redis查缓存
        String shopJson = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
        //判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            //1.存在，直接返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        //判断命中的是否是空值
        if (shopJson != null) {
            return null;

        }

        //2.实现缓存重建
        //2.1获取互斥锁(每个店铺都有一个锁key)
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        Shop shop = null;
        try {
            boolean idLock = tryLock(lockKey);
            //2.2判断获取是否成功
            if(!idLock){
                //2.3失败，休眠并重试
                Thread.sleep(50);
                return queryWithMutex(id);
            }

            //此时获取锁成功，需要再次驾车呢redis缓存是否存在
                //从redis查缓存
                shopJson = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
                //判断是否存在
                if (StrUtil.isNotBlank(shopJson)) {
                    //1.存在，直接返回
                    shop = JSONUtil.toBean(shopJson, Shop.class);
                    return shop;
                }
                //判断命中的是否是空值
                if (shopJson != null) {
                    return null;

                }
            //2.4成功（并且此时redis缓存还是不存在），根据id查询数据库

            //2.不存在，根据id查询数据库，就是下面的
            shop = getById(id);
//            //模拟重构的延时
//            Thread.sleep(200);
            //2.1不存在，返回错误
            if (shop == null) {
                //把空值写入redis(缓存穿透的空值解决办法)
                stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY+id,"",RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);

                return null;
            }

            //2.2存在，写入redis
            //过期时间，线程安全问题的兜底方案
            stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY+id,JSONUtil.toJsonStr(shop),RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            //写入redis后，要释放互斥锁
            unLock(lockKey);
        }




        //3返回
        return shop;

    }*/

    //根据id查店铺的缓存穿透解决方案
   /* public Shop queryWithPassTrough(Long id){

        //从redis查缓存
        String shopJson = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
        //判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            //1.存在，直接返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        //判断命中的是否是空值（缓存穿透）
        if (shopJson != null) {
            return null;

        }


        //2.不存在，根据id查询数据库
        Shop shop = getById(id);

        //2.1不存在，返回错误
        if (shop == null) {
            //把空值写入redis(缓存穿透的空值解决办法)
            stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY+id,"",RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);

            return null;
        }

        //2.2存在，写入redis
        //过期时间，线程安全问题的兜底方案
        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY+id,JSONUtil.toJsonStr(shop),RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        //3返回
        return shop;

    }
*/


    /*//获取互斥锁方法
    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }
    //释放锁
    private void unLock(String key){
        stringRedisTemplate.delete(key);
    }

    //店铺信息和逻辑鬼泣时间
    public void saveShoptoRedis(Long id,Long expireSeconds) throws InterruptedException {
        //1.查询店铺数据
        Shop shop = getById(id);
        //模拟时间延迟时间
        Thread.sleep(200);
        //2.封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        //3.写入redis
        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY+id,JSONUtil.toJsonStr(redisData));
        //System.out.println("存入 Redis 的完整 JSON: " + JSONUtil.toJsonStr(redisData));
    }*/

    //更新店铺
    //策略，先更新数据库，再删除缓存，更安全
    @Transactional
    @Override
    public Result updata(Shop shop) {
        Long id = shop.getId();
        if(id==null){
            return Result.fail("店铺id不能为空");
        }
        //1.更新数据库
        updateById(shop);
        //2.删除缓存
        stringRedisTemplate.delete(RedisConstants.CACHE_SHOP_KEY+ id);

        return Result.ok();
    }
}
