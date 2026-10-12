package com.hmdp;

import com.hmdp.entity.Shop;
import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@SpringBootTest
class HmDianPingApplicationTests {
    @Resource
    private ShopServiceImpl shopService;
    @Resource
    private CacheClient cacheClient;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    //线程池
    private ExecutorService es = Executors.newFixedThreadPool(500);

    @Test
    void testIdWorker() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(300);
        Runnable task=()->{
            for (int i = 0; i < 100; i++) {
                long id = redisIdWorker.nextId("order");
                System.out.println("id = "+id);
            }
            latch.countDown();
        };
        long begin = System.currentTimeMillis();
        for (int i = 0; i < 300; i++) {
            es.submit(task);
        }
        latch.await();
        long end = System.currentTimeMillis();
        System.out.println("time=" +(end-begin));
    }
    @Test
    void testSave() throws InterruptedException {
        Shop shop = shopService.getById(1L);
        cacheClient.setWIthLogicalExpire(RedisConstants.CACHE_SHOP_KEY+1L,shop,10L, TimeUnit.SECONDS);
    }
    @Test
    void initShopToRedis() {
        List<Shop> shops = shopService.list();
        for (Shop shop : shops) {
            cacheClient.setWIthLogicalExpire(
                    RedisConstants.CACHE_SHOP_KEY + shop.getId(),
                    shop,
                    30L,
                    TimeUnit.MINUTES
            );
        }
        System.out.println("店铺缓存预热完成：" + shops.size() + " 条");
    }

    @Test
    void loadShopData(){
        //1.查询店铺信息
        List<Shop> list = shopService.list();
        //2.分组，按照type_id
        Map<Long,List<Shop>> map =list.stream().collect(Collectors.groupingBy(shop -> shop.getTypeId()));
        //3.存入redis
        for (Map.Entry<Long, List<Shop>> entry : map.entrySet()) {
            //获取类型id
            Long typeId = entry.getKey();
            String key = RedisConstants.SHOP_GEO_KEY+typeId;
            //获取同类型店铺集合
            List<Shop> value = entry.getValue();

            List<RedisGeoCommands.GeoLocation<String>> locations =new ArrayList<>(value.size());

            //写redis
            //低效率普通遍历写入
            /*for (Shop shop : value) {
                stringRedisTemplate.opsForGeo().add(key,new Point(shop.getX(),shop.getY()),shop.getId().toString());
            }*/
            //高效写入：
            for (Shop shop : value) {
                locations.add(new RedisGeoCommands.GeoLocation<String>(
                        shop.getId().toString(),new Point(shop.getX(),shop.getY()))
                );
            }
            stringRedisTemplate.opsForGeo().add(key,locations);

        }
    }
}
