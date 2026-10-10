package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.PriorityOrdered;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.baomidou.mybatisplus.core.toolkit.Wrappers.query;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private RedissonClient redissonClient;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT=new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }


    //线程池，开启独立线程
    private final static ExecutorService seckill_order_executor= Executors.newSingleThreadExecutor();
    //线程任务
    @PostConstruct//类启动后马上要做的任务
    private void init(){
        seckill_order_executor.submit(new VocherOrderHander());
    }

    //阻塞队列
    private BlockingQueue<VoucherOrder> orderTasks=new ArrayBlockingQueue<>(1024*1024);
    private class VocherOrderHander implements Runnable{
        String queueName="stream.orders";
        @Override
        public void run() {
            while(true){
                try {
                    /// 1.获取消息队列中订单信息xread group g1 c1 count 1block 2000 streams streams.order >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    //2判断消息获取是否成功
                    if(list==null || list.isEmpty()){
                        //2.1失败，继续下一次循环
                        continue;
                    }
                    //解析消息里的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);

                    //2.2成功，可以下单
                    handVoucherOrder(voucherOrder);

                    //3.ACK确认 sack stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());

                } catch (Exception e) {
                    log.error("处理订单异常",e);
                    handlePendingList();
                }

            }
        }
        private void handlePendingList() {
            while(true){
                try {
                    /// 1.获取pending-list中订单信息xread group g1 c1 count 1 streams streams.order 0
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    //2判断消息获取是否成功
                    if(list==null || list.isEmpty()){
                        //2.1失败，结束循环
                        break;
                    }
                    //解析消息里的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);

                    //2.2成功，可以下单
                    handVoucherOrder(voucherOrder);

                    //3.ACK确认 sack stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());

                } catch (Exception e) {
                    log.error("处理订单异常",e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }

            }
        }
    }


    /*private class VocherOrderHander implements Runnable{

        @Override
        public void run() {
            while(true){
                /// 1.获取阻塞队列中订单信息
                try {
                    VoucherOrder voucherOrder = orderTasks.take();
                    //2.创建订单
                    handVoucherOrder(voucherOrder);
                } catch (Exception e) {
                    log.error("处理订单异常");
                }

            }
        }
    }*/

    private void handVoucherOrder(VoucherOrder voucherOrder) {
        //获取用户
        Long userId = voucherOrder.getUserId();
        //创建锁对象
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        //获取锁
        boolean isLock = lock.tryLock();
        if (!isLock) {
            //获取锁失败
            log.error("不允许重复下单");
            return;
        }
        try {
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            lock.unlock();
        }
    }





    private IVoucherOrderService proxy;

    @Override
    public Result seckillVoucher(Long voucherId) {
        //获取用户
        Long userId = UserHolder.getUser().getId();
        //订单id
        long orderId = redisIdWorker.nextId("order");
        //1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT, Collections.emptyList(), voucherId.toString(), userId.toString(),String.valueOf(orderId)
        );
        int res = result.intValue();
        //2.判断结果是否为0
        //3.不为0，无购买资格
        if(res!=0){
            return Result.fail(res==1?"库存不足":"请勿重复购买");
        }
        //4获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();

        //返回订单id
        return Result.ok(orderId);

    }

    //阻塞队列实现模式
    /*@Override
    public Result seckillVoucher(Long voucherId) {
        //获取用户
        Long userId = UserHolder.getUser().getId();
        //1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT, Collections.emptyList(), voucherId.toString(), userId.toString()
        );
        int res = result.intValue();
        //2.判断结果是否为0
        //3.不为0，无购买资格
        if(res!=0){
            return Result.fail(res==1?"库存不足":"请勿重复购买");
        }
        //4。有购买资格，把下单信息保存到阻塞队列中
        VoucherOrder voucherOrder = new VoucherOrder();
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);

        voucherOrder.setUserId(userId);
        voucherOrder.setVoucherId(voucherId);

        //放入阻塞队列
        orderTasks.add(voucherOrder);
        //获取事物的代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();



        //返回订单id
        return Result.ok(orderId);

    }*/
    /*public Result seckillVoucher(Long voucherId) {
        /// 1.查询优惠券
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        //2.判断是否开始
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
            return Result.fail("秒杀尚未开始");
        }
        //3，判断是否结束
        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
            return Result.fail("秒杀已经结束");
        }
        //4.库存是否充足
        if(voucher.getStock()<=0){
            return Result.fail("库存不足");
        }
        Long id = UserHolder.getUser().getId();
        //synchronized (id.toString().intern()){
        //创建锁对象
//        SimpleRedisLock lock = new SimpleRedisLock(stringRedisTemplate, "order" + id);
        RLock lock = redissonClient.getLock("lock:order:" + id);
        //获取锁
        boolean isLock = lock.tryLock();
        if (!isLock) {
            //获取锁失败
            return Result.fail("一个人只允许下一单");
        }
        try {
            //获取事物的代理对象
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        } finally {
            lock.unlock();
        }

    }*/
    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        //6一人一单
        Long id = voucherOrder.getUserId();

            //6.1查询订单
            int count = query().eq("user_id", id)
                    .eq("voucher_id", voucherOrder.getVoucherId())
                    .count();
            //6.2判断是否存在
            if (count > 0) {
                log.error("用户已经购买一次");
                return;
            }

            //5。扣减库存
            boolean success = seckillVoucherService.update()
                    .setSql("stock=stock-1")
                    .eq("voucher_id", voucherOrder.getVoucherId())
                    .gt("stock", 0)
                    .update();
            if (!success) {
                //扣减失败
                log.error("库存不足");
                return;
            }

                //6创建订单

            //订单写入数据库
            save(voucherOrder);

    }
}
