package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import lombok.NonNull;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;

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
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private RedissonClient redissonClient;
    @Override

    public Result seckillVoucher(Long voucherId) {
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

    }
    @Transactional
    public Result createVoucherOrder(Long voucherId) {
        //6一人一单
        Long id = UserHolder.getUser().getId();

            //6.1查询订单
            int count = query().eq("user_id", id)
                    .eq("voucher_id", voucherId)
                    .count();
            //6.2判断是否存在
            if (count > 0) {
                return Result.fail("该用户已经购买过了");
            }

            //5。扣减库存
            boolean success = seckillVoucherService.update()
                    .setSql("stock=stock-1")
                    .eq("voucher_id", voucherId)
                    .gt("stock", 0)
                    .update();
            if (!success) {
                //扣减失败
                return Result.fail("库存不足");
            }

            //6创建订单
            VoucherOrder voucherOrder = new VoucherOrder();
            long orderId = redisIdWorker.nextId("order");
            voucherOrder.setId(orderId);

            voucherOrder.setUserId(id);
            voucherOrder.setVoucherId(voucherId);
            //订单写入数据库
            save(voucherOrder);
            return Result.ok(orderId);

    }
}
