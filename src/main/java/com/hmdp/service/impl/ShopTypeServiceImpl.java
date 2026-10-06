package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.jws.Oneway;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Override
    public Result queryList() {
        //1.从redis查缓存
        List<String> shopListJson = stringRedisTemplate.opsForList().range(RedisConstants.CACH_SHOP_TYPE_KEY, 0, -1);

        //判断
        if(shopListJson!=null && !shopListJson.isEmpty()){
            //存在，返回,反序列化要求的对象
            List<ShopType> typeList =new ArrayList<>();
            for (String s : shopListJson) {
                ShopType shopType = JSONUtil.toBean(s, ShopType.class);
                typeList.add(shopType);
            }
            return Result.ok(typeList);

        }
        //不存在，查询数据库

        List<ShopType> typeList = query().orderByAsc("sort").list();

        if(typeList==null && typeList.isEmpty()){
            return Result.fail("没有查到店铺分类信息");

        }
        //写入redis(List结构，要逐条写入)
        List<String> jsonList = new ArrayList<>();
        for (ShopType shopType : typeList) {
            String jsonStr = JSONUtil.toJsonStr(shopType);
            jsonList.add(jsonStr);
        }
        stringRedisTemplate.opsForList().rightPushAll(RedisConstants.CACH_SHOP_TYPE_KEY,jsonList);


        return Result.ok(typeList);


    }
}
