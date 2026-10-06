package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.SystemConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {


    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Autowired
    private RedisTemplate<Object, Object> redisTemplate;

    @Override
    public Result sendCode(String phone, HttpSession session) {
        //1.校验手机号
        if(RegexUtils.isPhoneInvalid(phone)) {


            //2.不符合，返回错误信息
            return Result.fail("手机号格式不正确");
        }

        //3.符合，生成验证码
        String code = RandomUtil.randomNumbers(6);


        //4，保存验证码到session里
        stringRedisTemplate.opsForValue().set( RedisConstants.LOGIN_CODE_KEY+phone,code,RedisConstants.LOGIN_CODE_TTL, TimeUnit.MINUTES);


        //5.发送验证码
        log.debug("发送验证码成功，验证码：{}",code);

         //返回ok
        return Result.ok();
    }



    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        //校验手机号
        String phone = loginForm.getPhone();
        if(RegexUtils.isPhoneInvalid(phone)){
            return Result.fail("手机号格式错误");
        }

        //校验redis里的验证码
        String Cachecode = stringRedisTemplate.opsForValue().get(RedisConstants.LOGIN_CODE_KEY+phone);

        String code = loginForm.getCode();
        if(Cachecode==null || !Cachecode.equals(code)){
            //不一致呢个，报错
            return Result.fail("验证码错误");
        }

        //一致，根据手机号查用户（使用mybatis-plus)
        User user = query().eq("phone", phone).one();
        //判断用户是否存在
        if(user == null){
            //6.不存在，创建新用户
            user=createWithPhone(phone);

        }
        //7.保存用户信息到redis
        //7.1生成token，作登录令牌
        String token = UUID.randomUUID().toString(true);


        //7.2把user对象转为hash Map 存储
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        //Map<String, Object> userMap = BeanUtil.beanToMap(userDTO);
        Map<String, String> userMap = new HashMap<>();
        userMap.put("id", userDTO.getId().toString());
        userMap.put("nickName", userDTO.getNickName());
        userMap.put("icon", userDTO.getIcon());

        //7.3存储
        stringRedisTemplate.opsForHash().putAll(RedisConstants.LOGIN_USER_KEY+token,userMap);
        stringRedisTemplate.expire(RedisConstants.LOGIN_USER_KEY+token,RedisConstants.LOGIN_USER_TTL,TimeUnit.MINUTES);



        //7.4把token返回给客户端

        return Result.ok(token);
    }

    private User createWithPhone(String phone) {
        User user = new User();
        user.setPhone(phone);
        user.setNickName(SystemConstants.USER_NICK_NAME_PREFIX +RandomUtil.randomString(10));
        //保存用户-mybatis-plus
        save(user);
        return user;
    }
}

