-- 1.参数列表
--1.1优惠券id
local voucherId =ARGV[1]
--1.2用户id
local userId =ARGV[2]

--2.数据key
--2.1库存key
local stockKey='seckill:stock:' .. voucherId
--2.2订单key
local orderkey='seckill:order:' .. voucherId

--3脚本业务
--3.1判断暖库存是否充足
if(tonumber(redis.call('get',stockKey))<=0) then
    return 1
end
--3.2判断用户是否下单
if(redis.call('sismember',orderkey,userId)==1) then
    return 2
end

--3，3扣库存
redis.call('incrby',stockKey,-1)

--3.4下单，保存用户
redis.call('sadd',orderkey,userId)
return 0




