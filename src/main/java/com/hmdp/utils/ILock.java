package com.hmdp.utils;

public interface ILock {

    /**
     * 尝试获取锁，true代表获取成功
     */
    boolean tryLock(long timeoutSec);

    /**
     * 释放锁
     */
    void unlock();
}
