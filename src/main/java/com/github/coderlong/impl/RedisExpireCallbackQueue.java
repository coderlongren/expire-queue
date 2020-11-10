package com.github.coderlong.impl;

import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;

import com.github.coderlong.ExpireCallbackQueue;

/**
 * @author coderlongren
 *  基于Redis的过期队列
 * Created on 2020-11-05
 */
public class RedisExpireCallbackQueue<T> implements ExpireCallbackQueue<T> {

    @Override
    public void enqueue(T object, long expireAt) {

    }

    @Override
    public void consume(BiFunction<T, Date, CompletableFuture<Boolean>> callback, BooleanSupplier stopSignal) {

    }

    @Override
    public Date getExpireTime(T obj) {
        return null;
    }
}
