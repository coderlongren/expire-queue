package com.github.coderlong.impl;

import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

import com.github.coderlong.ExpireCallbackQueue;

import redis.clients.jedis.Jedis;

/**
 * @author coderlongren
 *  基于Redis的过期队列
 * Created on 2020-11-10
 */
public class RedisExpireCallbackQueue<T> implements ExpireCallbackQueue<T> {
    private static final long DEFAULT_SLEEP = 1L;
    private static final int DEFAULT_BATCH_COUNT = 100;
    private Jedis jedisConfig;
    private String queue;
    private int partitions;
    private Function<T, Integer> partition;

    /**
     *  对存入redi的对象 序列化String/  或者反序列化
     */
    private Function<T, String> encoder;
    private Function<String, T> decoder;

    @Override
    public void enqueue(T object, long expireAt) {
        String queueName = getQueueName(object);
        jedisConfig.zadd(queueName, expireAt, encoder.apply(object));

    }

    @Override
    public void consume(BiFunction<T, Date, CompletableFuture<Boolean>> callback, BooleanSupplier stopSignal) {

    }

    @Override
    public Date getExpireTime(T obj) {
        return null;
    }

    private String getQueueName(T object) {
        String queueName = queue;
        if (partitions > 0 && partition != null) {
            queueName = queue + "_" + (Math.abs(partition.apply(object)) % partitions);
        }
        return queueName;
    }

    class Builder<T> {
        private Jedis jedis;
        private int partitions;
        private Function<T, Integer> partition;
        private Function<T, String> encoder;
        private Function<String, T> decoder;
        private long sleepPeriod = DEFAULT_SLEEP;
        private int batchPopCount = DEFAULT_BATCH_COUNT;

        public Builder() {
        }

        public Builder<T> withJedisConfig(Jedis jedis) {
            this.jedis = jedis;
            return this;
        }
        public Builder<T> withPartitions(int partitions) {
            this.partitions = partitions;
            return this;
        }
        public Builder<T> withPartition(Function<T, Integer> partition) {
            this.partition = partition;
            return this;
        }

    }
}
