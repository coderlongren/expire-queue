package com.github.coderlong.impl;

import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

import org.apache.commons.lang3.StringUtils;

import com.github.coderlong.ExpireCallbackQueue;
import com.github.phantomthief.collection.BufferTrigger;

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
     *  默认消费线程不休眠
     */
    private long sleepPeriod = 0L;

    /**
     *  redis zset结构， 批量pop大小
     */
    private int batchPopCount = DEFAULT_BATCH_COUNT;
    private ExecutorService executorService;
    private AtomicBoolean[] runnings;
    /**
     *  对存入redis的对象 序列化/反序列化
     */
    private Function<T, String> encoder;
    private Function<String, T> decoder;

    private boolean enableBufferTrigger;

    private BufferTrigger bufferTrigger;

    public RedisExpireCallbackQueue(Jedis jedisConfig, String queue, int partitions,
            Function<T, Integer> partition, Function<T, String> encoder, Function<String, T> decoder, long sleepPeriod, int batchPopCount) {
        this.jedisConfig = jedisConfig;
        this.queue = queue;
        this.partitions = partitions;
        this.partition = partition;
        this.encoder = encoder;
        this.decoder = decoder;
        this.sleepPeriod = sleepPeriod;
        this.batchPopCount = batchPopCount;

        if (partitions <= 1) {
            executorService = Executors.newSingleThreadExecutor();
        } else if (partitions > 1){
            executorService = Executors.newFixedThreadPool(partitions);
        }
        for (int i = 0; i < partitions; i++) {
            runnings[i] = new AtomicBoolean(false);
        }

    }

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

    static class Builder<T> {
        private Jedis jedis;
        private int partitions;
        private Function<T, Integer> partition;
        private Function<T, String> encoder;
        private Function<String, T> decoder;
        private String queue;

        private long sleepPeriod = DEFAULT_SLEEP;
        private int batchPopCount = DEFAULT_BATCH_COUNT;

        public Builder() {
        }

        public Builder<T> withQueue(String queue) {
            this.queue = queue;
            return this;
        }

        public Builder<T> withJedis(Jedis jedis) {
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
        public Builder<T> withSleepPeriod(long value) {
            this.sleepPeriod = value;
            return this;
        }

        public Builder<T> withBatchPopCount(int value) {
            this.batchPopCount = value;
            return this;
        }

        public ExpireCallbackQueue<T> build() {
            ensure();
            return new RedisExpireCallbackQueue<>(jedis, queue, partitions, partition, encoder, decoder, sleepPeriod, batchPopCount);
        }

        private void ensure() {
            if (StringUtils.isBlank(queue)) {
                throw new IllegalArgumentException("Queue name is blank!");
            }
            if (encoder == null || decoder == null) {
                throw new IllegalArgumentException("must have encoder and decoder!");
            }
        }

    }

    public static <T> Builder<T> newBuilder() {
        return new Builder<>();
    }
}
