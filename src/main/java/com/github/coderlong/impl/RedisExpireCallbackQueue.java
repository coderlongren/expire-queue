package com.github.coderlong.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.coderlong.ExpireCallbackQueue;
import com.github.phantomthief.collection.BufferTrigger;

import redis.clients.jedis.Jedis;

/**
 * @author coderlongren
 *  基于Redis的过期队列
 * Created on 2020-11-10
 */
public class RedisExpireCallbackQueue<T> implements ExpireCallbackQueue<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisExpireCallbackQueue.class);

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
     * 序列化
     */
    private Function<T, String> encoder;

    /**
     *  反序列化
     */
    private Function<String, T> decoder;

    /**
     * @see BufferTrigger
     */
    private boolean enableBufferTrigger;

    /**
     *  TODO 支持本利聚合enqueue， 适合对消费有一定延迟，对enqueue性能追求极致
     */
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
        } else if (partitions > 1) {
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
        doConsumer(callback, stopSignal);
    }

    private void doConsumer(BiFunction<T, Date, CompletableFuture<Boolean>> callback, BooleanSupplier stopSignal) {
        if (callback == null) {

        }
        List<String> queueNames = getQueueNames();
        if (queueNames == null || queueNames.size() <= 0) {
            LOGGER.error("queueNames is empty");
            return;
        }
        for (int i = 0; i < queueNames.size(); i++) {
            String queueName = queueNames.get(i);
            AtomicBoolean running = runnings[i];
            if (running.compareAndSet(false, true)) {
                executorService.submit(() -> {
                    consume(queueName, callback, stopSignal);
                    running.set(false);
                });
            }
        }
    }


    private void consume(String queueName, BiFunction<T, Date, CompletableFuture<Boolean>> callback, BooleanSupplier stopSignal) {

    }

    private List<String> getQueueNames() {
        List<String> queueNames = new ArrayList<>();
        if (partitions > 0) {
            for (int i = 0; i < partitions; i++) {
                queueNames.add(this.queue + "_" + i);
            }
        } else {
            queueNames.add(this.queue);
        }
        return queueNames;
    }

    /**
     *  获取redis zset队列中对象过期时间
     * @param obj
     * @return
     */
    @Override
    public Date getExpireTime(T obj) {
        String queueName = getQueueName(obj);
        Double score = jedisConfig.zscore(queueName, encoder.apply(obj));
        return score == null ? null : new Date(score.longValue());
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
