package com.github.coderlong.impl;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.coderlong.ExpireCallbackQueue;
import com.github.coderlong.util.CompletableFutureWrapper;
import com.github.phantomthief.collection.BufferTrigger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

/**
 * @author coderlongren
 * 基于Redis的过期队列
 * Created on 2020-11-10
 */
public class RedisExpireCallbackQueue<T> implements ExpireCallbackQueue<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisExpireCallbackQueue.class);
    private static final int DEFAULT_MAX_BATCH_AWAIT_MINUTES = 5;
    private static final long DEFAULT_SLEEP = 1L;
    private static final int DEFAULT_BATCH_COUNT = 100;
    private Jedis jedisConfig;
    private String queue;
    private int partitions;
    private Function<T, Integer> partition;

    /**
     * 默认消费线程不休眠
     */
    private long sleepPeriod = 0L;

    /**
     * redis zset结构， 批量pop大小
     */
    private int batchPopCount = DEFAULT_BATCH_COUNT;
    private ExecutorService executorService;
    private AtomicBoolean[] runnings;
    /**
     * 序列化
     */
    private Function<T, String> encoder;

    /**
     * 反序列化
     */
    private Function<String, T> decoder;

    /**
     * @see BufferTrigger
     */
    private boolean enableBufferTrigger;

    /**
     * TODO 支持本利聚合enqueue， 适合对消费有一定延迟，对enqueue性能追求极致
     */
    private BufferTrigger bufferTrigger;

    public RedisExpireCallbackQueue(Jedis jedisConfig, String queue, int partitions,
            Function<T, Integer> partition, Function<T, String> encoder, Function<String, T> decoder, long sleepPeriod,
            int batchPopCount) {
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
        runnings = new AtomicBoolean[partitions];
        for (int i = 0; i < partitions; i++) {
            runnings[i] = new AtomicBoolean(false);
        }

    }

    @Override
    public void enqueue(T object, long expireAt) {
        String queueName = getQueueName(object);
        jedisConfig.zadd(queueName, expireAt, encoder.apply(object));
    }

    /**
     * 消费逻辑
     *
     * @param stopSignal 中断消费的hook (一般enqueue 和 consume不在一个jvm内， consumer进程应该保证jvm实例关闭时, {@link }stopSignal.set(false))
     */
    @Override
    public void consume(BiFunction<T, Date, CompletableFuture<Boolean>> callback, BooleanSupplier stopSignal) {
        doConsumer(callback, stopSignal);
    }

    private void doConsumer(BiFunction<T, Date, CompletableFuture<Boolean>> callback, BooleanSupplier stopSignal) {
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


    private void consume(String queueName, BiFunction<T, Date, CompletableFuture<Boolean>> callback,
            BooleanSupplier stopSignal) {
        while (stopSignal == null || !stopSignal.getAsBoolean()) {
            List<QueueItem> items = pop(queueName, System.currentTimeMillis(), batchPopCount);
            List<CompletableFuture> futures = items.stream().map(item ->
                    callback.apply(item.getObj(), new Date(item.getExpireAt())).whenComplete((res, err) -> {
                        if (err == null) {
                            if (res) {
                                remove(queueName, item.getObj());
                            } else {
                                LOGGER.error("consumer fail");
                            }
                        }
                    })
            ).collect(Collectors.toList());
            if (futures != null && futures.size() > 0) {
                CompletableFutureWrapper
                        .orTimeout(CompletableFuture.allOf(futures.toArray(new CompletableFuture[1])), Duration
                                .ofSeconds(DEFAULT_MAX_BATCH_AWAIT_MINUTES)).exceptionally(err -> {
                    LOGGER.error("batch consume timeout fail");
                    return null;
                });
            }
            if (items.size() < batchPopCount) {
                sleepUninterruptibly(sleepPeriod, TimeUnit.SECONDS);
            }
        }
    }

    private List<QueueItem> pop(String queueName, long expireBefore, int batchCount) {
        try {
            Set<Tuple> set = jedisConfig.zrangeByScoreWithScores(queueName, 0,
                    expireBefore, 0, batchCount);
            return map(set);
        } catch (Exception e) {
            LOGGER.error("failed to pop");
        }
        return Collections.emptyList();
    }

    private long remove(String queueName, T obj) {
        return jedisConfig.zrem(queueName, encoder.apply(obj));
    }

    private List<QueueItem> map(Set<Tuple> set) {
        if (set == null || set.size() == 0) {
            return Collections.emptyList();
        }
        return set.stream()
                .filter(Objects::nonNull).map(this::map)
                .collect(Collectors.toList());
    }

    private QueueItem map(Tuple tuple) {
        if (tuple == null) {
            return null;
        }
        QueueItem item = new QueueItem();
        item.obj = decoder.apply(tuple.getElement());
        item.expireAt = (long) tuple.getScore();
        return item;
    }

    private class QueueItem {

        private T obj;
        private long expireAt;

        public T getObj() {
            return obj;
        }

        public void setObj(T obj) {
            this.obj = obj;
        }

        public long getExpireAt() {
            return expireAt;
        }

        public void setExpireAt(long expireAt) {
            this.expireAt = expireAt;
        }

        @Override
        public String toString() {
            return "QueueItem{" + "obj=" + encoder.apply(obj) + ", expireAt=" + expireAt + '}';
        }
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
     * 获取redis zset队列中对象过期时间
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

    public static class Builder<T> {
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
        public Builder<T> withEncoder(Function<T, String> encoder) {
            this.encoder = encoder;
            return this;
        }

        public Builder<T> withDecoder(Function<String, T> decoder) {
            this.decoder = decoder;
            return this;
        }


        public ExpireCallbackQueue<T> build() {
            ensure();
            return new RedisExpireCallbackQueue<>(jedis, queue, partitions, partition, encoder, decoder, sleepPeriod,
                    batchPopCount);
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
