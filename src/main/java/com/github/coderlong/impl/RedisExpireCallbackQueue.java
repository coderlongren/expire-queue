package com.github.coderlong.impl;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.coderlong.ExpireCallbackQueue;
import com.github.coderlong.util.CompletableFutureWrapper;
import com.github.phantomthief.collection.BufferTrigger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
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
    private static final int DEFAULT_BUFFER_BATCH_COUNT = 50;

    private JedisPool jedisPool;
    private String queue;
    // 只支持扩容 不支持缩容
    private IntSupplier partitionsSupplier;
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
    private ThreadPoolExecutor threadPoolExecutor;
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

    private int linger;

    private int batchCount = DEFAULT_BUFFER_BATCH_COUNT;

    /**
     * TODO 支持本利聚合enqueue， 适合对消费有一定延迟，对enqueue性能追求极致
     */
    private BufferTrigger bufferTrigger;

    public RedisExpireCallbackQueue(JedisPool jedisPool, String queue, IntSupplier partitionsSupplier,
            Function<T, Integer> partition, Function<T, String> encoder, Function<String, T> decoder, long sleepPeriod,
            int batchPopCount, boolean enableBufferTrigger, int batchCount, int linger) {
        this.enableBufferTrigger = enableBufferTrigger;
        this.batchCount = batchCount;
        this.jedisPool = jedisPool;
        this.queue = queue;
        this.partitionsSupplier = partitionsSupplier;
        this.partition = partition;
        this.encoder = encoder;
        this.decoder = decoder;
        this.sleepPeriod = sleepPeriod;
        this.batchPopCount = batchPopCount;
        this.linger = linger;
        this.partitions = partitionsSupplier == null ? 1 : partitionsSupplier.getAsInt();
        if (partitions <= 1) {
            threadPoolExecutor = new ThreadPoolExecutor(1, 1,
                    0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
        } else {
            threadPoolExecutor = new ThreadPoolExecutor(partitions, partitions,
                    0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>());
        }
        runnings = new AtomicBoolean[partitions];
        for (int i = 0; i < partitions; i++) {
            runnings[i] = new AtomicBoolean(false);
        }
        if (enableBufferTrigger) {
            bufferTrigger = BufferTrigger.<QueueItem>batchBlockingTrigger()
                    .batchSize(batchCount)
                    .linger(linger, TimeUnit.SECONDS)
                    .setConsumerEx(this::consumerBufferTrigger)
                    .build();
        }

    }


    @Override
    public void enqueue(T object, long expireAt) {
        String queueName = getQueueName(object);
        if (this.enableBufferTrigger) {
            bufferTrigger.enqueue(new QueueItem(object, expireAt));
            return;
        }
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.zadd(queueName, expireAt, encoder.apply(object));
            double mem = expireAt;
        } catch (Exception e) {
            LOGGER.error("添加redis失败", e);
        } finally {
            if (jedis != null) {
                jedisPool.returnResourceObject(jedis);
            }
        }

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
                threadPoolExecutor.submit(() -> {
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
        Jedis jedis = jedisPool.getResource();
        try {

            Set<Tuple> set = jedis.zrangeByScoreWithScores(queueName, 0,
                    expireBefore, 0, batchCount);
            return map(set);
        } catch (Exception e) {
            LOGGER.error("failed to pop", e);
        } finally {
            jedisPool.returnResourceObject(jedis);
        }
        return Collections.emptyList();
    }

    private long remove(String queueName, T obj) {
        String redisValue = encoder.apply(obj);
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.zrem(queueName, encoder.apply(obj));
        } catch (Exception e) {
            LOGGER.error("delete redis fail. ", e);
        } finally {
            if(null != jedis) {
                jedisPool.returnResourceObject(jedis);
            }
        }
        return 1L;
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

        public QueueItem() {
        }

        public QueueItem(T obj, long expireAt) {
            this.obj = obj;
            this.expireAt = expireAt;
        }

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
        if (partitionsSupplier != null && partitionsSupplier.getAsInt() != partitions) {
            excessQueue();
        }
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
     *  linger 或者 batchCount 模式触发消费 (暂时只支持这两种消费模式)
     * @param items
     */
    private void consumerBufferTrigger(Collection<QueueItem> items) {
        if (items == null || items.isEmpty()) {
            return;
        }
        Jedis jedis = null;
        Map<String, Map<String, Double>> itemMap = new HashMap<>();
        items.stream().forEach(item -> {
            String queueName = getQueueName(item.getObj());
            Map<String, Double> queueMap = itemMap.getOrDefault(queueName, new HashMap<>());
            queueMap.put(encoder.apply(item.getObj()), (double) item.expireAt);
            itemMap.put(queueName, queueMap);

        });
        try {
            jedis = jedisPool.getResource();
            Pipeline pipeline = jedis.pipelined();
            AtomicInteger count = new AtomicInteger(0);
            itemMap.entrySet().stream().forEach(entry -> {
                pipeline.zadd(entry.getKey(), entry.getValue());
                count.addAndGet(entry.getValue().size());
            });
            LOGGER.info("批次： {}", count.get());
        } catch (Exception e) {
            LOGGER.error("buffer trigger批量消费失败");
        } finally {
            if (jedis != null) {
                jedisPool.returnResourceObject(jedis);
            }
        }

    }

    /**
     * 获取redis zset队列中对象过期时间
     */
    @Override
    public Date getExpireTime(T obj) {
        String queueName = getQueueName(obj);
        Double score = jedisPool.getResource().zscore(queueName, encoder.apply(obj));
        return score == null ? null : new Date(score.longValue());
    }


    private String getQueueName(T object) {
        String queueName = queue;
        if (partitionsSupplier != null && partitionsSupplier.getAsInt() != partitions) {
            excessQueue();
        }
        if (partitions > 0 && partition != null) {
            queueName = queue + "_" + (Math.abs(partition.apply(object)) % partitions);
        }
        return queueName;
    }

    /**
     *  加锁扩容
     */
    private synchronized void excessQueue() {
        if (partitionsSupplier.getAsInt() < partitions) {
            throw new IllegalArgumentException("partitions只支持扩容");
        }
        for (int i = partitions - 1; i < partitionsSupplier.getAsInt(); i++) {
            runnings[i] = new AtomicBoolean(false);
        }
        this.partitions = partitionsSupplier.getAsInt();
        threadPoolExecutor.setMaximumPoolSize(partitions);
        threadPoolExecutor.setCorePoolSize(partitions);
        threadPoolExecutor.prestartAllCoreThreads();
    }

    public static class Builder<T> {
        private JedisPool jedisPool;
        private IntSupplier partitionsSupplier;
        private Function<T, Integer> partition;
        private Function<T, String> encoder;
        private Function<String, T> decoder;
        private String queue;

        private long sleepPeriod = DEFAULT_SLEEP;
        private int batchPopCount = DEFAULT_BATCH_COUNT;
        private boolean enableBufferTrigger;
        private int batchCount;
        private static final int BUFFER_TRIGGER_LINGER_TIME = 1;
        private int linger = BUFFER_TRIGGER_LINGER_TIME;

        public Builder() {
        }
        public Builder<T> withEnableBufferTrigger(boolean enableBufferTrigger) {
            this.enableBufferTrigger = enableBufferTrigger;
            return this;
        }
        public Builder<T> withBatchCount(int batchCount) {
            this.batchCount = batchCount;
            return this;
        }

        public Builder<T> withQueue(String queue) {
            this.queue = queue;
            return this;
        }

        public Builder<T> withJedisPool(JedisPool jedisPool) {
            this.jedisPool = jedisPool;
            return this;
        }

        public Builder<T> withLinger(int linger) {
            this.linger = linger;
            return this;
        }


        public Builder<T> withPartitionsSupplier(IntSupplier partitionsSupplier) {
            this.partitionsSupplier = partitionsSupplier;
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
            return new RedisExpireCallbackQueue<>(jedisPool, queue, partitionsSupplier, partition, encoder, decoder, sleepPeriod,
                    batchPopCount, enableBufferTrigger, batchCount, linger);
        }

        private void ensure() {
            if (jedisPool == null) {
                throw new IllegalArgumentException("jedis config is null!");
            }

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
