import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.github.coderlong.ExpireCallbackQueue;
import com.github.coderlong.impl.RedisExpireCallbackQueue;
import com.github.coderlong.util.JsonUtil;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class TestExpireQueue {
    private static JedisPool jedisPool;
    private static ScheduledExecutorService scheduledExecutorService;
    @BeforeAll
    static void init() {
        GenericObjectPoolConfig redisConfig = new GenericObjectPoolConfig();
        redisConfig.setMaxTotal(100);
        redisConfig.setMaxWaitMillis(10 * 1000);
        redisConfig.setMaxIdle(1000);
        redisConfig.setTestOnBorrow(true);
        jedisPool = new JedisPool(redisConfig,"127.0.0.1", 6379);
        scheduledExecutorService = Executors.newScheduledThreadPool(1);
    }

    private static AtomicInteger counter = new AtomicInteger(0);

    @Test
    void testDeleteKey() {
    //jedis.zrem("myQueue_43",)
    }

    @Test
    void testConsume() throws InterruptedException {
        ExpireCallbackQueue<DataItem> queue = RedisExpireCallbackQueue.<DataItem>newBuilder()
                .withQueue("myQueue")
                .withJedisPool(jedisPool)
                .withPartitionsSupplier(() -> 100)
                .withBatchPopCount(50)
                .withSleepPeriod(1)
                .withPartition(item -> item.hashCode())
                .withEncoder(dataItem -> JsonUtil.toJson(dataItem))
                .withDecoder(jsonStr -> JsonUtil.fromJson(jsonStr, DataItem.class))
                .build();
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorService.scheduleAtFixedRate(consumer(queue), 0,60, TimeUnit.SECONDS);

        Thread.sleep(10000);
        assertEquals(10000, counter.get());
    }


    @Test
    void testProduceCount() {
        Jedis jedis = jedisPool.getResource();
        AtomicInteger atomicInteger = new AtomicInteger(0);
        String queueName = "myQueue";

        int count = 0;
        for (int i = 0; i < 100; i++) {
            count += jedis.zrange(queueName + "_" + i, 0, -1).size();
        }
        assertEquals(100, count);
    }

    @Test
    void test() throws InterruptedException {
        ExpireCallbackQueue<DataItem> queue = RedisExpireCallbackQueue.<DataItem>newBuilder()
                .withQueue("myQueue")
                .withJedisPool(jedisPool)
                .withPartitionsSupplier(() -> 100)
                .withBatchPopCount(50)
                .withPartition(item -> item.hashCode())
                .withEnableBufferTrigger(true)
                .withBatchCount(20)
                .withEncoder(dataItem -> JsonUtil.toJson(dataItem))
                .withDecoder(jsonStr -> JsonUtil.fromJson(jsonStr, DataItem.class))
                .build();

//        scheduledExecutorService.schedule(consumer(queue), 1, TimeUnit.SECONDS);

        for (int i = 0; i < 100; i++) {
            DataItem dataItem = new DataItem(i, "name" + i);
            queue.enqueue(dataItem, System.currentTimeMillis());
            if (i % 1000 == 0) {
                // 生产的慢一点
//                Thread.sleep(100);
            }
        }
        Thread.sleep(20000);
        assertEquals(counter.get(), 10000);
    }


    @Test
    void testBufferTrigger() throws InterruptedException {
        ExpireCallbackQueue<DataItem> queue = RedisExpireCallbackQueue.<DataItem>newBuilder()
                .withQueue("myQueue")
                .withJedisPool(jedisPool)
                .withPartitionsSupplier(() -> 100)
                .withBatchPopCount(50)
                .withPartition(item -> item.hashCode())
                .withEnableBufferTrigger(true)
                .withBatchCount(2000)
                .withEncoder(dataItem -> JsonUtil.toJson(dataItem))
                .withDecoder(jsonStr -> JsonUtil.fromJson(jsonStr, DataItem.class))
                .build();

        scheduledExecutorService.schedule(consumer(queue), 1, TimeUnit.SECONDS);

        for (int i = 0; i < 10000; i++) {
            DataItem dataItem = new DataItem(i, "name" + i);
            queue.enqueue(dataItem, System.currentTimeMillis());
            if (i % 1000 == 0) {
                // 生产的慢一点
                Thread.sleep(100);
            }
        }

        Thread.sleep(20000);
        assertEquals(10000, counter.get());
    }


    private Runnable consumer(ExpireCallbackQueue<DataItem> queue) {
        return () -> {
            queue.consume((item, date) -> {
                counter.getAndIncrement();
            }, null);
        };
    }

    static class DataItem {
        private long id;
        private String name;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DataItem dataItem = (DataItem) o;
            return id == dataItem.id &&
                    Objects.equals(name, dataItem.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, name);
        }

        public DataItem() {
        }

        public DataItem(long id, String name) {
            this.id = id;
            this.name = name;
        }

        public long getId() {
            return id;
        }

        public void setId(long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }
}
