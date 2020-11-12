
[![Coverage Status](https://coveralls.io/repos/github/coderlongren/expire-queue/badge.svg?branch=main)](https://coveralls.io/github/coderlongren/expire-queue?branch=main)
[![Total alerts](https://img.shields.io/lgtm/alerts/g/coderlongren/expire-queue.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/coderlongren/expire-queue/alerts/)

## 过期Queue 消费， 支持关系型DB & Redis 
- Java8
- Redis

```java
Function<DataItem, Integer> partition = (item -> item.hashCode());
ExpireCallbackQueue<DataItem> queue = RedisExpireCallbackQueue.<DataItem>newBuilder()
        .withQueue("myQueue")
        .withJedisPool(jedisPool)
        .withPartitions(100)
        .withBatchPopCount(50)
        .withPartition(partition)
        .withEncoder(dataItem -> JsonUtil.toJson(dataItem))
        .withDecoder(jsonStr -> JsonUtil.fromJson(jsonStr, DataItem.class))
        .build();

ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
scheduledExecutorService.schedule(consumer(queue), 1, TimeUnit.SECONDS);

for (int i = 0; i < 10000; i++) {
    DataItem dataItem = new DataItem(i, "name" + i);
    queue.enqueue(dataItem, System.currentTimeMillis());
    if (i % 1000 == 0) {
        // 生产的慢一点
        Thread.sleep(100);
    }
}
Thread.sleep(10000);
assertEquals(counter.get(), 10000);



private Runnable consumer(ExpireCallbackQueue<DataItem> queue) {
    return () -> {
        queue.consume((item, date) -> {
            counter.getAndIncrement();
        }, null);
    };
}
```