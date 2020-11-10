package com.github.coderlong;

import static java.lang.System.currentTimeMillis;

import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;

/**
 * @author coderlongren
 * 过期时候可以callBack的队列
 * @param <T>
 */
public interface ExpireCallbackQueue<T> {

    /**
     *  带过期时间，入队
     * @param object
     * @param expireAt (时间戳)
     */
    void enqueue(T object, long expireAt);

    /**
     * 扩展 Date类型
     * @param object
     * @param expireAt Java Date
     */
    default void enqueue(T object, Date expireAt) {
        enqueue(object, expireAt.getTime());
    }
    default void enqueue(T object, long expireTime, TimeUnit unit) {
        enqueue(object, currentTimeMillis() + unit.toMillis(expireTime));
    }


    default void consume(BiConsumer<T, Date> callback, BooleanSupplier stopSignal) {
        consume((t1, t2) -> {
            callback.accept(t1, t2);
            return CompletableFuture.completedFuture(true);
        }, stopSignal);
    }

    void consume(BiFunction<T, Date, CompletableFuture<Boolean>> callback,
            BooleanSupplier stopSignal);

    Date getExpireTime(T obj);

    default void consume(BiConsumer<T, Date> callback) {
        consume(callback, null);
    }

    default void consumeBatch(BiConsumer<T, Date> callback) {
        consumeBatch(callback, null);
    }

    default void consumeBatch(BiConsumer<T, Date> callback, BooleanSupplier stopSignal) {
        consumeBatch((t1, t2) -> {
            callback.accept(t1, t2);
            return CompletableFuture.completedFuture(true);
        }, stopSignal);
    }

    default void consumeBatch(BiFunction<T, Date, CompletableFuture<Boolean>> callback,
            BooleanSupplier stopSignal) {
        throw new UnsupportedOperationException("No implementation was found");
    }
}
