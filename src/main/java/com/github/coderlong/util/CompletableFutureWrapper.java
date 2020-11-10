package com.github.coderlong.util;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

/**
 * @author rensailong
 * Created on 2020-11-01
 */
public class CompletableFutureWrapper {
    /**
     * 构造私有
     */
    private CompletableFutureWrapper() {
    }

    /**
     * 包装成带超时的CompletableFuture
     */
    public static <T> CompletableFuture<T> orTimeout(final CompletableFuture<T> future, final Duration duration) {
        StackTraceElement[] currentStacks = Thread.currentThread().getStackTrace();
        final CompletableFuture<T> timeout = timeout(duration, currentStacks);
        return future.applyToEither(timeout, Function.identity());
    }

    /**
     * 超时任务, 参考Java9超时实现
     */
    private static <T> CompletableFuture<T> timeout(final Duration duration, StackTraceElement[] currentStacks) {
        final CompletableFuture<T> promise = new CompletableFuture<>();
        DELAYER.schedule(() -> {
            final TimeoutException ex = new TimeoutException("Timeout after " + duration);
            ex.setStackTrace(Arrays.stream(currentStacks)
                    .toArray(StackTraceElement[]::new));
            return promise.completeExceptionally(ex);
        }, duration.toMillis(), MILLISECONDS);
        return promise;
    }

    /**
     * Scheduled 线程池
     */
    private static final ScheduledThreadPoolExecutor DELAYER;

    static {
        DELAYER = new ScheduledThreadPoolExecutor(1, new DaemonThreadFactory());
        DELAYER.setRemoveOnCancelPolicy(true);
    }

    static final class DaemonThreadFactory implements ThreadFactory {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName("CompletableFutureDelayScheduler");
            return t;
        }
    }
}
