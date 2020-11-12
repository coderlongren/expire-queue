import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.github.coderlong.util.CompletableFutureWrapper;

import redis.clients.jedis.Jedis;

public class TestLocalRedis {
    private static Jedis jedis;
    @BeforeAll
    static void init() {
        jedis = new Jedis("127.0.0.1", 6379);
    }

    @Test
    void test() {
        System.out.println(jedis.get("a"));
    }

    @Test
    void testTmeOut() {
        CompletableFutureWrapper.orTimeout(CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }), Duration.ofSeconds(1)).whenComplete((res, err) -> {
            System.out.println( res + ";;" + err);
        }).exceptionally(err -> {
            System.out.println(err);
            return null;
        }).join();
    }
}
