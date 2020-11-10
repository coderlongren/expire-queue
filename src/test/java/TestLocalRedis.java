import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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
}
