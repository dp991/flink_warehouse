package app;


import org.example.utils.RedisUtil;
import redis.clients.jedis.Jedis;

public class Main {
    public static void main(String[] args) {
        Jedis jedis = RedisUtil.getJedis();
        System.out.println(jedis.get("Flink_长沙_洞口金龙山_竹市镇金龙村"));
    }
}

