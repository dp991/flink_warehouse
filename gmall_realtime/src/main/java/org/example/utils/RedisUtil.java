package org.example.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * redis工具类
 */
public class RedisUtil {

    public static JedisPool jedisPool = null;
    private static String JEDIS_HOST = "127.0.0.1";

    public static Jedis getJedis() {
        if (jedisPool == null) {
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            jedisPoolConfig.setMaxTotal(100);
            jedisPoolConfig.setBlockWhenExhausted(true);
            jedisPoolConfig.setMaxWaitMillis(2000);
            jedisPoolConfig.setMaxIdle(5);
            jedisPoolConfig.setMinIdle(5);
            jedisPoolConfig.setTestOnBorrow(true);

            jedisPool = new JedisPool(jedisPoolConfig, JEDIS_HOST, 6379, 1000);

            System.out.println("创建连接池");
            return jedisPool.getResource();
        } else {
//            System.out.println(" 连接池活跃连接数：" + jedisPool.getNumActive());
            return jedisPool.getResource();
        }
    }
}
