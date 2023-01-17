package org.example.utils;

import org.example.common.GmallConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * redis工具类
 */
public class RedisUtil {

    public static JedisPool jedisPool = null;

    /**
     * redis 连接池
     *
     * @return
     */
    public static Jedis getJedis() {

        if (jedisPool == null) {
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            jedisPoolConfig.setMaxTotal(100);
            jedisPoolConfig.setBlockWhenExhausted(true);
            jedisPoolConfig.setMaxWaitMillis(2000);
            jedisPoolConfig.setMaxIdle(5);
            jedisPoolConfig.setMinIdle(5);
            jedisPoolConfig.setTestOnBorrow(true);

            jedisPool = new JedisPool(jedisPoolConfig, GmallConfig.JEDIS_HOST, GmallConfig.JEDIS_PORT, GmallConfig.TIME_OUT);

            return jedisPool.getResource();

        } else {
//            System.out.println(" 连接池活跃连接数：" + jedisPool.getNumActive());
            return jedisPool.getResource();
        }
    }
}
