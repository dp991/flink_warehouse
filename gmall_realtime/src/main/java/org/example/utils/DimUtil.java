package org.example.utils;

import com.alibaba.fastjson.JSONObject;
import org.example.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.util.List;

/**
 * 查询维度信息
 */
public class DimUtil {

    public static JSONObject getDimInfo(Connection connection, String tableName, String id) throws Exception {

        //查询phoenix前查询redis
        Jedis jedis = RedisUtil.getJedis();
        String redisKey = "DIM:" + tableName + ":" + id;
        String dimInfoJsonStr = jedis.get(redisKey);
        if (dimInfoJsonStr != null) {
            //重置过期时间
            jedis.expire(redisKey, 24 * 60 * 60);
            //归还链接
            jedis.close();
            return JSONObject.parseObject(dimInfoJsonStr);
        }

        //拼接查询语句
        String sql = "SELECT * FROM " + GmallConfig.MYSQL_DATABASE_NAME + "." + tableName + " where id = '" + id + "'";
        //查询phoenix
        List<JSONObject> jsonObjects = JDBCUtil.queryList(connection, sql, JSONObject.class, false);

        JSONObject dimInfoJson = jsonObjects.get(0);
        //在返回结果之前将数据写入redis
        jedis.set(redisKey, dimInfoJson.toJSONString());
        jedis.expire(redisKey, 24 * 60 * 60);
        jedis.close();

        return dimInfoJson;

    }

    /**
     * 删除redis中的数据
     *
     * @param tableName
     * @param id
     */
    public static void delRedisDimInfo(String tableName, String id) {
        Jedis jedis = RedisUtil.getJedis();
        String redisKey = "DIM:" + tableName + ":" + id;
        jedis.decr(redisKey);
        jedis.close();
    }
}
