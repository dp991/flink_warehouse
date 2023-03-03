package org.liwei_data.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.example.function.CusomerDeserialization;
import org.example.utils.RedisUtil;
import org.liwei_data.bean.SightInfo;
import org.liwei_data.util.CommonUtil;
import org.liwei_data.util.JDBCSink;
import redis.clients.jedis.Jedis;

import java.util.Properties;

/**
 * 业务数据ods
 */
@Slf4j
public class SightInfoApp {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        Properties debeziumProperties = new Properties();
        debeziumProperties.put("snapshot.locking.mode", "none");// do not use lock
//        debeziumProperties.put("snapshot.mode", "initial");// do not use lock
        debeziumProperties.put("debezium.inconsistent.schema.handling.mode", "warn");

        SourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("127.0.0.1")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("liwei_base")
                .tableList("liwei_base.xc_sight_info")
                .deserializer(new CusomerDeserialization())
                .startupOptions(StartupOptions.latest()) //initial() 应该从时间戳为：2023-02-22 16:03:04
                .debeziumProperties(debeziumProperties)
                .build();

        DataStreamSource<String> streamSource = env.addSource(sourceFunction);//keep message ordering


        DataStream<SightInfo> sightDS = streamSource.map(json -> {
            JSONObject object = JSON.parseObject(json);
            JSONObject after = object.getJSONObject("after");
            String name = after.getString("name");
            String comment_score = after.getString("comment_score");
            String comment_count = after.getString("comment_count");
            String rank_class = after.getString("rank_class");
            after.put("rank_class", CommonUtil.getRankClass(name));
            after.put("name", CommonUtil.nameExcludeRankInfo(name));
            after.put("comment_count", CommonUtil.getCommentCount(comment_count));
            after.put("comment_score", CommonUtil.getCommentScore(comment_score));
            after.put("province", "");
            after.put("adcode", "");
            after.put("district", "");
            after.put("town", "");
            after.put("rank_info", rank_class);
            after.put("lon", 0.0);
            after.put("lat", 0.0);
            return JSON.parseObject(after.toJSONString(), SightInfo.class);
        });

        SingleOutputStreamOperator<SightInfo> mapDS = sightDS.map(new RichMapFunction<SightInfo, SightInfo>() {
            private transient Jedis jedis;

            @Override
            public void open(Configuration parameters) throws Exception {
                jedis = RedisUtil.getJedis();
            }

            @Override
            public SightInfo map(SightInfo sightInfo) throws Exception {
                String city = sightInfo.getCity();
                String address = sightInfo.getAddress();
                String name = sightInfo.getName();

                String newAddress1 = CommonUtil.getAddress(city, address);
                String regExp = "[\n`~!@#$%^&*()+=|{}':;',\\[\\].<>/?~！@#￥%……&*（）——+|{}【】‘；\r\t：”“’。， 、？]";
                String newAddress = newAddress1
                        .replace(" ", "")
                        .replace(" ", "")
                        .replaceAll(regExp, "")
                        .replace(" ", "")
                        .replaceAll("\"", "")
                        .replaceAll("\n", "")
                        .replaceAll("\r", "")
                        .replaceAll("\t", "")
                        .replaceAll("\\n","");

                String key = "Flink_" + city + "_" + name + "_" + newAddress;
                if (jedis.get(key) == null) {
                    try {
                        //经纬度进行赋值
                        CommonUtil.getRequest(sightInfo, newAddress);
                        if (sightInfo.getLon() == null || sightInfo.getLon() == 0 || sightInfo.getLon() == 0.0) {
                            newAddress = city + "市" + newAddress;
                            log.error("getRequest 第二次尝试,key={},address: {}", key, newAddress);
                            CommonUtil.getRequest(sightInfo, newAddress);
                        }

                        if (sightInfo.getLon() != null && sightInfo.getLon() > 0.0) {
                            //key加入到redis中
                            jedis.set(key, sightInfo.getLon() + "::" + sightInfo.getLat());
                        }
                    } catch (Exception e) {
                        log.error(e.getMessage());
                    }
                } else {
                    return null;
                }
                return sightInfo;
            }
        }).filter(s -> s != null && s.getLat() != null && s.getLon() != 0.0);

        String sql = "replace into sight_info(province,city,adcode,district,town,name,rank_class,heat_score,comment_score,comment_count,rank_info,address,open_info,phone,lon,lat) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        Integer batchSize = 10;
        mapDS.addSink(JDBCSink.mysqlSinkFunction(sql, batchSize));

        env.execute("FlinkCDC");

    }
}
