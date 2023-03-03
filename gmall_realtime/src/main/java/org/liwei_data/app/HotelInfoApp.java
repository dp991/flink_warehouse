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
import org.liwei_data.bean.HotelInfo;
import org.liwei_data.util.CommonUtil;
import org.liwei_data.util.JDBCSink;
import redis.clients.jedis.Jedis;

import java.util.Properties;

/**
 * 业务数据ods
 */
@Slf4j
public class HotelInfoApp {
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
                .tableList("liwei_base.xc_hotel_info")
                .deserializer(new CusomerDeserialization())
                .startupOptions(StartupOptions.latest()) //initial() 应该从时间戳为：2023-02-22 16:03:04
                .debeziumProperties(debeziumProperties)
                .build();

        DataStreamSource<String> streamSource = env.addSource(sourceFunction);//keep message ordering


        DataStream<HotelInfo> hotelInfoDS = streamSource.map(json -> {
            JSONObject object = JSON.parseObject(json);
            JSONObject after = object.getJSONObject("after");
            String score = after.getString("score");
            String address = after.getString("address");
            String commentCount = after.getString("comment_count");
            String city = after.getString("city");
            String price = after.getString("price");

            after.put("price", CommonUtil.getPrice(price));
            after.put("score", CommonUtil.getHotelScore(score));
            after.put("analysis_address", CommonUtil.getHotelAddress(city, address));
            after.put("comment_count", CommonUtil.getHotelCommentCount(commentCount));

            after.put("province", "");
            after.put("adcode", "");
            after.put("district", "");
            after.put("town", "");
            after.put("lon", 0.0);
            after.put("lat", 0.0);
            return JSON.parseObject(after.toJSONString(), HotelInfo.class);
        });

        SingleOutputStreamOperator<HotelInfo> mapDS = hotelInfoDS.map(new RichMapFunction<HotelInfo, HotelInfo>() {
            private transient Jedis jedis;

            @Override
            public void open(Configuration parameters) throws Exception {
                jedis = RedisUtil.getJedis();
            }

            @Override
            public HotelInfo map(HotelInfo hotelInfo) throws Exception {
                String city = hotelInfo.getCity();
                String address = hotelInfo.getAddress();
                String analysisAddress = hotelInfo.getAnalysis_address();
                String hotelName = hotelInfo.getHotel_name();


                String key = "hotel_" + city + "_" + hotelName + "_" + address;
                if (jedis.get(key) == null) {
                    try {
                        //经纬度进行赋值
                        CommonUtil.getRequest(hotelInfo, analysisAddress);
                        if (hotelInfo.getLon() == null || hotelInfo.getLon() == 0 || hotelInfo.getLon() == 0.0) {
                            if (!hotelName.contains(city)){
                                hotelName = city+hotelName;
                            }
                            log.error("getRequest 第二次尝试,key={},address: {}", key, hotelName);
                            CommonUtil.getRequest(hotelInfo, hotelName);
                        }

                        if (hotelInfo.getLon() != null && hotelInfo.getLon() > 0.0) {
                            //key加入到redis中
                            jedis.set(key, hotelInfo.getLon() + "::" + hotelInfo.getLat());
                        }
                    } catch (Exception e) {
                        log.error(e.getMessage());
                    }
                } else {
                    return null;
                }
                return hotelInfo;
            }
        }).filter(s -> s != null && s.getLat() != null && s.getLon() != 0.0);

        String sql = "insert into hotel_info(province,city,adcode,district,town,hotel_name,hotel_type,score,comment,comment_count,address,analysis_address,price,picture,lon,lat) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        Integer batchSize = 10;
        mapDS.addSink(JDBCSink.mysqlSinkFunction(sql, batchSize));

        env.execute("FlinkCDC");

    }
}
