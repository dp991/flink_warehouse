package org.liwei_data.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import kong.unirest.HttpResponse;
import kong.unirest.Unirest;
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
public class FlinkCDC {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(5);

        Properties debeziumProperties = new Properties();
        debeziumProperties.put("snapshot.locking.mode", "none");// do not use lock
        debeziumProperties.put("debezium.inconsistent.schema.handling.mode", "warn");

        SourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("127.0.0.1")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("liwei_base")
                .tableList("liwei_base.xc_sight_info")
                .deserializer(new CusomerDeserialization())
                .startupOptions(StartupOptions.latest()) //initial()
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

            String url = "https://apis.map.qq.com/jsapi?qt=geoc&addr=%s&key=TU5BZ-MKD3W-L43RW-O3ZBW-GWMZK-QBB25&output=jsonp&pf=jsapi&ref=jsapi";

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
                        .replaceAll("\"", "");
                String xyURL = String.format(url, newAddress);
                String key = "Flink_" + city + "_" + name + "_" + newAddress;
                if (jedis.get(key) == null) {
                    Double x = 0.0;
                    Double y = 0.0;
                    try {
                        Thread.sleep(800);
                        // todo 可以使用异步请求
                        HttpResponse<String> response = Unirest.get(xyURL)
                                .asString();
                        if (response.getStatus() == 200) {
                            JSONObject jsonObject = JSON.parseObject(response.getBody());
                            JSONObject detail = jsonObject.getJSONObject("detail");
                            if (detail != null) {
                                x = detail.getDouble("pointx");
                                y = detail.getDouble("pointy");
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("address: " + sightInfo.getAddress());
                        System.out.println(e.getMessage());
                    }
                    sightInfo.setLon(x);
                    sightInfo.setLat(y);
                    jedis.set(key, x + "," + y);
                } else {
                    String xAndY = jedis.get(key);
                    String[] split = xAndY.split(",");
                    sightInfo.setLon(Double.valueOf(split[0]));
                    sightInfo.setLat(Double.valueOf(split[1]));
                }
                return sightInfo;
            }
        });

        String sql = "replace into sight_info_final(city,name,rank_class,heat_score,comment_score,comment_count,rank_info,address,open_info,phone,lon,lat) values(?,?,?,?,?,?,?,?,?,?,?,?)";
        Integer batchSize = 100;
        mapDS.addSink(JDBCSink.mysqlSinkFunction(sql, batchSize));


        env.execute("FlinkCDC");

    }
}
