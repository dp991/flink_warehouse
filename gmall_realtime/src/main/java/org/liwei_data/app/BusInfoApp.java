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
import org.liwei_data.bean.BusInfo;
import org.liwei_data.util.CommonUtil;
import org.liwei_data.util.JDBCSink;
import redis.clients.jedis.Jedis;

import java.util.Properties;

/**
 * 业务数据ods
 */
@Slf4j
public class BusInfoApp {
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
                .tableList("liwei_base.bus_8684")
                .deserializer(new CusomerDeserialization())
                .startupOptions(StartupOptions.latest()) //initial()
                .debeziumProperties(debeziumProperties)
                .build();

        DataStreamSource<String> streamSource = env.addSource(sourceFunction);//keep message ordering


        DataStream<BusInfo> busInfoDS = streamSource.map(json -> {
            JSONObject object = JSON.parseObject(json);
            JSONObject after = object.getJSONObject("after");

            String lineName = after.getString("line_name");
            String price = after.getString("price");
            String company = after.getString("company");
            after.put("price_info", price);
            if (company != null){
                after.put("company", company.replace("公交公司：", "").trim());
            }else {
                after.put("company", "");
            }


            after.put("bus_line_name", after.getString("bus_name"));

            after.remove("price");
            after.remove("bus_name");

            after.put("line_name", lineName.replace("[", "").replace("]", ""));
            after.put("address", "");
            after.put("province", "");
            after.put("adcode", "");
            after.put("district", "");
            after.put("town", "");
            after.put("lon", 0.0);
            after.put("lat", 0.0);
            return JSON.parseObject(after.toJSONString(), BusInfo.class);
        });

        SingleOutputStreamOperator<BusInfo> mapDS = busInfoDS.map(new RichMapFunction<BusInfo, BusInfo>() {
            private transient Jedis jedis;

            @Override
            public void open(Configuration parameters) throws Exception {
                jedis = RedisUtil.getJedis();
            }

            @Override
            public BusInfo map(BusInfo bus) throws Exception {
                String city = bus.getCity();
                String stationName = bus.getStation_name().trim();
                String lineName = bus.getLine_name();
                String forward = bus.getForward();

                if (!stationName.contains("公交站")) {
                    stationName = stationName + "公交站";
                }
                String address = city + stationName;
                bus.setAddress(address.trim());

                String key = city + "_" + lineName + "_" + forward + "_" + stationName;

                if (address.trim().length() > 0) {
                    if (jedis.get(key) == null) {
                        try {
                            //经纬度进行赋值
                            CommonUtil.getRequest(bus, address);
                            if (bus.getLon() == null || bus.getLon() == 0 || bus.getLon() == 0.0) {
                                log.error("getRequest 第二次尝试,key={},address: {}", key, stationName);
                                CommonUtil.getRequest(bus, city + "市" + stationName);
                            }

                            if (bus.getLon() != null && bus.getLon() > 0.0) {
                                //key加入到redis中
                                if (bus.getAddress() == null || bus.getAddress().length() == 0) {
                                    jedis.set(key, bus.getLon() + "::" + bus.getLat() + "::" + bus.getProvince() + "::" + bus.getCity() + "::" + bus.getAdcode() + "::" + bus.getDistrict() + "::" + bus.getTown() + "::" + address);
                                    bus.setAddress(address);
                                } else {
                                    jedis.set(key, bus.getLon() + "::" + bus.getLat() + "::" + bus.getProvince() + "::" + bus.getCity() + "::" + bus.getAdcode() + "::" + bus.getDistrict() + "::" + bus.getTown() + "::" + bus.getAddress());
                                }
                            }
                        } catch (Exception e) {
                            log.error(e.getMessage());
                        }
                    } else {
//                    String rs = jedis.get(key);
//                    String[] arr = jedis.get(key).split("::");
//                    System.out.println("redis key 已存在：key = " + key + " , value = " + rs);
//                    bus.setCity(arr[3]);
//                    bus.setLon(Double.valueOf(arr[0]));
//                    bus.setLat(Double.valueOf(arr[1]));
//                    bus.setProvince(arr[2]);
//                    bus.setAdcode(arr[4]);
//                    bus.setDistrict(arr[5]);
//                    bus.setTown(arr[6]);
//                    bus.setAddress(arr[7]);
                        return null;
                    }
                } else {
                    return null;
                }
                return bus;
            }
        }).filter(s -> s != null && s.getLat() != null && s.getLon() != 0.0);
//
        String sql = "insert into bus_station_info(province,city,adcode,district,town,line_name,bus_line_name,forward,station_name,address,run_time,run_time_desc,company,price_info,station_id,lon,lat) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        Integer batchSize = 500;
        mapDS.addSink(JDBCSink.mysqlSinkFunction(sql, batchSize));

        env.execute("FlinkCDC");

    }
}
