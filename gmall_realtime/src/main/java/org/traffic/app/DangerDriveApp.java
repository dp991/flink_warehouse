package org.traffic.app;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.example.utils.DateTimeUtil;
import org.example.utils.MyKafkaUtil;
import org.traffic.bean.CarInfo;
import org.traffic.bean.LimitCarInfo;
import org.traffic.bean.ViolationInfo;
import org.traffic.common.TrafficConfig;

import java.sql.*;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * 危险驾驶：在5分钟内，连续经过3个卡口且超速行驶即为危险驾驶
 * CEP复杂事件编程步骤：
 *  1、创建流
 *  2、定义模式
 *  3、事件流应用于模式
 *  4、选择事件
 */
public class DangerDriveApp {
    public static void main(String[] args) throws Exception {
        //  执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //消费traffic_monoitor数据,创建主流
        String sourceTopic = "traffic_monoitor";
        String groupId = "traffic_monoitor_app";
        DataStreamSource<String> kafkaDS = env.fromSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId), WatermarkStrategy.noWatermarks(), "traffic_monoitor_app");

        SingleOutputStreamOperator<CarInfo> MonitorCarDS = kafkaDS.map(line -> {
            String[] arr = line.split("\t");
            return new CarInfo(
                    arr[0].trim(), arr[1].trim(), arr[2].trim(), arr[3].trim(), arr[4].trim(), arr[5].trim(), Double.valueOf(arr[6].trim()));
        });
        //设置watermark、分配时间戳 解决乱序数据
        SingleOutputStreamOperator<CarInfo> transferDS = MonitorCarDS.assignTimestampsAndWatermarks(WatermarkStrategy.<CarInfo>forBoundedOutOfOrderness(Duration.ofSeconds(4))
                .withTimestampAssigner(new SerializableTimestampAssigner<CarInfo>() {
                    @Override
                    public long extractTimestamp(CarInfo carInfo, long l) {
                        return DateTimeUtil.YmDHmstoTs(carInfo.getActionTime());
                    }
                }));

        //todo 待优化
        SingleOutputStreamOperator<LimitCarInfo> overSpeedCarInfo = transferDS.map(new RichMapFunction<CarInfo, LimitCarInfo>() {
            Connection connection;
            PreparedStatement pstmt;
            boolean stop = false;

            @Override
            public void open(Configuration parameters) throws Exception {
                connection = DriverManager.getConnection(TrafficConfig.MYSQL_URL, TrafficConfig.MYSQL_USER, TrafficConfig.MYSQL_PASSWORD);
                pstmt = connection.prepareStatement("" +
                        "select area_id,road_id,monitor_id,speed_limit from t_monitor_speed_limit" +
                        " where area_id= ? and monitor_id= ? and road_id = ?");
            }

            @Override
            public LimitCarInfo map(CarInfo carInfo) throws Exception {
                String roadId = carInfo.getRoadId();
                String areaId = carInfo.getAreaId();
                String monitorId = carInfo.getMonitorId();
                pstmt.setString(1, areaId);
                pstmt.setString(2, monitorId);
                pstmt.setString(3, roadId);
                ResultSet resultSet = pstmt.executeQuery();
                Double defaultLimitSpeed = 60.0;
                while (resultSet.next()) {
                    defaultLimitSpeed = resultSet.getDouble("speed_limit");
                }
                return new LimitCarInfo(
                        areaId, roadId, monitorId, carInfo.getCameraId(), carInfo.getActionTime(), carInfo.getCarId(), carInfo.getSpeed(), defaultLimitSpeed);
            }

            @Override
            public void close() throws Exception {
                stop = true;
                try {
                    pstmt.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
                try {
                    connection.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        //定义模式
        Pattern<LimitCarInfo, LimitCarInfo> infoPattern = Pattern.<LimitCarInfo>begin("first").where(new SimpleCondition<LimitCarInfo>() {
            @Override
            public boolean filter(LimitCarInfo limitCarInfo) throws Exception {
                return limitCarInfo.getSpeed() > limitCarInfo.getLimitSpeed() * 1.2;
            }
        }).followedBy("second").where(new SimpleCondition<LimitCarInfo>() {
            @Override
            public boolean filter(LimitCarInfo limitCarInfo) throws Exception {
                return limitCarInfo.getSpeed() > limitCarInfo.getLimitSpeed() * 1.2;
            }
        }).followedBy("third").where(new SimpleCondition<LimitCarInfo>() {
            @Override
            public boolean filter(LimitCarInfo limitCarInfo) throws Exception {
                return limitCarInfo.getSpeed() > limitCarInfo.getLimitSpeed() * 1.2;
            }
        }).within(Time.seconds(10));

        //事件流应用模式
        PatternStream<LimitCarInfo> patternStream = CEP.pattern(overSpeedCarInfo.keyBy(mi -> mi.getCarId()), infoPattern);

        //选择事件
        SingleOutputStreamOperator<ViolationInfo> rsDS = patternStream.select(new PatternSelectFunction<LimitCarInfo, ViolationInfo>() {
            @Override
            public ViolationInfo select(Map<String, List<LimitCarInfo>> map) throws Exception {
                LimitCarInfo first = map.get("first").iterator().next();
                LimitCarInfo second = map.get("second").iterator().next();
                LimitCarInfo third = map.get("third").iterator().next();
                String info = "车辆涉嫌危险驾驶，第一次经过卡口时间: "+first.getActionTime()+"第二次经过卡口时间: "+second.getActionTime()+"第二次经过卡口时间: "+third.getActionTime();
                return new ViolationInfo(first.getCarId(), "危险驾驶", first.getActionTime(), info);
            }
        });

        rsDS.print();
        //todo sink到数据库


        env.execute("DangerDriveApp");
    }

}
