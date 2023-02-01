package org.traffic.app;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;
import org.example.utils.DateTimeUtil;
import org.example.utils.MyKafkaUtil;
import org.traffic.bean.CarInfo;
import org.traffic.bean.OverSpeedInfo;
import org.traffic.bean.SpeedLimitInfo;
import org.traffic.common.TrafficConfig;
import org.traffic.util.CommonJDBCSink;

import java.sql.*;

/**
 * 卡口车辆速度超速分析
 * <p>
 * kafka---->flink--->主流数据
 * mysql(速度配置信息)---->flink--->广播配置流数据
 */
public class OverSpeedApp {
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

        //读取区域道路卡口限速信息(mysql数据) 封装成配置数据并进行广播,每隔半小时读取一次
        //todo 待优化 ---> flink cdc
        DataStreamSource<SpeedLimitInfo> speedLimitInfoDS = env.addSource(new RichSourceFunction<SpeedLimitInfo>() {
            Connection connection;
            PreparedStatement pstmt;

            boolean stop = false;

            //初始化mysql连接信息
            @Override
            public void open(Configuration parameters) throws Exception {
                connection = DriverManager.getConnection(TrafficConfig.MYSQL_URL, TrafficConfig.MYSQL_USER, TrafficConfig.MYSQL_PASSWORD);
                pstmt = connection.prepareStatement("select area_id,road_id,monitor_id,speed_limit from t_monitor_speed_limit");
            }

            //生产数据的方法，一般使用一个循环来读取mysql数据
            @Override
            public void run(SourceContext<SpeedLimitInfo> ctx) throws Exception {
                while (!stop) {
                    ResultSet resultSet = pstmt.executeQuery();
                    while (resultSet.next()) {

                        String areaId = resultSet.getString(1);
                        String roadId = resultSet.getString(2);
                        String monitorId = resultSet.getString(3);
                        Double speedLimit = Double.valueOf(resultSet.getString(4));
                        ctx.collect(new SpeedLimitInfo(areaId, roadId, monitorId, speedLimit));
                    }
                    //每隔半小时查询一次
                    Thread.sleep(30 * 60 * 1000);
                }
            }

            @Override
            public void cancel() {
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

        //限速信息广播
        MapStateDescriptor<String, Double> mapStateDescriptor = new MapStateDescriptor<>("speed_limit_info", String.class, Double.class);
        BroadcastStream<SpeedLimitInfo> speedLimitInfoBroadcastStream = speedLimitInfoDS.broadcast(mapStateDescriptor);

        //链接两个流
        SingleOutputStreamOperator<OverSpeedInfo> process = MonitorCarDS
                .connect(speedLimitInfoBroadcastStream)
                .process(new BroadcastProcessFunction<CarInfo, SpeedLimitInfo, OverSpeedInfo>() {
                    //针对主流数据处理
                    @Override
                    public void processElement(CarInfo carInfo, BroadcastProcessFunction<CarInfo, SpeedLimitInfo, OverSpeedInfo>.ReadOnlyContext ctx, Collector<OverSpeedInfo> out) throws Exception {
                        String areaId = carInfo.getAreaId();
                        String monitorId = carInfo.getMonitorId();
                        String getRoadId = carInfo.getRoadId();
                        String key = areaId + "_" + getRoadId + "_" + monitorId;
                        //获取广播状态
                        ReadOnlyBroadcastState<String, Double> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                        if (broadcastState.contains(key)) {
                            Double speedInfo = broadcastState.get(key);
                            if (carInfo.getSpeed() > speedInfo * 1.2) {
                                //超速行驶
                                out.collect(new OverSpeedInfo(
                                        carInfo.getCarId()
                                        , carInfo.getMonitorId()
                                        , carInfo.getRoadId()
                                        , carInfo.getSpeed()
                                        , speedInfo, DateTimeUtil.YmDHmstoTs(carInfo.getActionTime())));
                            }

                        } else {
                            //默认限速60
                            if (carInfo.getSpeed() > 60 * 1.2) {
                                out.collect(new OverSpeedInfo(
                                        carInfo.getCarId()
                                        , carInfo.getMonitorId()
                                        , carInfo.getRoadId()
                                        , carInfo.getSpeed()
                                        , 60.0, DateTimeUtil.YmDHmstoTs(carInfo.getActionTime())));
                            }
                        }
                    }

                    //处理广播流数据
                    @Override
                    public void processBroadcastElement(SpeedLimitInfo speedLimitInfo, BroadcastProcessFunction<CarInfo, SpeedLimitInfo, OverSpeedInfo>.Context ctx, Collector<OverSpeedInfo> out) throws Exception {
                        BroadcastState<String, Double> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                        String key = speedLimitInfo.getAreaId() + "_" + speedLimitInfo.getRoadId() + "_" + speedLimitInfo.getMonitorId();
                        System.out.println(key+"\t"+speedLimitInfo.getSpeedLimit());
                        broadcastState.put(key, speedLimitInfo.getSpeedLimit());
                    }
                });

        process.print("超速车辆");

        //将结果写入mysql表
        String sql = "insert into  t_overspeed_info(car_id,monitor_id,road_id,real_speed,limit_speed,action_time) values(?,?,?,?,?,?)";
        process.addSink(new CommonJDBCSink<OverSpeedInfo>(sql));


        env.execute("OverSpeedApp");
    }
}
