package org.traffic.app;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.utils.DateTimeUtil;
import org.example.utils.MyKafkaUtil;
import org.traffic.bean.CarInfo;

import java.time.Duration;
import java.util.HashSet;
import java.util.Iterator;

/**
 * 实时统计某个区域车辆总数：布隆过滤器
 */
public class RealTimeCarCountApp {
    public static void main(String[] args) throws Exception {
//  执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //消费traffic_monoitor数据,创建主流
        String sourceTopic = "traffic_monoitor";
        String groupId = "traffic_monoitor_app";
        DataStreamSource<String> kafkaDS = env.fromSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId), WatermarkStrategy.noWatermarks(), "traffic_monitor_app");

        SingleOutputStreamOperator<CarInfo> MonitorCarDS = kafkaDS.map(line -> {
            String[] arr = line.split("\t");
            return new CarInfo(
                    arr[0].trim(), arr[1].trim(), arr[2].trim(), arr[3].trim(), arr[4].trim(), arr[5].trim(), Double.valueOf(arr[6].trim()));
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<CarInfo>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner(new SerializableTimestampAssigner<CarInfo>() {
            @Override
            public long extractTimestamp(CarInfo carInfo, long l) {
                return DateTimeUtil.YmDHmstoTs(carInfo.getActionTime());
            }
        }));

        //每隔1分钟每个区域车辆的总数
        MonitorCarDS.keyBy(CarInfo::getAreaId)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .apply(new WindowFunction<CarInfo, String, String, TimeWindow>() {
                    //iterable：当前窗口的数据
                    HashSet<String> carSet = new HashSet<String>();

                    @Override
                    public void apply(String s, TimeWindow timeWindow, Iterable<CarInfo> iterable, Collector<String> out) throws Exception {
                        Iterator<CarInfo> iterator = iterable.iterator();
                        while (iterator.hasNext()) {
                            CarInfo carInfo = iterator.next();
                            carSet.add(carInfo.getCarId());
                        }
                        out.collect("当前区域：" + s + ",车辆总数为：" + carSet.size());
                    }
                }).print();


        env.execute("RealTimeCarCountApp");
    }
}


