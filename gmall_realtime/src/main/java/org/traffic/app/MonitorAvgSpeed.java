package org.traffic.app;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.utils.DateTimeUtil;
import org.example.utils.MyKafkaUtil;
import org.traffic.bean.CarInfo;
import org.traffic.bean.MonitorAvgSpeedInfo;
import org.traffic.util.JDBCSink;

import java.time.Duration;

/**
 * 卡口的拥堵情况：每隔1分钟计算过去5分钟卡口车辆的平均速度：总速度/总车辆数
 */
public class MonitorAvgSpeed {
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

        //设置watermark、分配时间戳
        SingleOutputStreamOperator<CarInfo> transferDS = MonitorCarDS.assignTimestampsAndWatermarks(WatermarkStrategy.<CarInfo>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<CarInfo>() {
                    @Override
                    public long extractTimestamp(CarInfo carInfo, long l) {
                        return DateTimeUtil.YmDHmstoTs(carInfo.getActionTime());
                    }
                }));

        //设置窗口，分组计算每隔1分钟每个卡口的平均速度,增量计算窗口aggregate+全量计算
        SingleOutputStreamOperator<MonitorAvgSpeedInfo> avgSpeedDS = transferDS
                .keyBy(mi -> mi.getAreaId() + "_" + mi.getRoadId() + "_" + mi.getMonitorId())
                .window(SlidingProcessingTimeWindows.of(Time.minutes(5), Time.minutes(1)))
                .aggregate(new AggregateFunction<CarInfo, Tuple2<Long, Double>, Tuple2<Long, Double>>() {
                    //初始化
                    @Override
                    public Tuple2<Long, Double> createAccumulator() {
                        return new Tuple2<>(0L, 0.0);
                    }

                    @Override
                    public Tuple2<Long, Double> add(CarInfo carInfo, Tuple2<Long, Double> t) {

                        //(车辆总和，速度总和)
                        return new Tuple2<>(t.f0 + 1, t.f1 + carInfo.getSpeed());
                    }

                    @Override
                    public Tuple2<Long, Double> getResult(Tuple2<Long, Double> acc1) {
                        return acc1;
                    }

                    @Override
                    public Tuple2<Long, Double> merge(Tuple2<Long, Double> acc1, Tuple2<Long, Double> acc2) {
                        return new Tuple2<>(acc1.f0 + acc1.f0, acc1.f1 + acc2.f1);
                    }
                }, new WindowFunction<Tuple2<Long, Double>, MonitorAvgSpeedInfo, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow timeWindow, Iterable<Tuple2<Long, Double>> iterable, Collector<MonitorAvgSpeedInfo> out) throws Exception {
                        long start = timeWindow.getStart();
                        long end = timeWindow.getEnd();
                        Tuple2<Long, Double> element = iterable.iterator().next();
                        Double avgSpeed = Double.valueOf(String.format("%.2f", element.f1 / element.f0));
                        out.collect(new MonitorAvgSpeedInfo(start, end, key, avgSpeed, element.f0));
                    }
                });

        //保存到数据库
        String sql = "insert into t_average_speed(start_time, end_time,monitor_id,avg_speed,car_count) values(?,?,?,?,?)";
        avgSpeedDS.addSink(JDBCSink.mysqlSinkFunction(sql,1000));

        env.execute("MonitorAvgSpeed");
    }
}
