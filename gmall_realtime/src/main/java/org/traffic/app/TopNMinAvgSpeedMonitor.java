package org.traffic.app;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.utils.DateTimeUtil;
import org.example.utils.MyKafkaUtil;
import org.traffic.bean.CarInfo;
import org.traffic.bean.MonitorSpeedCountInfo;
import org.traffic.bean.SpeedCountInfo;

import java.time.Duration;
import java.util.*;

/**
 * 统计每隔1分钟统计最近5分钟，每个区域最通常的卡口
 */
public class TopNMinAvgSpeedMonitor {
    public static void main(String[] args) throws Exception {
        //  执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //消费traffic_monoitor数据,创建主流
        String sourceTopic = "traffic_monoitor";
        String groupId = "traffic_monoitor_app";
//        DataStreamSource<String> kafkaDS = env.fromSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId), WatermarkStrategy.noWatermarks(), "traffic_monoitor_app");

        DataStreamSource<String> kafkaDS = env.socketTextStream("127.0.0.1",9999);
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

        //每隔1分钟，统计过去5分钟最通畅的卡口
        SingleOutputStreamOperator<MonitorSpeedCountInfo> top5DS = transferDS.windowAll(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1))).process(new ProcessAllWindowFunction<CarInfo, MonitorSpeedCountInfo, TimeWindow>() {
            @Override
            public void process(ProcessAllWindowFunction<CarInfo, MonitorSpeedCountInfo, TimeWindow>.Context ctx, Iterable<CarInfo> iterable, Collector<MonitorSpeedCountInfo> out) throws Exception {
                HashMap<String, SpeedCountInfo> map = new HashMap<String, SpeedCountInfo>();
                Iterator<CarInfo> iterator = iterable.iterator();
                while (iterator.hasNext()) {
                    CarInfo carInfo = iterator.next();
                    String areaId = carInfo.getAreaId();
                    String roadId = carInfo.getRoadId();
                    String monitorId = carInfo.getMonitorId();
                    Double speed = carInfo.getSpeed();
                    String key = areaId + "_" + roadId + "_" + monitorId;
                    if (map.containsKey(key)) {
                        //判断当前车辆速度
                        SpeedCountInfo speedCountInfo = map.get(key);
                        if (speed >= 120.0) {
                            speedCountInfo.setHighSpeedCount(speedCountInfo.getHighSpeedCount() + 1);
                        } else if (speed >= 90.0) {
                            speedCountInfo.setMiddleSpeedCount(speedCountInfo.getMiddleSpeedCount() + 1);
                        } else if (speed >= 60.0) {
                            speedCountInfo.setNormalSpeedCount(speedCountInfo.getNormalSpeedCount() + 1);
                        } else {
                            speedCountInfo.setLowSpeedCount(speedCountInfo.getLowSpeedCount() + 1);
                        }

                    } else {
                        if (speed >= 120.0) {
                            map.put(key, new SpeedCountInfo(1L, 0L, 0L, 0L));
                        } else if (speed >= 90.0) {
                            map.put(key, new SpeedCountInfo(0L, 1L, 0L, 0L));
                        } else if (speed >= 60.0) {
                            map.put(key, new SpeedCountInfo(0L, 0L, 1L, 0L));
                        } else {
                            map.put(key, new SpeedCountInfo(0L, 0L, 0L, 1L));
                        }

                    }
                }

                //map对象排序
                ArrayList<Map.Entry<String, SpeedCountInfo>> arrayList = new ArrayList<>(map.entrySet());
                Collections.sort(arrayList, ((o1, o2) -> {
                    if (o1.getValue().getHighSpeedCount() != o2.getValue().getHighSpeedCount()) {
                        return (int) (o1.getValue().getHighSpeedCount() - o2.getValue().getHighSpeedCount());
                    } else if (o1.getValue().getMiddleSpeedCount() != o2.getValue().getMiddleSpeedCount()) {
                        return (int) (o1.getValue().getMiddleSpeedCount() - o2.getValue().getMiddleSpeedCount());
                    } else if (o1.getValue().getNormalSpeedCount() != o2.getValue().getNormalSpeedCount()) {
                        return (int) (o1.getValue().getNormalSpeedCount() - o2.getValue().getNormalSpeedCount());
                    } else {
                        return (int) (o1.getValue().getLowSpeedCount() - o2.getValue().getLowSpeedCount());
                    }
                }));

                //取前5名
                arrayList.stream().limit(5).forEach(element -> {
                    out.collect(new MonitorSpeedCountInfo(
                            DateTimeUtil.toYMDhms(new Date(ctx.window().getStart())),
                            DateTimeUtil.toYMDhms(new Date(ctx.window().getEnd())),
                            element.getKey(),
                            element.getValue().getHighSpeedCount(),
                            element.getValue().getMiddleSpeedCount(),
                            element.getValue().getNormalSpeedCount(),
                            element.getValue().getLowSpeedCount()));
                });

            }
        });

        top5DS.print();

        //todo jdbc sink


        env.execute("TopNMinAvgSpeedMonitor");
    }
}
