package org.traffic.app;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.example.utils.DateTimeUtil;
import org.traffic.bean.PoliceInfo;
import org.traffic.bean.ViolationInfo;

import java.time.Duration;

/**
 * 出警信息分析：双流join,intervalJoin,状态编程和触发器
 * <p>
 * kakfa topic A:违法车辆信息数据
 * 京G90639,涉嫌套牌,2023-02-01 21:12:44,xxxx
 * 京G90638,事故,2023-02-01 21:12:44
 * <p>
 * kakfa topic B:出警信息数据
 * pid1,京G90639,2023-02-01 21:12:55,成功
 */
public class PoliceAnalysis {

    public static void main(String[] args) throws Exception {

        //  执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // todo 数据源测试
        DataStreamSource<String> violationStream = env.socketTextStream("127.0.0.1", 9999);
        DataStreamSource<String> policeStream = env.socketTextStream("127.0.0.1", 8888);

        SingleOutputStreamOperator<ViolationInfo> violationInfoDS = violationStream.map(line -> {
            String[] arr = line.split(",");
            return new ViolationInfo(arr[0], arr[1], arr[2], arr[3]);
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<ViolationInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<ViolationInfo>() {
                    @Override
                    public long extractTimestamp(ViolationInfo violationInfo, long l) {
                        return DateTimeUtil.YmDHmstoTs(violationInfo.getCreateTime());
                    }
                }));


        SingleOutputStreamOperator<PoliceInfo> policeInfoDS = policeStream.map(line -> {
            String[] arr = line.split(",");
            return new PoliceInfo(arr[0], arr[1], arr[2], arr[3]);
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<PoliceInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<PoliceInfo>() {
                    @Override
                    public long extractTimestamp(PoliceInfo policeInfo, long l) {
                        return DateTimeUtil.YmDHmstoTs(policeInfo.getActionTime());
                    }
                }));

        //违法车辆和出警信息关联
        SingleOutputStreamOperator<String> process = violationInfoDS.keyBy(ViolationInfo::getCarId)
                .intervalJoin(policeInfoDS.keyBy(PoliceInfo::getCarId))
                .between(Time.seconds(-10), Time.seconds(10))
                //能匹配上的数据才会被process方法执行
                .process(new ProcessJoinFunction<ViolationInfo, PoliceInfo, String>() {
                    @Override
                    public void processElement(ViolationInfo violationInfo, PoliceInfo policeInfo, ProcessJoinFunction<ViolationInfo, PoliceInfo, String>.Context ctx, Collector<String> out) throws Exception {
                        String str = "车辆：" + violationInfo.getCarId() + "，违法时间：" + violationInfo.getCreateTime() + "，出警时间：" + policeInfo.getActionTime() + "，处理状态：成功";
                        out.collect(str);
                    }
                });

        process.print();

        //方法2：使用connect，使用定时器和状态编程
//        violationInfoDS.keyBy(ViolationInfo::getCarId).connect(policeInfoDS.keyBy(PoliceInfo::getCarId)
//                .process(new KeyedCoProcessFunction<String, ViolationInfo, PoliceInfo, String>() {
//
//                    //设置出警状态
//                    ValueState<PoliceInfo> policeState;
//
//                    //设置违法状态
//                    ValueState<ViolationInfo> violationState;
//
//                    @Override
//                    public void open(Configuration parameters) throws Exception {
//                        policeState = getRuntimeContext().getState(new ValueStateDescriptor<PoliceInfo>("policestate", PoliceInfo.class));
//                        violationState = getRuntimeContext().getState(new ValueStateDescriptor<ViolationInfo>("ViolationInfo", ViolationInfo.class));
//                    }
//
//                    //处理违法车辆
//                    @Override
//                    public void processElement1(ViolationInfo violationInfo, KeyedCoProcessFunction<String, ViolationInfo, PoliceInfo, String>.Context context, Collector<String> collector) throws Exception {
//                        PoliceInfo info = policeState.value();
//                        if (info != null) {
//                            //已经有出警信息
//                            collector.collect("当前车辆有出警信息");
//                            //删除定时器
//                            context.timerService().deleteEventTimeTimer(DateTimeUtil.YmDHmstoTs(info.getActionTime()) + 5000);
//                            //状态清空
//                            policeState.clear();
//
//
//                        } else {
//                            //当前车辆没有出警信息，就设置一个定时器，当前事件延迟5s后执行
//                            context.timerService().registerEventTimeTimer(DateTimeUtil.YmDHmstoTs(violationInfo.getCreateTime()) + 5000);
//                            //更新违法状态
//                            violationState.update(violationInfo);
//
//                        }
//                    }
//
//                    //处理出警信息
//                    @Override
//                    public void processElement2(PoliceInfo policeInfo, KeyedCoProcessFunction<String, ViolationInfo, PoliceInfo, String>.Context context, Collector<String> collector) throws Exception {
//                        ViolationInfo value = violationState.value();
//                        if (value != null) {
//                            //有违法信息
//                            collector.collect("当前车辆已出警，出警信息：" + policeInfo.toString());
//                            //取消定时器
//                            context.timerService().deleteEventTimeTimer(DateTimeUtil.YmDHmstoTs(value.getCreateTime()) + 5000);
//                            //清空状态
//                            violationState.clear();
//                        } else {
//                            //没有违法信息,设置定时器,当前出警时间+5s后触发
//                            context.timerService().registerEventTimeTimer(DateTimeUtil.YmDHmstoTs(policeInfo.getActionTime()) + 5000);
//                            //更新出警状态
//                            policeState.update(policeInfo);
//
//                        }
//                    }
//
//                    @Override
//                    public void onTimer(long timestamp, KeyedCoProcessFunction<String, ViolationInfo, PoliceInfo, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
//                        ViolationInfo violationInfoState = violationState.value();
//                        PoliceInfo policeInfoState = policeState.value();
//                        //有违法记录，没有出警记录
//                        if (violationInfoState != null) {
//                            out.collect("车辆：" + violationInfoState.getCarId() + "有违法记录， 在5秒内没有出警记录");
//                        }
//                        //有出警记录，没有违法记录
//                        if (policeInfoState != null) {
//                            out.collect("车辆：" + policeInfoState.getCarId() + "有出警记录， 在5秒内没有违法记录");
//                        }
//
//                        //清空状态
//                        violationState.clear();
//                        policeState.clear();
//
//                    }
//                });


        env.execute("PoliceAnalysis");

    }
}
