package org.traffic.app;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.utils.DateTimeUtil;
import org.example.utils.MyKafkaUtil;
import org.traffic.bean.CarInfo;
import org.traffic.bean.ViolationInfo;

/**
 * 套牌车辆实时报警分析
 */
public class RepeatCarInfoApp {
    public static void main(String[] args) throws Exception {
        //  执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //消费traffic_monoitor数据,创建主流
        String sourceTopic = "traffic_monoitor";
        String groupId = "traffic_monoitor_app";
        DataStreamSource<String> kafkaDS = env.fromSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId), WatermarkStrategy.noWatermarks(), "traffic_monoitor_app");

        //测试
//        DataStreamSource<String> kafkaDS = env.socketTextStream("127.0.0.1",9999);

        SingleOutputStreamOperator<CarInfo> monitorCarDS = kafkaDS.map(line -> {
            String[] arr = line.split("\t");
            return new CarInfo(
                    arr[0].trim(), arr[1].trim(), arr[2].trim(), arr[3].trim(), arr[4].trim(), arr[5].trim(), Double.valueOf(arr[6].trim()));
        });

        //业务处理 状态编程 统计在10s内通过两个卡口的套牌车辆
        SingleOutputStreamOperator<ViolationInfo> violidationDS = monitorCarDS
                .keyBy(carInfo -> carInfo.getCarId()).process(new KeyedProcessFunction<String, CarInfo, ViolationInfo>() {
                    //给每个车辆设置状态，状态中存储的是每辆车经过卡口的时间
                    ValueState<String> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("valuestate", String.class));
                    }

                    @Override
                    public void processElement(CarInfo carInfo, KeyedProcessFunction<String, CarInfo, ViolationInfo>.Context ctx, Collector<ViolationInfo> out) throws Exception {
                        String actionTime = carInfo.getActionTime();
                        String time = valueState.value();
                        if (time == null || time.length() == 0) {
                            //没有状态
                            valueState.update(actionTime);
                        } else {
                            //有状态,获取状态值，并与本条时间做差值，小于10s即为套牌车
                            Long pre = DateTimeUtil.YmDHmstoTs(time);
                            Long current = DateTimeUtil.YmDHmstoTs(actionTime);
                            if (Math.abs(pre - current) <= 10 * 1000) {
                                //违法车辆
                                out.collect(new ViolationInfo(carInfo.getCarId(), "套牌车辆", carInfo.getActionTime(),
                                        "车辆：" + carInfo.getCarId() + " 涉嫌套牌,上次经过卡口时间：" + time + ",本次经过时间：" + actionTime));
                            }
                            //更新状态
                            String newTime = pre >= current ? time : actionTime;
                            valueState.update(newTime);
                        }

                    }
                });

        violidationDS.print();

        env.execute("RepeatCarInfoApp");
    }
}
