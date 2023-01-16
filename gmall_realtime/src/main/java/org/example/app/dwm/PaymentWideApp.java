package org.example.app.dwm;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.example.bean.OrderWide;
import org.example.bean.PaymentInfo;
import org.example.bean.PaymentWide;
import org.example.utils.MyKafkaUtil;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * 支付宽表
 */
public class PaymentWideApp {
    public static void main(String[] args) throws Exception {

        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //并行度设置与kafka主题分区数一致
        env.setParallelism(2);
        //读取kafka主题数据并转换为java对象 提取时间戳生成watermark
        String groupId = "payment_wide_group";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";

        SingleOutputStreamOperator<OrderWide> orderWideDS = env.fromSource(MyKafkaUtil.getKafkaConsumer(orderWideSourceTopic, groupId), WatermarkStrategy.noWatermarks(), "dwm_order_wide")
                .map(line -> JSON.parseObject(line, OrderWide.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderWide>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                    @Override
                    public long extractTimestamp(OrderWide orderWide, long recordTime) {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        try {
                            return sdf.parse(orderWide.getCreateTime()).getTime();
                        } catch (ParseException e) {
                            e.printStackTrace();
                            return recordTime;
                        }
                    }
                }));

        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = env.fromSource(MyKafkaUtil.getKafkaConsumer(paymentInfoSourceTopic, groupId), WatermarkStrategy.noWatermarks(), "dwd_payment_info")
                .map(line -> JSON.parseObject(line, PaymentInfo.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<PaymentInfo>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                    @Override
                    public long extractTimestamp(PaymentInfo paymentInfo, long recordTime) {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        try {
                            return Long.valueOf(paymentInfo.getCreateTime());
                        } catch (Exception e) {
                            e.printStackTrace();
                            return recordTime;
                        }
                    }
                }));

        //双流join
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoDS.keyBy(PaymentInfo::getOrderId)
                .intervalJoin(orderWideDS.keyBy(OrderWide::getOrderId))
                .between(Time.minutes(-15), Time.seconds(5))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>.Context context, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(paymentInfo, orderWide));
                    }
                });

        //数据写入kafka
        paymentWideDS.print();
//        paymentWideDS.map(JSON::toJSONString).sinkTo(MyKafkaUtil.getKafkaProducer(paymentWideSinkTopic));

        //启动任务
        env.execute("PaymentWideApp");

    }
}
