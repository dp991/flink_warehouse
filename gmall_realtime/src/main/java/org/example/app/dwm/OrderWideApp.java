package org.example.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.example.bean.OrderDetail;
import org.example.bean.OrderInfo;
import org.example.bean.OrderWide;
import org.example.function.DimAsyncFunction;
import org.example.utils.MyKafkaUtil;

import java.text.SimpleDateFormat;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * 订单宽表
 */
public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //并行度设置与kafka主题分区数一致
        env.setParallelism(2);
        //读取kafka主题数据

        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String groupId = "dwd_order_wide";
        String sinkTopic = "dwm_order_wide";

        //读取kafka主题数据，并转换为JavaBean对象，获取时间戳生成watermark
        SingleOutputStreamOperator<OrderInfo> orderInfoDS = env.fromSource(MyKafkaUtil.getKafkaConsumer(orderInfoSourceTopic, groupId), WatermarkStrategy.noWatermarks(), "dwd_order_info")
                .map(line -> {
                    OrderInfo orderInfo = JSON.parseObject(line, OrderInfo.class);
                    Long createTime = Long.parseLong(orderInfo.getCreateTime());
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    String[] dateTimeArr = sdf.format(createTime).split(" ");
                    orderInfo.setCreateDate(dateTimeArr[0]);
                    orderInfo.setCreateHour(dateTimeArr[1].split(":")[0]);
                    orderInfo.setCreateTs(createTime);
                    return orderInfo;
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                    @Override
                    public long extractTimestamp(OrderInfo orderInfo, long l) {
                        return orderInfo.getCreateTs();
                    }
                }));


        SingleOutputStreamOperator<OrderDetail> orderDetailDS = env.fromSource(MyKafkaUtil.getKafkaConsumer(orderDetailSourceTopic, groupId), WatermarkStrategy.noWatermarks(), "dwd_order_detail")
                .map(line -> {
                    OrderDetail orderDetail = JSON.parseObject(line, OrderDetail.class);
                    String createTime = orderDetail.getCreateTime();
                    orderDetail.setCreateTs(Long.parseLong(createTime));
                    return orderDetail;
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                    @Override
                    public long extractTimestamp(OrderDetail orderDetail, long l) {
                        return orderDetail.getCreateTs();
                    }
                }));

        //双流join
        DataStream<OrderWide> orderWideWithNoDimDS = orderInfoDS.keyBy(OrderInfo::getId)
                .intervalJoin(orderDetailDS.keyBy(OrderDetail::getOrderId))
                .between(Time.seconds(-20), Time.seconds(20)) //生产环境给最大的延迟时间
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>.Context context, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });

        //打印测试

        //关联维度信息 维度信息在hbase或者mysql中、优化：使用异步查询（Async I/O）
        //todo 异步id访问外部数据库 https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/datastream/operators/asyncio/
        //关系用户维度
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream.unorderedWait(
                orderWideWithNoDimDS,
                new DimAsyncFunction<OrderWide>("dim_user_info") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getUserId().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws Exception {
                        orderWide.setUserGender(dimInfo.getString("gender"));
//                        String birthday = dimInfo.getString("birthday");
//                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
//                        long currentTimeMillis = System.currentTimeMillis();
//                        long ts = sdf.parse(birthday).getTime();
//                        Long age = (currentTimeMillis - ts) / (1000 * 60 * 60 * 24 * 365L);
                        Random random = new Random();
                        int age = random.nextInt(100);
                        orderWide.setUserAge(age);
                    }
                },
                60,
                TimeUnit.SECONDS);

        //关联地区维度
        SingleOutputStreamOperator<OrderWide> orderWiderWithUserInfoAndProDS = AsyncDataStream.unorderedWait(
                orderWideWithUserDS,
                new DimAsyncFunction<OrderWide>("dim_base_province") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getProvinceId().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws Exception {
                        orderWide.setProvinceName(dimInfo.getString("name"));
                        orderWide.setProvinceAreaCode(dimInfo.getString("area_code"));
                        orderWide.setProvinceISOCode(dimInfo.getString("iso_code"));
                        orderWide.setProvince31662Code(dimInfo.getString("iso_3166_2"));
                    }
                }, 60,
                TimeUnit.SECONDS);

        //关联sku维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSKUDS = AsyncDataStream.unorderedWait(
                orderWiderWithUserInfoAndProDS, new DimAsyncFunction<OrderWide>("dim_sku_info") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getSkuId().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws Exception {
                        orderWide.setSkuName(dimInfo.getString("sku_name"));
                        orderWide.setCategory3Id(dimInfo.getLong("category3_id"));
                        orderWide.setSpuId(dimInfo.getLong("spu_id"));
                        orderWide.setTmId(dimInfo.getLong("tm_id"));
                    }
                }, 60,
                TimeUnit.SECONDS);

        //关联spu维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSPUDS = AsyncDataStream.unorderedWait(
                orderWideWithSKUDS, new DimAsyncFunction<OrderWide>("dim_spu_info") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getSpuId().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws Exception {
                        orderWide.setSpuName(dimInfo.getString("spu_name"));
                    }
                }, 60,
                TimeUnit.SECONDS);

        //关联tm维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTMDS = AsyncDataStream.unorderedWait(
                orderWideWithSPUDS, new DimAsyncFunction<OrderWide>("dim_base_trademark") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getTmId().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws Exception {
                        orderWide.setTmName(dimInfo.getString("tm_name"));
                    }
                }, 60,
                TimeUnit.SECONDS);

        //关联category维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS = AsyncDataStream.unorderedWait(
                orderWideWithTMDS, new DimAsyncFunction<OrderWide>("dim_base_category3") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getCategory3Id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws Exception {
                        orderWide.setCategory3Name(dimInfo.getString("name"));
                    }
                }, 60,
                TimeUnit.SECONDS);

        //将数据写入kafka
        orderWideWithCategory3DS.print("rs:");
        orderWideWithCategory3DS.map(JSONObject::toJSONString)
                .sinkTo(MyKafkaUtil.getKafkaProducer(sinkTopic));

        //启动任务
        env.execute("OrderWideApp");
    }
}
