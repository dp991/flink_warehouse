package org.example.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.bean.OrderWide;
import org.example.bean.PaymentWide;
import org.example.bean.ProductStats;
import org.example.common.GmallConstant;
import org.example.function.DimAsyncFunction;
import org.example.utils.ClickHouseUtil;
import org.example.utils.DateTimeUtil;
import org.example.utils.MyKafkaUtil;

import java.time.Duration;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
//数据流：(业务数据、行为数据)app/web --> springboot --> kafka
//程序：flinkcdc\baseLogApp\baseDNApp\orderWideApp\paymentWideApp\productstatsApp

public class ProductStatsApp {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //并行度设置与kafka主题分区数一致
        env.setParallelism(1);

        //读取kafka数据，7个主题数据
        String groupId = "product_stats_app";

        String pageViewSourceTopic = "dwd_page_log";
        String orderWideSourceTopic = "dwm_order_wide";  //主题暂无数据
        String paymentWideSourceTopic = "dwm_payment_wide";//主题暂无数据
        String cartInfoSourceTopic = "dwd_cart_info";
        String favorInfoSourceTopic = "dwd_favor_info";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";

        DataStreamSource<String> pvDS = env.fromSource(MyKafkaUtil.getKafkaConsumer(pageViewSourceTopic, groupId), WatermarkStrategy.noWatermarks(), "dwd_page_log");
        DataStreamSource<String> favorDS = env.fromSource(MyKafkaUtil.getKafkaConsumer(favorInfoSourceTopic, groupId), WatermarkStrategy.noWatermarks(), "dwd_favor_info");
        DataStreamSource<String> cartDS = env.fromSource(MyKafkaUtil.getKafkaConsumer(cartInfoSourceTopic, groupId), WatermarkStrategy.noWatermarks(), "dwd_cart_info");
        DataStreamSource<String> orderDS = env.fromSource(MyKafkaUtil.getKafkaConsumer(orderWideSourceTopic, groupId), WatermarkStrategy.noWatermarks(), "dwm_order_wide");
        DataStreamSource<String> payDS = env.fromSource(MyKafkaUtil.getKafkaConsumer(paymentWideSourceTopic, groupId), WatermarkStrategy.noWatermarks(), "dwm_payment_wide");
        DataStreamSource<String> refundDS = env.fromSource(MyKafkaUtil.getKafkaConsumer(refundInfoSourceTopic, groupId), WatermarkStrategy.noWatermarks(), "dwd_order_refund_info");
        DataStreamSource<String> commentDS = env.fromSource(MyKafkaUtil.getKafkaConsumer(commentInfoSourceTopic, groupId), WatermarkStrategy.noWatermarks(), "dwd_comment_info");

        //将7个流数据格式同一
        //曝光和点击数据
        SingleOutputStreamOperator<ProductStats> productStatsWithClickAndDisplayDS = pvDS.flatMap(new FlatMapFunction<String, ProductStats>() {
            @Override
            public void flatMap(String s, Collector<ProductStats> out) throws Exception {

                //将数据转换为json对象
                JSONObject jsonObject = JSON.parseObject(s);
                //取出page信息
                JSONObject page = jsonObject.getJSONObject("page");
                Long ts = jsonObject.getLong("ts");
                String pageId = page.getString("page_id");
                if ("good_detail".equals(pageId) && "sku_id".equals(page.getString("item_type"))) {
                    out.collect(ProductStats.builder()
                            .skuId(page.getLong("item"))
                            .clickCt(1L)
                            .ts(ts)
                            .build());
                }

                //尝试取出曝光数据
                JSONArray displays = jsonObject.getJSONArray("displays");
                if (displays != null && displays.size() > 0) {
                    for (int i = 0; i < displays.size(); i++) {
                        //取出单跳曝光数据
                        JSONObject display = displays.getJSONObject(i);
                        if ("sku_id".equals(display.getString("item_type"))) {
                            out.collect(ProductStats.builder()
                                    .skuId(display.getLong("item"))
                                    .displayCt(1L)
                                    .ts(ts)
                                    .build());
                        }
                    }
                }
            }
        });
        //收藏数据
        SingleOutputStreamOperator<ProductStats> productStatsWithFavorDS = favorDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            return ProductStats.builder()
                    .skuId(jsonObject.getLong("sku_id"))
                    .favorCt(1L)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });
        //加入购物车数据
        SingleOutputStreamOperator<ProductStats> productStatsWithCartDS = cartDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            return ProductStats.builder()
                    .skuId(jsonObject.getLong("sku_id"))
                    .cartCt(1L)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });
        //下单数据
        SingleOutputStreamOperator<ProductStats> productStatsWithOrderDS = orderDS.map(line -> {
            OrderWide orderWide = JSON.parseObject(line, OrderWide.class);
            HashSet<Object> orderIds = new HashSet<>();
            orderIds.add(orderWide.getOrderId());

            return ProductStats.builder()
                    .skuId(orderWide.getSkuId())
                    .orderSkuNum(orderWide.getSkuNum())
                    .orderAmount(orderWide.getOrderPrice())
                    .orderIdSet(orderIds)
                    .ts(DateTimeUtil.toTs(orderWide.getCreateTime()))
                    .build();
        });

        //支付数据
        SingleOutputStreamOperator<ProductStats> productStatsWithPayDS = payDS.map(line -> {
            PaymentWide paymentWide = JSON.parseObject(line, PaymentWide.class);
            HashSet<Object> orderIds = new HashSet<>();
            orderIds.add(paymentWide.getOrderId());

            return ProductStats.builder()
                    .skuId(paymentWide.getSkuId())
                    .paymentAmount(paymentWide.getOrderPrice())
                    .paidOrderIdSet(orderIds)
                    .ts(DateTimeUtil.toTs(paymentWide.getPaymentCreateTime()))
                    .build();
        });

        //退款退单数据
        SingleOutputStreamOperator<ProductStats> productStatsWithRefundDS = refundDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            HashSet<Object> orderIds = new HashSet<>();
            orderIds.add(jsonObject.getLong("id"));

            return ProductStats.builder()
                    .skuId(jsonObject.getLong("sku_id"))
                    .refundAmount(jsonObject.getBigDecimal("refund_amount"))
                    .refundOrderIdSet(orderIds)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        //评价数据
        SingleOutputStreamOperator<ProductStats> productStatsWithCommentDS = commentDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            HashSet<Object> orderIds = new HashSet<>();
            orderIds.add(jsonObject.getLong("id"));
            String appraise = jsonObject.getString("appraise");
            long goodCt = 0L;
            if (GmallConstant.APPRAISE_GOOD.equals(appraise)) {
                goodCt = 1L;
            }

            return ProductStats.builder()
                    .skuId(jsonObject.getLong("sku_id"))
                    .commentCt(1L)
                    .goodCommentCt(goodCt)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        // union 7 个流
        DataStream<ProductStats> unionDS = productStatsWithClickAndDisplayDS.union(
                productStatsWithFavorDS,
                productStatsWithCartDS,
                productStatsWithOrderDS,
                productStatsWithPayDS,
                productStatsWithRefundDS,
                productStatsWithCommentDS
        );

        //提取时间戳生成watermark
        SingleOutputStreamOperator<ProductStats> productStatsWithWMDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<ProductStats>() {
            @Override
            public long extractTimestamp(ProductStats productStats, long l) {
                return productStats.getTs();
            }
        }));

        //分组开窗聚合，按照sku_id分组，10秒中的滚动窗口，结合增量聚合（累加值）和全量聚合（提取窗口信息）
        SingleOutputStreamOperator<ProductStats> reduceDS = productStatsWithWMDS.keyBy(ProductStats::getSkuId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<ProductStats>() {
                    @Override
                    public ProductStats reduce(ProductStats p1, ProductStats p2) throws Exception {
                        p1.setDisplayCt(p1.getDisplayCt() + p2.getDisplayCt());
                        p1.setClickCt(p1.getClickCt() + p2.getClickCt());
                        p1.setCartCt(p1.getCartCt() + p2.getCartCt());
                        p1.setFavorCt(p1.getFavorCt() + p2.getFavorCt());
                        p1.setOrderAmount(p1.getOrderAmount().add(p2.getOrderAmount()));
                        p1.getOrderIdSet().addAll(p2.getOrderIdSet());
                        p1.setOrderSkuNum(p1.getOrderSkuNum() + p2.getOrderSkuNum());
                        p1.setPaymentAmount(p1.getPaymentAmount().add(p2.getPaymentAmount()));
                        p1.getRefundOrderIdSet().addAll(p2.getRefundOrderIdSet());
                        p1.setRefundAmount(p1.getRefundAmount().add(p2.getRefundAmount()));
                        p1.getPaidOrderIdSet().addAll(p2.getPaidOrderIdSet());
                        p1.setCommentCt(p1.getCommentCt() + p2.getCommentCt());
                        p1.setGoodCommentCt(p1.getGoodCommentCt() + p2.getGoodCommentCt());

                        return p1;
                    }
                }, new WindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {

                    @Override
                    public void apply(Long aLong, TimeWindow timeWindow, Iterable<ProductStats> input, Collector<ProductStats> out) throws Exception {
                        ProductStats productStats = input.iterator().next();
                        //取出窗口时间
                        productStats.setStt(DateTimeUtil.toYMDhms(new Date(timeWindow.getStart())));
                        productStats.setEdt(DateTimeUtil.toYMDhms(new Date(timeWindow.getEnd())));

                        productStats.setOrderCt((long) productStats.getOrderIdSet().size());
                        productStats.setPaidOrderCt((long) productStats.getPaidOrderIdSet().size());
                        productStats.setRefundOrderCt((long) productStats.getRefundOrderIdSet().size());

                        out.collect(productStats);
                    }
                });

        // 关联维度信息
        //关联sku维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuDS = AsyncDataStream.unorderedWait(reduceDS,
                new DimAsyncFunction<ProductStats>("dim_sku_info") {
                    @Override
                    public String getKey(ProductStats input) {
                        return input.getSkuId().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfo) throws Exception {

                        productStats.setSkuName(dimInfo.getString("sku_name"));
                        productStats.setSkuPrice(dimInfo.getBigDecimal("sku_price"));
                        productStats.setSkuId(dimInfo.getLong("sku_id"));
                        productStats.setTmId(dimInfo.getLong("tm_id"));
                        productStats.setCategory3Id(dimInfo.getLong("category3_id"));

                    }
                }, 60, TimeUnit.SECONDS);

        //关联spu维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDS = AsyncDataStream.unorderedWait(productStatsWithSkuDS,
                new DimAsyncFunction<ProductStats>("dim_spu_info") {
                    @Override
                    public String getKey(ProductStats input) {
                        return input.getSkuId().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfo) throws Exception {

                        productStats.setSpuName(dimInfo.getString("spu_name"));

                    }
                }, 60, TimeUnit.SECONDS);

        //关联categories维度
        SingleOutputStreamOperator<ProductStats> productStatsWithCategroy3DS = AsyncDataStream.unorderedWait(productStatsWithSpuDS,
                new DimAsyncFunction<ProductStats>("dim_base_category3") {
                    @Override
                    public String getKey(ProductStats input) {
                        return input.getCategory3Id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfo) throws Exception {

                        productStats.setCategory3Name(dimInfo.getString("name"));

                    }
                }, 60, TimeUnit.SECONDS);

        //关联TM维度
        SingleOutputStreamOperator<ProductStats> productStatsWithTMDS = AsyncDataStream.unorderedWait(productStatsWithCategroy3DS,
                new DimAsyncFunction<ProductStats>("dim_base_trademark") {
                    @Override
                    public String getKey(ProductStats input) {
                        return input.getTmId().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfo) throws Exception {

                        productStats.setTmName(dimInfo.getString("tm_name"));

                    }
                }, 60, TimeUnit.SECONDS);

        //将数据写入ck
        productStatsWithTMDS.addSink(ClickHouseUtil.getSinkFunction("insert into default.product_stats(" +
                "stt,edt,sku_id,sku_name,sku_price,spu_id,spu_name,tm_id,tm_name,category3_id,category3_name,display_ct,click_ct,favor_ct,cart_ct,order_sku_num,order_amount,order_ct,payment_amount" +
                ",paid_order_ct,refund_order_ct,refund_amount,commnt_ct,good_comment_ct,ts) values " +
                "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",20));

        env.execute("ProductStatsApp");
    }
}
