package org.example.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.bean.VisitorStatus;
import org.example.utils.ClickHouseUtil;
import org.example.utils.DateTimeUtil;
import org.example.utils.MyKafkaUtil;
import scala.Tuple4;

import java.time.Duration;
import java.util.Date;

/**
 * dws层访客主题宽表计算：pv、uv、跳出率、连续访问时长
 * 三流 union
 * 启动：（数据源：gmall2020-mock-log-2020-12-18.jar、GmallLoggerApplication）baseLogApp、UserJumpDetailApp、UniqueVisitApp
 * 数据写入clickhouse
 */
public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //并行度设置与kafka主题分区数一致
        env.setParallelism(1);

        //读取kafka主题数据 创建流
        String groupId = "visitor_status_app";

        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";

        DataStreamSource<String> uvDS = env.fromSource(MyKafkaUtil.getKafkaConsumer(uniqueVisitSourceTopic, groupId), WatermarkStrategy.noWatermarks(), "dwm_unique_visit");
        DataStreamSource<String> ujDS = env.fromSource(MyKafkaUtil.getKafkaConsumer(userJumpDetailSourceTopic, groupId), WatermarkStrategy.noWatermarks(), "dwm_user_jump_detail");
        DataStreamSource<String> pvDS = env.fromSource(MyKafkaUtil.getKafkaConsumer(pageViewSourceTopic, groupId), WatermarkStrategy.noWatermarks(), "dwd_page_log");

        //将每个流处理成相同的数据类型
        //转换uv
        SingleOutputStreamOperator<VisitorStatus> visitStatsWithUVDS = uvDS.map(json -> {
            JSONObject jsonObject = JSON.parseObject(json);
            JSONObject common = jsonObject.getJSONObject("common");
            return new VisitorStatus("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    1L, 0L, 0L, 0L, 0L,
                    jsonObject.getLong("ts")
            );
        });

        //转换uj
        SingleOutputStreamOperator<VisitorStatus> visitStatsWithUJDS = ujDS.map(json -> {
            JSONObject jsonObject = JSON.parseObject(json);
            JSONObject common = jsonObject.getJSONObject("common");
            return new VisitorStatus("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 0L, 0L, 1L, 0L,
                    jsonObject.getLong("ts")
            );
        });

        //转换pv流
        SingleOutputStreamOperator<VisitorStatus> visitStatsWithPVDS = pvDS.map(json -> {
            //获取公共字段
            JSONObject jsonObject = JSON.parseObject(json);
            JSONObject common = jsonObject.getJSONObject("common");
            JSONObject page = jsonObject.getJSONObject("page");
            //获取页面信息
            String lastPageId = page.getString("last_page_id");
            long sv = 0L;
            if (lastPageId == null || lastPageId.length() <= 0) {
                sv = 1L;
            }
            return new VisitorStatus("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 1L, sv, 0L,
                    page.getLong("during_time"),
                    jsonObject.getLong("ts")
            );
        });

        //union
        DataStream<VisitorStatus> unionDS = visitStatsWithUVDS.union(visitStatsWithUJDS, visitStatsWithPVDS);

        //提取时间戳生成watermark
        SingleOutputStreamOperator<VisitorStatus> visitorStatusWithWMDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.<VisitorStatus>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStatus>() {
                    @Override
                    public long extractTimestamp(VisitorStatus visitorStatus, long l) {
                        return visitorStatus.getTs();
                    }
                }));

        //按照维度信息分组(四个字段)
        KeyedStream<VisitorStatus, Tuple4<String, String, String, String>> keyedStream = visitorStatusWithWMDS.keyBy(new KeySelector<VisitorStatus, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStatus visitorStatus) throws Exception {
                return new Tuple4<>(visitorStatus.getAr(), visitorStatus.getCh(), visitorStatus.getIsNew(), visitorStatus.getVc());
            }
        });

        //开窗聚合 10s的滚动窗口
        WindowedStream<VisitorStatus, Tuple4<String, String, String, String>, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(11)));
        //聚合,需要提取窗口信息
        SingleOutputStreamOperator<VisitorStatus> resultDS = windowedStream.reduce(new ReduceFunction<VisitorStatus>() {
            @Override
            public VisitorStatus reduce(VisitorStatus v1, VisitorStatus v2) throws Exception {
                v1.setUvCt(v1.getUvCt() + v2.getUvCt());
                v1.setPvCt(v1.getPvCt() + v2.getPvCt());
                v1.setSvCt(v1.getSvCt() + v2.getSvCt());
                v1.setUjCt(v1.getUjCt() + v2.getUjCt());
                v1.setDurCt(v1.getDurCt() + v2.getDurCt());
                return v1;
            }
        }, new WindowFunction<VisitorStatus, VisitorStatus, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> tuple4, TimeWindow timeWindow, Iterable<VisitorStatus> input, Collector<VisitorStatus> out) throws Exception {
                long start = timeWindow.getStart();
                long end = timeWindow.getEnd();

                VisitorStatus visitorStatus = input.iterator().next();
                //补充信息
                visitorStatus.setStt(DateTimeUtil.toYMDhms(new Date(start)));
                visitorStatus.setEdt(DateTimeUtil.toYMDhms(new Date(end)));
                out.collect(visitorStatus);

            }
        });

        resultDS.print("resultDS");
        //将数据写入ck
        String sql = "insert into default.visitor_stats(stt,edt,vc,ch,ar,is_new,uv_ct,pv_ct,sv_ct,uj_ct,dur_sum,ts) values(?,?,?,?,?,?,?,?,?,?,?,?)";
        resultDS.addSink(ClickHouseUtil.getSinkFunction(sql));


        env.execute("VisitorStatusApp");
    }
}
