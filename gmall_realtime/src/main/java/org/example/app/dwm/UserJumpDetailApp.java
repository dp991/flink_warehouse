package org.example.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import org.example.utils.MyKafkaUtil;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * 用户跳出（登录一次就跳出）  CEP案例
 */
public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception{
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //并行度设置与kafka主题分区数一致
        env.setParallelism(2);
        //读取kafka主题数据

        String sourceTopic = "dwd_page_log";
        String groupId = "user_jump_detail";
        String sinkTopic = "dwm_user_jump_detail";
        DataStreamSource<String> kafkaDS = env.fromSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId), WatermarkStrategy.noWatermarks(), "user_jump_detail");

        //将每行数据转换为json对象并生成watermark
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject).assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject jsonObject, long l) {
                return jsonObject.getLong("ts");
            }
        }));

        //定义模式序列
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                return lastPageId == null || lastPageId.length() <= 0;
            }
        }).next("next").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                return lastPageId == null || lastPageId.length() <= 0;
            }
        }).within(Time.seconds(10));

        // todo 两次模式条件一样，可以使用循环模式
//        Pattern<JSONObject, JSONObject> pattern2 = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
//            @Override
//            public boolean filter(JSONObject value) throws Exception {
//                String lastPageId = value.getJSONObject("page").getString("last_page_id");
//                return lastPageId == null || lastPageId.length() <= 0;
//            }
//        })
//                .times(2)
//                .consecutive() //指定严格近邻（相当于next），如果没有consecutive就相当于followBy
//                .within(Time.seconds(10));

        //将模式应用到流上
        PatternStream<JSONObject> patternStream = CEP.pattern(
                jsonObjDS.keyBy(json -> json.getJSONObject("common").getString("mid")),
                pattern);

        //提取匹配上的和超时事件
        OutputTag<JSONObject> timeOutTag = new OutputTag<JSONObject>("time_out") {
        };
        SingleOutputStreamOperator<JSONObject> selectDS = patternStream.select(timeOutTag, new PatternTimeoutFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject timeout(Map<String, List<JSONObject>> map, long ts) throws Exception {
                //超时事件（只有第一条，没有后续数据）取出第一条返回
                return map.get("start").get(0);
            }
        }, new PatternSelectFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {
                //两条数据都匹配上了，也返回第一条数据
                return map.get("start").get(0);
            }
        });

        DataStream<JSONObject> timeoutDS = selectDS.getSideOutput(timeOutTag);
        //union两种事件
        DataStream<JSONObject> unionDS = selectDS.union(timeoutDS);

        //将数据写入kafka
        unionDS.print("单跳");
        unionDS.map(JSONAware::toJSONString).sinkTo(MyKafkaUtil.getKafkaProducer(sinkTopic));
        //启动任务
        env.execute("UserJumpDetailApp");
    }
}
