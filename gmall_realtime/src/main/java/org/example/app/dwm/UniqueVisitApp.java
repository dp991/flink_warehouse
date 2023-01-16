package org.example.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.utils.MyKafkaUtil;

import java.text.SimpleDateFormat;

/**
 * uv 日活统计
 */
public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {
        //获取执行环境

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //读取kafka dwd_page_log主题数据
        String sourceTopic = "dwd_page_log";
        String groupId = "unique_visit_app";
        String sinkTopic = "dwm_unique_visit";
        DataStreamSource<String> kafkaDS = env.fromSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId), WatermarkStrategy.noWatermarks(), "unique_visit_app");

        //将每行数据转为json
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        //过滤数据 状态编程 只保留每个mid每天第一次登录的数据
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<JSONObject> uvDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {
            private ValueState<String> dateState;
            private SimpleDateFormat simpleDateFormat;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("date-state", String.class);
                //todo 关键点 设置状态过期时间以及时间更新的方式
                StateTtlConfig ttl = new StateTtlConfig.Builder(Time.hours(24)).setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).build();
                stateDescriptor.enableTimeToLive(ttl);
                dateState = getRuntimeContext().getState(stateDescriptor);
                simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {

                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                if (lastPageId == null || lastPageId.length() <= 0) {
                    //取出状态数据
                    String lastDate = dateState.value();
                    String currentDate = simpleDateFormat.format(value.getLong("ts"));

                    if (!currentDate.equals(lastDate)) {
                        //更新状态 返回true
                        dateState.update(currentDate);
                        return true;
                    }
                }
                return false;
            }
        });

        uvDS.print("uv");
        //将数据写入kafka
        uvDS.map(line -> line.toJSONString())
                .sinkTo(MyKafkaUtil.getKafkaProducer(sinkTopic));

        //启动任务
        env.execute("UniqueVisitApp");
    }

}
