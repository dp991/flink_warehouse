package org.example.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.utils.MyKafkaUtil;

/**
 * kafka ods 分流 ---> kafka dwd
 */
public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        //  执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //开启checkpoint 生产环境必须添加checkpoint
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop:8020/gmall-flink/ck");
//
//        env.enableCheckpointing(5000); //头跟头间隔5s，生存上5到10分钟
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000); //头跟尾间隔

        //消费ods_base_log数据
        String topic = "ods_base_log";
        String groupId = "base_log_app";
        DataStreamSource<String> kafkaDS = env.fromSource(MyKafkaUtil.getKafkaConsumer(topic, groupId),
                WatermarkStrategy.noWatermarks(), "ods_base_log_app");

        //数据转换为json,处理脏数据,将数据写入测输出流
        OutputTag<String> dirtyOutputTag = new OutputTag<String>("dirty"){};
        SingleOutputStreamOperator<JSONObject> processDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    //解析异常，将数据写入测输出流
                    context.output(dirtyOutputTag, value);
                }
            }
        });
        //打印脏数据
        processDS.getSideOutput(dirtyOutputTag).print("dirty>>>>>>");
        //新老用户校验 状态编程
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = processDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"))
                .map(new RichMapFunction<JSONObject, JSONObject>() {

                    private ValueState<String> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("value-state", String.class));
                    }

                    @Override
                    public JSONObject map(JSONObject value) throws Exception {
                        String isNew = value.getJSONObject("common").getString("is_new");
                        if ("1".equals(isNew)) {
                            String state = valueState.value();
                            if (state != null) {
                                //修改标记
                                value.getJSONObject("common").put("is_new", "0");
                            } else {
                                valueState.update("1");
                            }
                        }
                        return value;
                    }
                });

        //分流 测输出
        OutputTag<String> startOutputTag = new OutputTag<String>("start"){};
        OutputTag<String> displayOutputTag = new OutputTag<String>("display"){};

        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {
                //启动日志
                String start = value.getString("start");
                if (start != null && start.length() > 0) {
                    ctx.output(startOutputTag, value.toJSONString());
                } else {
                    //页面日志写入主流
                    out.collect(value.toJSONString());

                    //曝光数据
                    JSONArray displays = value.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        //获取页面id
                        String pageId = value.getJSONObject("page").getString("page_id");
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            //添加页面id
                            display.put("page_id", pageId);
                            //将数据写出到曝光测输出流
                            ctx.output(displayOutputTag, display.toJSONString());
                        }
                    }
                }
            }
        });
        //提取测输出流
        DataStream<String> startDS = pageDS.getSideOutput(startOutputTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayOutputTag);

        //将流打印并输出到对应的kafka主题dwd_start_log, dwd_page_log, dwd_display_log
        startDS.print("start>>>>>>");
        pageDS.print("page>>>>>>");
        displayDS.print("display>>>>>>");

        startDS.sinkTo(MyKafkaUtil.getKafkaProducer("dwd_start_log"));
        pageDS.sinkTo(MyKafkaUtil.getKafkaProducer("dwd_page_log"));
        displayDS.sinkTo(MyKafkaUtil.getKafkaProducer("dwd_display_log"));

        //启动
        env.execute("BaseLogApp");
    }
}
