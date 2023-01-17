package org.example.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.function.CusomerDeserialization;
import org.example.function.DimSinkFunction;
import org.example.function.TableProcessFunction;
import org.example.bean.TableProcess;
import org.example.utils.MyKafkaUtil;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * kafka主题流
 * mysql 配置广播流
 * 链接流写入kafka和hbase
 */
public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        //  执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //消费ods_base_db数据,创建主流
        String sourceTopic = "ods_base_db";
        String groupId = "base_db_app";
        DataStreamSource<String> kafkaDS = env.fromSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId), WatermarkStrategy.noWatermarks(), "base_db_app");

        //数据转换为json,处理脏数据,将数据写入测输出流,过滤delete数据 主流
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject)
                .filter(new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        String type = value.getString("type");
                        return !"delete".equals(type);
                    }
                });

        //todo 使用flink cdc消费配置表并处理形成 广播流
        Properties debeziumProperties = new Properties();
        debeziumProperties.put("snapshot.locking.mode", "none");// do not use lock
        SourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("127.0.0.1")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall_flink_realtime")
                .tableList("gmall_flink_realtime.table_process")
                .deserializer(new CusomerDeserialization())
                .startupOptions(StartupOptions.initial()) //initial()
                .debeziumProperties(debeziumProperties)
                .build();
        DataStreamSource<String> tableProcessStrDS = env.addSource(sourceFunction);

        tableProcessStrDS.print("tableProcessStrDS");

//        //广播流
        MapStateDescriptor mapStateDescriptor = new MapStateDescriptor<String, TableProcess>("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = tableProcessStrDS.broadcast(mapStateDescriptor);

//        //链接广播流和主流数据
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>("hbase-tag") {
        };
        SingleOutputStreamOperator<JSONObject> kafka = jsonObjDS.connect(broadcastStream).process(new TableProcessFunction(hbaseTag, mapStateDescriptor));

//        //提取kafka数据流和hbase数据流
        DataStream<JSONObject> hbase = kafka.getSideOutput(hbaseTag);
        //将kafka数据写入kafka主题，hbase数据写入hbase表
//        hbase.print("hbase>>>>");//hbase sink

        //数据通过phoenix写入hbase
        hbase.addSink(new DimSinkFunction());

        //主流数据写入kafka,每条数据可能对应不同的主题 kafka 动态sink到不同的topic中
        kafka.sinkTo(MyKafkaUtil.getKafkaSinkProducer(new KafkaRecordSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, KafkaSinkContext kafkaSinkContext, Long aLong) {
                return new ProducerRecord<byte[], byte[]>(jsonObject.getString("sinkTable"), jsonObject.getString("after").getBytes());
            }
        }));

        //启动
        env.execute("BaseDBApp");
    }
}
