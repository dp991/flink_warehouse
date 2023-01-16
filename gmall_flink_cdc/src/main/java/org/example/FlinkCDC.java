package org.example;

import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Properties;

/**
 * datastream的方式进行cdc
 * 任务结束前需要做savepoint，重启时需要使用savepoint的方式进行重启
 */
public class FlinkCDC {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //开启checkpoint
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop:8020/gmall-flink/ck");
//
//        env.enableCheckpointing(5000); //头跟头间隔5s，生存上5到10分钟
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000); //头跟尾间隔


        Properties debeziumProperties = new Properties();
        debeziumProperties.put("snapshot.locking.mode", "none");// do not use lock

        SourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("127.0.0.1")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall_flink_realtime")
                .tableList("gmall_flink_realtime.table_process")  // 不添加该参数，消费所有表；如果指定，需要使用dbname.tableName
                .deserializer(new CusomerDeserialization()) // new StringDebeziumDeserializationSchema()
                .startupOptions(StartupOptions.latest()) //initial()
                .debeziumProperties(debeziumProperties)
                .build();


        DataStreamSource<String> streamSource = env.addSource(sourceFunction);

        streamSource.print();

        env.execute("FlinkCDC");

    }
}
