package org.example.app.ods;

import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.example.function.CusomerDeserialization;
import org.example.utils.MyKafkaUtil;

import java.util.Properties;

/**
 * 业务数据ods
 */
public class FlinkCDC {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties debeziumProperties = new Properties();
        debeziumProperties.put("snapshot.locking.mode", "none");// do not use lock

        SourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("127.0.0.1")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall_flink")
                .deserializer(new CusomerDeserialization())
                .startupOptions(StartupOptions.latest()) //initial()
                .debeziumProperties(debeziumProperties)
                .build();

        DataStreamSource<String> streamSource = env.addSource(sourceFunction).setParallelism(1);//keep message ordering


        streamSource.print();

        //将数据写入kafka
        String sinkTopic = "ods_base_db";

        streamSource.sinkTo(MyKafkaUtil.getKafkaProducer(sinkTopic));

        env.execute("FlinkCDC");

    }
}
