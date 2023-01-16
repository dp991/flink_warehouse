package org.example.app.dws;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.bean.KeywordStats;
import org.example.function.SplitFunction;
import org.example.utils.ClickHouseUtil;
import org.example.utils.MyKafkaUtil;

/**
 * flink sql 搜索关键词业务
 */
public class KeywordStatsApp {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //并行度设置与kafka主题分区数一致
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //使用ddl方式读取kafka数据
        String topic = "dwd_page_log";
        String groupId = "keyword_stats_app";
        String tableDDL =
                "create table page_view ( " +
                "   common Map<String, String>," +
                "   page Map<String, String>," +
                "   ts BIGINT," +
                "   rt as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000))," +
                "   WATERMARK FOR rt AS rt - INTERVAL '1' SECOND)" + " WITH (" +
                MyKafkaUtil.getKafkaDDL(topic, groupId) + ")";

        tableEnv.executeSql(tableDDL);

        //过滤数据：上一眺数据为search 和搜索关键词不为null
        String filterSQL =
                "select " +
                        "   page['item'] full_word, " +
                        "   rt " +
                        "from " +
                        "   page_view " +
                        "where " +
                        "   page['last_page_id'] = 'search' and page['item'] is not null ";

        Table fullWordTable = tableEnv.sqlQuery(filterSQL);

        // 注册udf 运行分词处理
        tableEnv.createTemporarySystemFunction("split_words", SplitFunction.class);
        String splitSQL = "select " +
                "   word, " +
                "   rt " +
                "from " +
                "   " + fullWordTable + ",LATERAL TABLE(split_words(full_word))";

        Table wordTable = tableEnv.sqlQuery(splitSQL);

        //分组 开窗 聚合
        String groupSQL = "select "+
                "   'search' source, "+
                "   DATE_FORMAT(TUMBLE_START(rt,INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt, " +
                "   DATE_FORMAT(TUMBLE_END(rt,INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt, " +
                "   word keyword , "+
                "   count(*) ct, "+
                "   UNIX_TIMESTAMP()*1000 ts "+
                "from "+wordTable+
                " group by "+
                "   word, "+
                "   TUMBLE(rt,INTERVAL '10' SECOND)";
        Table restultTable = tableEnv.sqlQuery(groupSQL);

        //动态表转为流
        DataStream<KeywordStats> dataStream = tableEnv.toAppendStream(restultTable, KeywordStats.class);

        //打印输出到clickhouse
        dataStream.print();
        dataStream.addSink(ClickHouseUtil.getSinkFunction("insert into default.keyword_stats(keyword,ct,source,stt,edt,ts) values(?,?,?,?,?,?)"));

        //执行
        env.execute("KeywordStatsApp");
    }
}
