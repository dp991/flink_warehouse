package org.example.app.dws;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.bean.ProvinceStats;
import org.example.utils.ClickHouseUtil;
import org.example.utils.MyKafkaUtil;

/**
 * flink sql
 */
public class ProvinceStatsApp {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //并行度设置与kafka主题分区数一致
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 使用ddl创建表 提取时间戳生成watermark
        String topic = "dwm_order_wide";
        String groupId = "test_group";
        String tableDDL = "create table order_wide(" +
                " province_id BIGINT," +
                " province_name STRING," +
                " province_area_code STRING," +
                " province_iso_code STRING," +
                " province_3166_2_code STRING," +
                " order_id BIGINT," +
                " split_total_amount DECIMAL," +
                " create_time STRING, " +
                " rt as TO_TIMESTAMP(create_time)," +
                " WATERMARK FOR rt AS rt - INTERVAL '1' SECOND ) WITH (" +
                MyKafkaUtil.getKafkaDDL(topic, groupId) + ")";

        tableEnv.executeSql(tableDDL);
        //执行查询 分组 开窗 聚合
        String querySQL = "SELECT " +
                "   DATE_FORMAT(TUMBLE_START(rt,INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt, " +
                "   DATE_FORMAT(TUMBLE_END(rt,INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt, " +
                "   province_id, " +
                "   province_name, " +
                "   province_area_code, " +
                "   province_iso_code, " +
                "   province_3166_2_code, " +
                "   count(distinct order_id) order_count, " +
                "   sum(split_total_amount) order_amount, " +
                "   UNIX_TIMESTAMP() * 1000 ts "+
                "FROM " +
                "   order_wide " +
                "GROUP BY " +
                "   province_id, " +
                "   province_name, " +
                "   province_area_code, " +
                "   province_iso_code, " +
                "   province_3166_2_code, " +
                "   TUMBLE(rt,INTERVAL '10' SECOND)";

        Table table = tableEnv.sqlQuery(querySQL);

        //将动态表转换为流
        DataStream<ProvinceStats> dataStream = tableEnv.toAppendStream(table, ProvinceStats.class);
        dataStream.print();

        //打印数据并写入clickhouse
        dataStream.addSink(ClickHouseUtil.getSinkFunction("insert into default.province_stats values(?,?,?,?,?,?,?,?,?,?)",20));

        //执行任务
        env.execute("ProvinceStatsApp");
    }
}
