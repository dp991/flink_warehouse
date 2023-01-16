package org.example;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkSQLCDC {
    public static void main(String[] args) throws Exception {
        //1、执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2、建表
        String sql = "CREATE TABLE mysql_binlog (\n" +
                " id STRING NOT NULL,\n" +
                " tm_name STRING,\n" +
                " logo_url STRING,\n" +
                " PRIMARY KEY(id) NOT ENFORCED\n"+
                " ) WITH ( \n" +
                " 'connector' = 'mysql-cdc',\n" +
                " 'hostname' = '127.0.0.1',\n" +
                " 'port' = '3306',\n" +
                " 'username' = 'root',\n" +
                " 'password' = '123456',\n" +
                " 'database-name' = 'gmall_flink',\n" +
                " 'table-name' = 'base_trademark'\n" +
                ")";

        tableEnv.executeSql(sql);

        //3、查询数据
        Table table = tableEnv.sqlQuery("select * from mysql_binlog");

        //4、将表转换为流
        DataStream<Tuple2<Boolean, Row>> dataStream = tableEnv.toRetractStream(table, Row.class);

        dataStream.print();

        env.execute("FlinkCDCWithSQL");
    }
}
