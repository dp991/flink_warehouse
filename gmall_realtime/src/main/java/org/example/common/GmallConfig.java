package org.example.common;

public class GmallConfig {

    /**
     * phoenix连接信息
     */
    public static final String HBASE_SCHEMA = "GMALL_REALTIME";
    public static final String PHONEIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    public static final String PHOENIX_SERVER = "jdbc:phoenix:172.25.92.6:2181";


    /**
     * mysql 连接信息
     */
    public static final String MYSQL_DATABASE_NAME = "gmall_flink_dim";
    public static final String MYSQL_DRIVER_NAME = "com.mysql.cj.jdbc.Driver";
    public static final String MYSQL_URL = "jdbc:mysql://127.0.0.1:3306/gmall_flink_dim?characterEncoding=utf8&useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true";
    public static final String MYSQL_USER = "root";
    public static final String MYSQL_PASSWORD = "123456";


    /**
     * clickhouse 连接信息
     */

    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://127.0.0.1:8123/default";
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";
    public static final String CLICKHOUSE_USER = "default";
    public static final String CLICKHOUSE_PASSWD = "123456";

    /**
     * kafka 连接信息
     */

    public static final String KAFKA_BROKERS = "192.168.120.181:9092";
    public static final String KAFKA_DEFAULT_TOPIC = "test_topic";


    /**
     * redis 连接信息
     */
    public static final String JEDIS_HOST = "127.0.0.1";


}
