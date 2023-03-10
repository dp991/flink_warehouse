package org.traffic.common;

public class TrafficConfig {


    /**
     * mysql 连接信息
     */
    public static final String MYSQL_DATABASE_NAME = "traffic_monitor";
    public static final String MYSQL_DRIVER_NAME = "com.mysql.cj.jdbc.Driver";
    public static final String MYSQL_URL = "jdbc:mysql://127.0.0.1:3306/traffic_monitor?characterEncoding=utf8&useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true";
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
    public static final int JEDIS_PORT = 6379;
    public static final Integer TIME_OUT = 1000;


}
