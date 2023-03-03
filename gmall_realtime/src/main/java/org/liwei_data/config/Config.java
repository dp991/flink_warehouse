package org.liwei_data.config;

public class Config {


    /**
     * mysql 连接信息
     */
    public static final String MYSQL_DATABASE_NAME = "liwei_base";
    public static final String MYSQL_DRIVER_NAME = "com.mysql.cj.jdbc.Driver";
    public static final String MYSQL_URL = "jdbc:mysql://127.0.0.1:3306/liwei_base?characterEncoding=utf8&useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true";
    public static final String MYSQL_USER = "root";
    public static final String MYSQL_PASSWORD = "123456";


    public static final String REMOTE_MYSQL_DATABASE_NAME = "intelliwell_dw";
    public static final String REMOTE_MYSQL_DRIVER_NAME = "com.mysql.cj.jdbc.Driver";
    public static final String REMOTE_MYSQL_URL = "jdbc:mysql://rm-bp1gbw38086695ed9oo.mysql.rds.aliyuncs.com:3306/intelliwell_dw?characterEncoding=utf8&useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true";
    public static final String REMOTE_MYSQL_USER = "intelliwelldw";
    public static final String REMOTE_MYSQL_PASSWORD = "Intelliwell6688";

}
