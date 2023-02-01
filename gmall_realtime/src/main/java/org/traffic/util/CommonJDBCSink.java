package org.traffic.util;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.example.bean.TransientSink;
import org.traffic.common.TrafficConfig;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * mysql通用插入封装工具类
 * 优化：如需批量操作，可在sink之前将数据收集一段时间，结合countwind函数操作
 *
 * @param <T>
 */
public class CommonJDBCSink<T> extends RichSinkFunction<T> {

    Connection connection;
    PreparedStatement pstmt;

    String sql;

    int batchSize;

    public CommonJDBCSink(String sql) {
        this.sql = sql;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(TrafficConfig.MYSQL_URL, TrafficConfig.MYSQL_USER, TrafficConfig.MYSQL_PASSWORD);
        //"insert into  t_overspeed_info(car_id,monitor_id,road_id,real_speed,limit_speed,action_time) values(?,?,?,?,?,?)"
        pstmt = connection.prepareStatement(sql);
    }

    @Override
    public void invoke(T t, Context context) throws Exception {

        Field[] fields = t.getClass().getDeclaredFields();

        //遍历字段
        int offset = 0;
        for (int i = 0; i < fields.length; i++) {
            //获取字段
            Field field = fields[i];

            //设置私有属性可访问
            field.setAccessible(true);

            //获取字段上的注解，该字段不需要写到数据库
            TransientSink annotation = field.getAnnotation(TransientSink.class);
            if (annotation != null) {
                //存在该注解，继续，此字段不需要写出去
                offset++;
                continue;
            }

            //获取值,反射获取值
            Object value = field.get(t);

            //给预编译sql赋值
            pstmt.setObject(i + 1 - offset, value);
        }
        pstmt.executeUpdate();
    }

    @Override
    public void close() throws Exception {
        if (connection != null){
            connection.close();
        }
        if (pstmt!= null){
            pstmt.close();
        }
    }
}
