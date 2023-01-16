package org.example.utils;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.example.bean.TransientSink;
import org.example.common.GmallConfig;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * clickhouse 工具类
 * <p>
 * 正常              反射
 * object.getField() => field.get(obj)
 * object.getMethod() => method.invoke(obj,args)
 */
public class ClickHouseUtil {

    public static <T> SinkFunction<T> getSinkFunction(String sql) {

        return JdbcSink.<T>sink(
                sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                        try {
                            //获取所有的属性信息
                            Field[] fields = t.getClass().getDeclaredFields();
//                            Method[] methods = t.getClass().getMethods();
//                            for (Method method:methods){
//                                method.invoke(t,"");
//                            }
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
                                preparedStatement.setObject(i + 1 - offset, value);

                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                },
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(20).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .withUsername(GmallConfig.CLICKHOUSE_USER)
                        .withPassword(GmallConfig.CLICKHOUSE_PASSWD)
                        .build());
    }

}
