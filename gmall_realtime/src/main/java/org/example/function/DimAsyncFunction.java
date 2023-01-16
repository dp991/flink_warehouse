package org.example.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;

import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.example.common.GmallConfig;
import org.example.utils.DimUtil;
import org.example.utils.ThreadPoolUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 维度异步查询
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimAsyncJoinFunction<T> {

    private Connection connection;
    private ThreadPoolExecutor threadPoolExecutor;
    public String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    /**
     * 链接初始化
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.MYSQL_DRIVER_NAME);
        connection = DriverManager.getConnection(GmallConfig.MYSQL_URL, GmallConfig.MYSQL_USER, GmallConfig.MYSQL_PASSWORD);
        threadPoolExecutor = ThreadPoolUtil.getThreadPoolExecutor();
    }


    /**
     * 业务处理
     *
     * @param input
     * @param resultFuture
     */
    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) {

        threadPoolExecutor.submit(new Runnable() {
            @Override
            public void run() {
                //获取查询的主键
                String id = getKey(input);
                //查询维度信息
                try {
                    JSONObject dimInfo = DimUtil.getDimInfo(connection, tableName, id);
                    //补充维度信息
                    if (dimInfo != null) {
                        join(input, dimInfo);
                    }
                    //将数据输出
                    resultFuture.complete(Collections.singletonList(input));

                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

            }
        });

    }

    /**
     * 超时处理 可以再次访问数据库
     *
     * @param input
     * @param resultFuture
     */
    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) {
        System.out.println("timeout:" + input);
    }
}
