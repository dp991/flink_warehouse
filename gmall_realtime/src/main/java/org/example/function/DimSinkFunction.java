package org.example.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.example.common.GmallConfig;
import org.example.utils.DimUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Collection;
import java.util.Set;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {

    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
//        System.out.println("数据库链接初始化");
        Class.forName(GmallConfig.MYSQL_DRIVER_NAME);
        connection = DriverManager.getConnection(GmallConfig.MYSQL_URL, GmallConfig.MYSQL_USER, GmallConfig.MYSQL_PASSWORD);
        connection.setAutoCommit(true);
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        PreparedStatement preparedStatement = null;
        try {
            //获取sql
            String sinkTable = value.getString("sinkTable");
            JSONObject after = value.getJSONObject("after");
            String sql = genUpsertSQL(sinkTable, after);

            System.out.println("sql:" + sql);

            //预编译sql
            preparedStatement = connection.prepareStatement(sql);

            //判断当前数据如果为更新操作，则先删除redis中的数据
            if ("update".equals(value.getString("type"))) {
                DimUtil.delRedisDimInfo(sinkTable.toUpperCase(), after.getString("id"));
            }

            //执行sql
            preparedStatement.executeUpdate();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
    }

    // sql:upsert into db.tn(id,tm_name) values('.'...)
    private String genUpsertSQL(String sinkTable, com.alibaba.fastjson.JSONObject data) {

        Set<String> keySey = data.keySet();
        Collection<Object> values = data.values();
        return "replace into " + GmallConfig.MYSQL_DATABASE_NAME + "." + sinkTable + "(" + StringUtils.join(keySey, ",") + ") values ('" +
                StringUtils.join(values, "','") + "') ";
    }
}
