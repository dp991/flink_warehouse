package org.example.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.bean.TableProcess;
import org.example.common.GmallConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * 动态分流
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private OutputTag<JSONObject> outputTag;

    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public TableProcessFunction(OutputTag<JSONObject> outputTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.outputTag = outputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化建表phoenix
        System.out.println("==================open方法初始化===========================");
        Class.forName(GmallConfig.MYSQL_DRIVER_NAME);
        connection = DriverManager.getConnection(GmallConfig.MYSQL_URL, GmallConfig.MYSQL_USER, GmallConfig.MYSQL_PASSWORD);
    }

    @Override
    public void processBroadcastElement(String value, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        //广播流 1、解析数据（string=>TableProcess）2、检查hbase建表 3、写入状态
        JSONObject jsonObject = JSON.parseObject(value);
        String data = jsonObject.getString("after");
        TableProcess tableProcess = JSON.parseObject(data, TableProcess.class);

        //建表
        if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
            checkTable(tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkExtend());
        }
        //写入状态,进行广播
        BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapStateDescriptor);
        String key = tableProcess.getSourceTable() + "-" + tableProcess.getOperateType();
        broadcastState.put(key, tableProcess);
    }

    @Override
    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {
        //主流 1、读取状态 2、过滤数据 3、分流
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        String tableName = value.getString("tableName");
        String type = value.getString("type");
        String key = tableName + "-" + type;
        TableProcess tableProcess = broadcastState.get(key);
        if (tableProcess != null) {
            //2、过滤数据
            JSONObject data = value.getJSONObject("after");
            filterColumn(data, tableProcess.getSinkColumns());

            //3、分流
            //将输出表/主题信息写入value
            value.put("sinkTable", tableProcess.getSinkTable());
            String sinkType = tableProcess.getSinkType();
            if (TableProcess.SINK_TYPE_KAFKA.equals(sinkType)) {
                //kafka数据写入主流
                collector.collect(value);

            } else if (TableProcess.SINK_TYPE_HBASE.equals(sinkType)) {
                //hbase数据写入测输出流
                readOnlyContext.output(outputTag, value);
            }

        }
    }

    /**
     * 建表语句：create table if not exists db.tn(id varchar primary key,tm_name varchar) xxx;
     *
     * @param sinkTable
     * @param sinkColumns
     * @param sinkPk
     * @param sinkExtend
     */
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {

        if (sinkPk == null) {
            sinkPk = "id";
        }

        if (sinkExtend == null) {
            sinkExtend = "";
        }
        StringBuffer createTableSQL = new StringBuffer(" create table if not exists ")
                .append(GmallConfig.MYSQL_DATABASE_NAME)
                .append(".")
                .append(sinkTable)
                .append("(");

        String[] fieds = sinkColumns.split(",");
        for (int i = 0; i < fieds.length; i++) {
            String fied = fieds[i];
            //判断是否为主键
            if (sinkPk.equals(fied)) {
                createTableSQL.append(fied).append(" VARCHAR(255) PRIMARY KEY ");
            } else {
                createTableSQL.append(fied).append(" VARCHAR(255) ");
            }
            //判断是否为最后一个字段
            if (i < fieds.length - 1) {
                createTableSQL.append(",");
            }
        }
        //hbase建表时使用sinkExtend
//        createTableSQL.append(")").append(sinkExtend);
        createTableSQL.append(")");
        //打印建表语句
        System.out.println(createTableSQL);

        PreparedStatement preparedStatement = null;
        try {
            //预编译sql
            preparedStatement = connection.prepareStatement(createTableSQL.toString());
            //执行
            preparedStatement.execute();
        } catch (SQLException e) {
            throw new RuntimeException("建表失败: " + sinkTable);
        } finally {
            try {
                if (preparedStatement != null) {
                    preparedStatement.close();
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

        }

    }

    /**
     * 过滤字段
     *
     * @param data
     * @param sinkColumns
     */
    private void filterColumn(JSONObject data, String sinkColumns) {
        String[] fieds = sinkColumns.split(",");
        List<String> columns = Arrays.asList(fieds);

//        Iterator<Map.Entry<String, Object>> iterator = data.entrySet().iterator();

//        while (iterator.hasNext()) {
//            Map.Entry<String, Object> next = iterator.next();
//            if (!columns.contains(next.getKey())){
//                iterator.remove();
//            }
//        }

        data.entrySet().removeIf(next -> !columns.contains(next.getKey()));

    }

}
