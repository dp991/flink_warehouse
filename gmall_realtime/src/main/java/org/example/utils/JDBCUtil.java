package org.example.utils;

import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * jdbc通用工具类
 */
public class JDBCUtil {

    /**
     * 通用查询语句
     *
     * @param connection
     * @param sql
     * @param clz
     * @param underScoreToCamel
     * @param <T>
     * @return
     * @throws Exception
     */
    public static <T> List<T> queryList(Connection connection, String sql, Class<T> clz, boolean underScoreToCamel) throws Exception {

        ArrayList<T> resultList = new ArrayList<>();
        //预编译sql
        PreparedStatement preparedStatement = connection.prepareStatement(sql);

        //执行查询
        ResultSet resultSet = preparedStatement.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        //解析结果
        while (resultSet.next()) {
            //创建泛型对象
            T t = clz.newInstance();
            //泛型对象赋值
            for (int i = 1; i < columnCount + 1; i++) {
                //获取列名
                String columnName = metaData.getColumnName(i);
                //判断是否需要转换为驼峰命名
                if (underScoreToCamel) {
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());
                }
                //获取值
                Object value = resultSet.getObject(i);
                //给泛型对象赋值
                BeanUtils.setProperty(t, columnName, value);
            }
            //对象添加到集合
            resultList.add(t);
        }
        preparedStatement.close();
        resultSet.close();
        return resultList;
    }

}
