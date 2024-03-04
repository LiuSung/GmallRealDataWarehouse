package com.gmall.realtime.utils;

import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 当前工具类可以适用于任何JDBC方式访问的数据库中的任何查询语句
 */
public class JdbcUtil {
    public static <T> List<T> queryList(Connection connection, String sql,Class<T> clz, boolean underScoreToCamel) throws SQLException, InstantiationException, IllegalAccessException, InvocationTargetException {
        // 创建集合用于存放结果数据
        ArrayList<T> result = new ArrayList<>();
        //编译SQL
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        //执行查询
        ResultSet resultSet = preparedStatement.executeQuery();
        //获取查询的元数据信息
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        //遍历结果集，将每行数据转换为T对象并加入结果集
        while (resultSet.next()){
            //创建t对象
            T t = clz.newInstance();

            //列遍历，并给T对象赋值
            for(int i = 0; i < columnCount; i++){

                //获取列名与列值
                String columnName = metaData.getColumnName(i+1);
                Object value = resultSet.getObject(columnName);

                //判断是否需要将列名转换为驼峰命名
                if(underScoreToCamel){
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,columnName.toLowerCase());
                }
                //泛型对象赋值
                BeanUtils.setProperty(t,columnName,value);
            }
            result.add(t);
        }
        resultSet.close();
        preparedStatement.close();
        // 返回集合
        return result;
    }
}
