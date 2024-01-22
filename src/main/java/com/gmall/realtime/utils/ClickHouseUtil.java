package com.gmall.realtime.utils;

import com.gmall.realtime.bean.TransientSink;
import com.gmall.realtime.common.GmallConfig;
import lombok.SneakyThrows;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ClickHouseUtil {
    public static <T> SinkFunction<T> getSinkFuction(String sql){
        return JdbcSink.<T>sink(sql,
                new JdbcStatementBuilder<T>() {
                    @SneakyThrows
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                        //通过反射获取t对象中数据属性
                        Class<?> tClass = t.getClass();
                        Field[] declaredFields = tClass.getDeclaredFields();
                        //遍历属性
                        int offset = 0;
                        for (int i = 0; i < declaredFields.length; i++) {
                            Field field = declaredFields[i];
                            field.setAccessible(true);
                            Annotation annotation = field.getAnnotation(TransientSink.class);
                            if (annotation != null){
                                offset++;
                                continue;
                            }

                            Object value = field.get(t);
                            preparedStatement.setObject(i+1-offset,value);

                        }
                    }
                },
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(5)
                        .withBatchIntervalMs(1000L)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build());
    }
}
