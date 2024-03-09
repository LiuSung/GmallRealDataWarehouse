package com.gmall.realtime.utils;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.gmall.realtime.app.func.DimJoinFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T> implements DimJoinFunction<T> {
    private DruidDataSource druidDataSource;
    private ThreadPoolExecutor threadPoolExecutor;
    private String tableName;

    public DimAsyncFunction(){

    }
    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        druidDataSource = DruidDSUtil.createDataSource();
        threadPoolExecutor = ThreadPoolUtil.getThreadPoolExecutor();
    }


    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        threadPoolExecutor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    //获取连接
                    DruidPooledConnection connection = druidDataSource.getConnection();
                    //查询维表获取维度信息
                    String key = getKey(input);
                    JSONObject dimInfo = DimUtil.getDimInfo(connection, tableName, key);
                    //将维度信息补充至当前数据
                    if(dimInfo != null){
                        join(input,dimInfo);
                    }
                    //归还连接
                    connection.close();
                    //将结果写出
                    resultFuture.complete(Collections.singletonList(input));
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("关联维表失败：" + input + ",Table:" + tableName);
                    resultFuture.complete(Collections.singletonList(input));
                }
            }
        });

    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {

    }
}
