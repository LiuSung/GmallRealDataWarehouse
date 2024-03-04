package com.gmall.realtime.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.gmall.realtime.utils.DimUtil;
import com.gmall.realtime.utils.DruidDSUtil;
import com.gmall.realtime.utils.PhoenixUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {
    private DruidDataSource druidDataSource = null;
    @Override
    public void open(Configuration parameters) throws Exception {
        druidDataSource = DruidDSUtil.createDataSource();
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        //获取连接
        DruidPooledConnection connection = druidDataSource.getConnection();

        String sinkTable = value.getString("SinkTable");
        JSONObject data = value.getJSONObject("data");
        //获取数据类型
        String type = value.getString("type");
        //如果数据类型为update，则删除redis缓存中的数据
        if(type.equals("update")){
            DimUtil.delDimInfo(sinkTable.toUpperCase(),data.getString("id"));
        }
        //写出数据
        PhoenixUtil.upSertValues(connection,sinkTable,data);
        //归还连接
        connection.close();
    }
}
