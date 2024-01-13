package com.gmall.realtime.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gmall.realtime.bean.TableProcess;
import com.gmall.realtime.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private Connection connection;
    private MapStateDescriptor<String, TableProcess> broadcastDescriptor;

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapstate) {
        this.broadcastDescriptor = mapstate;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        PreparedStatement preparedStatement = null;

        try {
            //特殊字符处理
            if(sinkPk == null || "".equals(sinkPk)){
                sinkPk = "id";
            }
            if(sinkExtend == null){
                sinkExtend = "";
            }
            //拼接SQL
            StringBuilder createTableSql = new StringBuilder("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");
            String[] columns = sinkColumns.split(",");
            for (int i = 0; i < columns.length; i++) {
                String column = columns[i];
                if(sinkPk.equals(column)){
                    createTableSql.append(column).append(" varchar primary key");
                }else{
                    createTableSql.append(column).append(" varchar");
                }
                if(i < columns.length -1){
                    createTableSql.append(",");
                }
            }
            createTableSql.append(")").append(sinkExtend);

            //编译sql
            preparedStatement = connection.prepareStatement(createTableSql.toString());
            //执行sql
            preparedStatement.execute();
        } catch (SQLException e) {
            throw new RuntimeException("建表失败: " + sinkTable);
        }finally {
            if(preparedStatement != null){
                try {
                    //释放资源
                    preparedStatement.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    //{"before":null,"after":{"source_table":"aa","sink_table":"bb","sink_columns":"cc","sink_pk":"dd","sink_extend":"ee"},
    // "source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1704894465000,"snapshot":"false","db":"gmall-config","sequence":null,"table":"table_process","server_id":1,"gtid":null,"file":"mysql-bin.000008","pos":377,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1704894465683,"transaction":null}
    @Override
    public void processBroadcastElement(String s, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        JSONObject jsonObject = JSON.parseObject(s);
        TableProcess tableProcess = JSON.parseObject(jsonObject.getString("after"), TableProcess.class);

        checkTable(tableProcess.getSinkTable(),
                   tableProcess.getSinkColumns(),
                   tableProcess.getSinkPk(),
                   tableProcess.getSinkExtend());

        //写入状态广播出去
        BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(broadcastDescriptor);
        broadcastState.put(tableProcess.getSourceTable(),tableProcess);
    }

    private void filter(JSONObject data, String SinkColumns) {
        String[] columns = SinkColumns.split(",");
        List<String> list = Arrays.asList(columns);

        Set<Map.Entry<String, Object>> entries = data.entrySet();
        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
        while(iterator.hasNext()){
            Map.Entry<String, Object> next = iterator.next();
            if(!list.contains(next.getKey())){
                iterator.remove();
            }
        }
    }

    //{"database":"gmall","table":"favor_info","type":"insert","ts":1704798219,"xid":217972,"commit":true,
    // "data":{"id":1744269580549595177,"user_id":null,"sku_id":null,"spu_id":null,"is_cancel":null,"create_time":null,"cancel_time":null}}
    @Override
    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(broadcastDescriptor);
        String table = value.getString("table");
        TableProcess tableProcess = broadcastState.get(table);

        //判断tableProcess是否为Null,如果为Null说明广播状态中没有该表，则丢弃此数据
        if(tableProcess != null){
            filter(value.getJSONObject("data"),tableProcess.getSinkColumns());
            value.put("SinkTable",tableProcess.getSinkTable());
            collector.collect(value);
        }else {
            System.out.println("not found the key from BroadcastStatment: "+ table);
        }
    }


}
