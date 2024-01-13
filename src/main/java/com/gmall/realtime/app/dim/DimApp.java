package com.gmall.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gmall.realtime.app.func.DimSinkFunction;
import com.gmall.realtime.app.func.TableProcessFunction;
import com.gmall.realtime.bean.TableProcess;
import com.gmall.realtime.utils.KafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * author: xuan.liu
 * description: OdsToDim Dim维度表动态创建以及数据更新
 */
public class DimApp {
    public static void main(String[] args) throws Exception {
        //todo 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //1.1 开启checkpoint
        env.enableCheckpointing(5 * 60000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10 * 60000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000L));
        //1.2 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://192.168.141.100:9820/flink/ck");
        System.setProperty("HADOOP_USER_NAME","root");

        //todo 2. 读取kafka topic_db主题数据创建主流
        String topic = "topic_db";
        String group_id = "dim_app_74033";
        DataStreamSource<String> kafkaDs = env.fromSource(KafkaUtil.getKafkaSource(topic, group_id), WatermarkStrategy.noWatermarks(), "dim-kafkaSource");
        //todo 3. 过滤掉非JSON数据 & 保留新增、变化以及初始化数据
        SingleOutputStreamOperator<JSONObject> jsonDs = kafkaDs.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    String type = jsonObject.getString("type");
                    if ("insert".equals(type) || "update".equals(type) || "bootstrap-instert".equals(type)) {
                        collector.collect(jsonObject);
                    }
                } catch (Exception e) {
                    System.out.println("脏数据：" + s);
                }
            }
        });
        //todo 4. 使用flinkCDC 读取Mysql配置信息创建配置流
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("192.168.141.100")
                .port(3306)
                .username("root")
                .password("000000")
                .databaseList("gmall-config")
                .tableList("gmall-config.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
        DataStreamSource<String> mysqlDs = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "dim-mysqlSource");
        //todo 5. 将配置流处理成广播流
        MapStateDescriptor<String, TableProcess> mapstate = new MapStateDescriptor<>("map-stateDes", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = mysqlDs.broadcast(mapstate);
        //todo 6. 主流与广播流connect
        BroadcastConnectedStream<JSONObject, String> connectStream = jsonDs.connect(broadcastStream);
        //todo 7. 处理连接流，根据配置信息处理主流
        SingleOutputStreamOperator<JSONObject> dimDs = connectStream.process(new TableProcessFunction(mapstate));
        //todo 8. 将数据写到Phoenix
        dimDs.addSink(new DimSinkFunction());
        //todo 9. 启动任务
        env.execute("Ods_To_Dim");
    }
}
