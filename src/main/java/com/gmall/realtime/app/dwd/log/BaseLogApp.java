package com.gmall.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.gmall.realtime.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * author: xuan.liu
 * description: log 用户行为日志脏数据过滤以及日志类型分流
 */

public class BaseLogApp {
    public static void main(String[] args) throws Exception {

        //TODO: 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //1. 开启CheckPoint
        env.enableCheckpointing(5 * 60000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10 * 60000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L));

        //2. 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://192.168.141.100:9820/flink/ck");
        System.setProperty("HADOOP_USER_NAME", "root");

        //TODO: 获取用户行为日志将其处理成JSON,将非JSON数据写入测输出流
        String topic = "topic_log";
        String groupid = "base_applog_74033";
        DataStreamSource<String> streamSource = env.fromSource(KafkaUtil.getKafkaSource(topic, groupid), WatermarkStrategy.noWatermarks(), "base-applog");

        OutputTag<String> dirty = new OutputTag<String>("dirty"){};
        SingleOutputStreamOperator<JSONObject> JsonDs = streamSource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject valueObject = JSON.parseObject(value);
                    collector.collect(valueObject);
                } catch (Exception e){
                    context.output(dirty,value);
                }
            }
        });
        DataStream<String> DirtySideOutPut = JsonDs.getSideOutput(dirty);
        DirtySideOutPut.print(">>>>>>>>>>>");

        //TODO: 拆分数据类型写入测输出流，启动、页面、曝光、动作、错误

        //启动与页面互斥; 页面包含曝光和动作; 错误与启动、页面共存，将页面放入主流
        //{"common":{"ar":"110000","ba":"iPhone","ch":"Appstore","is_new":"1","md":"iPhone 8","mid":"mid_572134","os":"iOS 12.4.1","uid":"636","vc":"v2.1.132"},
        // "displays":[{"display_type":"activity","item":"1","item_type":"activity_id","order":1,"pos_id":5},{"display_type":"activity","item":"1","item_type":"activity_id","order":2,"pos_id":5},{"display_type":"promotion","item":"21","item_type":"sku_id","order":3,"pos_id":1},{"display_type":"promotion","item":"15","item_type":"sku_id","order":4,"pos_id":4},{"display_type":"query","item":"13","item_type":"sku_id","order":5,"pos_id":5},{"display_type":"query","item":"5","item_type":"sku_id","order":6,"pos_id":1},{"display_type":"query","item":"17","item_type":"sku_id","order":7,"pos_id":4},{"display_type":"query","item":"5","item_type":"sku_id","order":8,"pos_id":4},{"display_type":"query","item":"15","item_type":"sku_id","order":9,"pos_id":5},{"display_type":"query","item":"4","item_type":"sku_id","order":10,"pos_id":5},{"display_type":"query","item":"17","item_type":"sku_id","order":11,"pos_id":5}],"page":{"during_time":1139,"page_id":"home"},"ts":1705068674000}
        OutputTag<String> erroTag = new OutputTag<String>("erro"){};
        OutputTag<String> startTag = new OutputTag<String>("start"){};
        OutputTag<String> displayTag = new OutputTag<String>("display"){};
        OutputTag<String> actionTag = new OutputTag<String>("action"){};
        SingleOutputStreamOperator<String> pageDS = JsonDs.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {
                // 写出err
                String errlog = value.getString("err");
                if (errlog != null) {
                    context.output(erroTag, errlog);
                    value.remove("err");
                }

                // 写出start
                String startlog = value.getString("start");
                if (startlog != null) {
                    context.output(startTag, startlog);
                    value.remove("start");
                } else {
                    String common = value.getString("common");
                    String page_id = value.getJSONObject("page").getString("page_id");
                    Long ts = value.getLong("ts");

                    // 写出dispalys
                    JSONArray displaylogs = value.getJSONArray("displays");
                    if (displaylogs != null && displaylogs.size() > 1) {
                        for (int i = 0; i < displaylogs.size(); i++) {
                            JSONObject displaylog = displaylogs.getJSONObject(i);
                            displaylog.put("common", common);
                            displaylog.put("page_id", page_id);
                            displaylog.put("ts", ts);
                            context.output(displayTag, displaylog.toJSONString());
                        }
                    }
                    // 写出actions
                    JSONArray actionlogs = value.getJSONArray("actions");
                    if (actionlogs != null && actionlogs.size() > 1) {
                        for (int i = 0; i < actionlogs.size(); i++) {
                            JSONObject actionlog = actionlogs.getJSONObject(i);
                            actionlog.put("common", common);
                            actionlog.put("page_id", page_id);
                            context.output(actionTag, actionlog.toJSONString());
                        }
                    }
                    // 写出page
                    value.remove("displays");
                    value.remove("actions");
                    collector.collect(value.toJSONString());

                }
            }
        });

        //TODO: 提取各个测输出流
        DataStream<String> errDS = pageDS.getSideOutput(erroTag);
        DataStream<String> startDs = pageDS.getSideOutput(startTag);
        DataStream<String> displayDs = pageDS.getSideOutput(displayTag);
        DataStream<String> actionDS = pageDS.getSideOutput(actionTag);

        //TODO: 写出到对应主题
//        errDS.print("err>>>>>>>");
//        startDs.print("start>>>>>>");
//        displayDs.print("display>>>>>>");
//        actionDS.print("action>>>>>>");
//        pageDS.print("page>>>>>>>>");

        String page_topic = "dwd_traffic_page_log";
        String start_topic = "dwd_traffic_start_log";
        String display_topic = "dwd_traffic_display_log";
        String action_topic = "dwd_traffic_action_log";
        String error_topic = "dwd_traffic_error_log";

        errDS.addSink(KafkaUtil.getFlinkKafkaProducer(error_topic));
        startDs.addSink(KafkaUtil.getFlinkKafkaProducer(start_topic));
        displayDs.addSink(KafkaUtil.getFlinkKafkaProducer(display_topic));
        actionDS.addSink(KafkaUtil.getFlinkKafkaProducer(action_topic));
        pageDS.addSink(KafkaUtil.getFlinkKafkaProducer(page_topic));
        //TODO: 执行操作
        env.execute("BaseLogApp");
    }
}
