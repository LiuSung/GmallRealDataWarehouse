package com.gmall.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gmall.realtime.utils.DateFormatUtil;
import com.gmall.realtime.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * author: xuan.liu
 * description: 独立访客明细表
 */
public class DwdTrafficUniqueVisitorDetail {
    public static void main(String[] args) throws Exception {

        //TODO: 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStateBackend(new HashMapStateBackend());

        env.enableCheckpointing(5*60000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10*60000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000L));
        env.getCheckpointConfig().setCheckpointStorage("hdfs://192.168.141.100:9820/flink/ck");
        //TODO: 读取页面数据过滤脏数据
        String topic = "dwd_traffic_page_log";
        String groupid = "dwdtrafficuniquevisitordetail";
        DataStreamSource<String> StreamDS = env.fromSource(KafkaUtil.getKafkaSource(topic, groupid), WatermarkStrategy.noWatermarks(), "dwd_traffic_pagelog_source");

        OutputTag<String> DirtyTag = new OutputTag<String>("dirty") {};
        SingleOutputStreamOperator<String> UnDirtyDS = StreamDS.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String s, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject.toJSONString());
                } catch (Exception e) {
                    context.output(DirtyTag, s);
                }
            }
        });
        //TODO: 数据去重
        //{"common":{"ar":"110000","uid":"61","os":"Android 11.0","ch":"web","is_new":"1","md":"Redmi k30","mid":"mid_404358","vc":"v2.1.132","ba":"Redmi"},
        // "page":{"page_id":"good_detail","item":"19","during_time":2326,"item_type":"sku_id","last_page_id":"good_list","source_type":"promotion"}
        // ,"ts":1705133387000}
        KeyedStream<String, String> keybyDs = UnDirtyDS.keyBy(s -> JSON.parseObject(s).getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<String> filterDS = keybyDs.filter(new RichFilterFunction<String>() {
            private ValueState<String> CurrentDayValueStat = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> currentDayValueStat = new ValueStateDescriptor<>("CurrentDayValueStat", String.class);
                CurrentDayValueStat = getRuntimeContext().getState(currentDayValueStat);
            }

            @Override
            public boolean filter(String value) throws Exception {
                JSONObject valueJSON = JSON.parseObject(value);
                Long ts = valueJSON.getLong("ts");
                String currdate = DateFormatUtil.toDate(ts);
                if (CurrentDayValueStat == null || currdate.equals(CurrentDayValueStat)) {
                    CurrentDayValueStat.update(currdate);
                    return true;
                } else {
                    return false;
                }
            }
        });
        //TODO: 数据写入主题
        String targetTopic = "dwd_traffic_unique_visitor_detail";
        filterDS.addSink(KafkaUtil.getFlinkKafkaProducer("targetTopic"));
        //TODO: 启动任务
        env.execute("DwdTrafficUniqueVisitorDetail");

    }
}
