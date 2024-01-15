package com.gmall.realtime.app.dwd.log;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gmall.realtime.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class DwdTrafficUserJumpDetail {
    public static void main(String[] args) throws Exception {

        //TODO: 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5*60000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10*60000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000L));
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://192.168.141.100:9820/flink/ck");
        System.setProperty("HADOOP_USER_NAME","root");

        //TODO: 获取page数据并定义水位线事件事件
        String topic = "dwd_traffic_page_log";
        String groupid = "dwdtrafficuserjumpdetail";
        WatermarkStrategy<String> stringWatermarkStrategy = WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<String>() {
            @Override
            public long extractTimestamp(String s, long l) {
                return JSON.parseObject(s).getLong("ts");
            }
        });
        DataStreamSource<String> sourceDs = env.fromSource(KafkaUtil.getKafkaSource(topic, groupid), stringWatermarkStrategy, "dwd_traffic_pagelog_source");

        //TODO: 过滤脏数据
        OutputTag<String> dirtyTag = new OutputTag<String>("dirty") {
        };
        SingleOutputStreamOperator<JSONObject> unDirtyDs = sourceDs.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    context.output(dirtyTag, s);
                }
            }
        });
        DataStream<String> dirtyDs = unDirtyDs.getSideOutput(dirtyTag);
        dirtyDs.print("DirtyDs>>>>>>>>>>");

        //TODO: 将数据按照mid分组
        KeyedStream<JSONObject, String> keybyDs = unDirtyDs.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));

        //TODO: 定义CEP pattern规则
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("page").getString("last_page_id") == null;
            }
        }).next("next").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("page").getString("last_page_id") == null;
            }
        }).within(Time.seconds(10));

        //TODO: 讲规则作用到keyby流
        PatternStream<JSONObject> parttenDs = CEP.pattern(keybyDs, pattern);

        //TODO: 提取超时事件以及匹配事件并将两个流合并
        OutputTag<String> timeOutTag = new OutputTag<String>("TimeOut") {
        };
        SingleOutputStreamOperator<String> selectDs = parttenDs.select(timeOutTag, new PatternTimeoutFunction<JSONObject, String>() {
            @Override
            public String timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                return map.get("start").get(0).toJSONString();
            }
        }, new PatternSelectFunction<JSONObject, String>() {
            @Override
            public String select(Map<String, List<JSONObject>> map) throws Exception {
                return map.get("start").get(0).toJSONString();
            }
        });

        DataStream<String> timeoutDs = selectDs.getSideOutput(timeOutTag);
        DataStream<String> unionDs = selectDs.union(timeoutDs);

        //TODO: 写到kafka主题
        selectDs.print("selectDS>>>>>>");
        timeoutDs.print("timeoutDs>>>>");
        String targetTopic = "dwd_traffic_user_jump_detail";
        unionDs.addSink(KafkaUtil.getFlinkKafkaProducer(targetTopic));
        //TODO: 执行程序
        env.execute("dwdtrafficuserjumpdetail");
    }
}
