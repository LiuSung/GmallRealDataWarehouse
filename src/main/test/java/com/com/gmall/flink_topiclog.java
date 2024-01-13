package com.com.gmall;

import com.gmall.realtime.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class flink_topiclog {
    public static void main(String[] args) throws Exception {
        //TODO: 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //TODO: 获取用户行为日志将其处理成JSON,将非JSON数据写入测输出流
        String topic = "topic_log";
        String groupid = "base_applog_74033";
        DataStreamSource<String> streamSource = env.fromSource(KafkaUtil.getKafkaSource(topic, groupid), WatermarkStrategy.noWatermarks(), "base-applog");
        streamSource.print();
        env.execute();

    }
}
