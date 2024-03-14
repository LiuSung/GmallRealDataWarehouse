package com.gmall.realtime.app.dwd.db;
import com.gmall.realtime.utils.KafkaUtil;
import com.gmall.realtime.utils.MysqlUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class DwdInteractionComment {
    public static void main(String[] args) throws Exception {

        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//        //1. 开启CheckPoint
//        env.enableCheckpointing(5 * 60000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10 * 60000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L));
//
//        //2. 设置状态后端
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://192.168.141.100:9820/flink/ck");
//        System.setProperty("HADOOP_USER_NAME", "root");

        //设置ttl为5s
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));
        // TODO 2. 从 Kafka 读取业务数据，封装为 Flink SQL 表
        tableEnv.executeSql(KafkaUtil.getTopicDb("dwdinteractioncomment"));

        // TODO 3. 读取评论表数据
        Table commentInfo = tableEnv.sqlQuery("select " +
                "data['id'] id, " +
                "data['user_id'] user_id, " +
                "data['sku_id'] sku_id, " +
                "data['order_id'] order_id, " +
                "data['create_time'] create_time, " +
                "data['appraise'] appraise, " +
                "`pt` pt " +
                "from topic_db " +
                "where `table` = 'comment_info' " +
                "and `type` = 'insert' ");
        tableEnv.createTemporaryView("comment_info", commentInfo);

        // TODO 4. 建立 MySQL-LookUp 字典表
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());

        // TODO 5. 关联两张表
        Table resultTable = tableEnv.sqlQuery("select " +
                "ci.id, " +
                "ci.user_id, " +
                "ci.sku_id, " +
                "ci.order_id, " +
                "date_format(ci.create_time,'yyyy-MM-dd') date_id, " +
                "ci.create_time, " +
                "ci.appraise, " +
                "dic.dic_name " +
                "from comment_info ci " +
                "join " +
                "base_dic for system_time as of ci.pt as dic " +
                "on ci.appraise = dic.dic_code");
        tableEnv.createTemporaryView("result_table", resultTable);

        // TODO 6. 建立 Kafka-Connector dwd_interaction_comment 表
        tableEnv.executeSql("create table dwd_interaction_comment( " +
                "id string, " +
                "user_id string, " +
                "sku_id string, " +
                "order_id string, " +
                "date_id string, " +
                "create_time string, " +
                "appraise_code string, " +
                "appraise_name string " +
                ")" + KafkaUtil.getKafkaSinkDDL("dwd_interaction_comment"));

        // TODO 7. 将关联结果写入 Kafka-Connector 表
        tableEnv.executeSql("" +
                "insert into dwd_interaction_comment select * from result_table");
        env.execute("dwdinteractioncomment");
    }
}