package com.gmall.realtime.app.dwd.db;

import com.gmall.realtime.utils.KafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdToolCouponOrder {
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

        // TODO 2. 从 Kafka 读取业务数据，封装为 Flink SQL 表
        tableEnv.executeSql(KafkaUtil.getTopicDb("dwdtoolcouponorder"));

        // TODO 3. 读取优惠券领用表数据，筛选满足条件的优惠券下单数据
        Table couponUseOrder = tableEnv.sqlQuery("select " +
                "data['id'] id, " +
                "data['coupon_id'] coupon_id, " +
                "data['user_id'] user_id, " +
                "data['order_id'] order_id, " +
                "date_format(data['using_time'],'yyyy-MM-dd') date_id, " +
                "data['using_time'] using_time " +
                "from topic_db " +
                "where `table` = 'coupon_use' " +
                "and `type` = 'update' " +
                "and data['coupon_status'] = '1402' " +
                "and `old`['coupon_status'] = '1401'");

        tableEnv.createTemporaryView("result_table", couponUseOrder);

        // TODO 4. 建立 Kafka-Connector dwd_tool_coupon_order 表
        tableEnv.executeSql("create table dwd_tool_coupon_order( " +
                "id string, " +
                "coupon_id string, " +
                "user_id string, " +
                "order_id string, " +
                "date_id string, " +
                "order_time string " +
                ")" + KafkaUtil.getKafkaSinkDDL("dwd_tool_coupon_order"));

        // TODO 5. 将数据写入 Kafka-Connector 表
        tableEnv.executeSql("" +
                "insert into dwd_tool_coupon_order select " +
                "id, " +
                "coupon_id, " +
                "user_id, " +
                "order_id, " +
                "date_id, " +
                "using_time order_time " +
                "from result_table").print();
        env.execute("dwdtoolcouponorder");
    }
}
