package com.gmall.realtime.app.dwd.db;


import com.gmall.realtime.utils.KafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeOrderDetail {
    public static void main(String[] args) throws Exception {
        //TODO 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

//        env.enableCheckpointing(5*60000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10*6000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://192.168.141.100:9820/flink/ck");
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000L));
//        env.setStateBackend(new HashMapStateBackend());
//        System.setProperty("HADOOP_USER_NAME","root");
        //TODO 2. 读取Kafka订单预处理主题数据创建表
        tableEnv.executeSql("create table order_detail_pre_table( " +
                "    `id` string, " +
                "    `order_id` string, " +
                "    `sku_id` string, " +
                "    `sku_name` string, " +
                "    `order_price` string, " +
                "    `sku_num` string, " +
                "    `create_time` string, " +
                "    `source_type` string, " +
                "    `source_id` string, " +
                "    `split_total_amount` string, " +
                "    `split_activity_amount` string, " +
                "    `split_coupon_amount` string, " +
                "    `source_type_id` string, " +
                "    `source_type_name` string, " +
                "    `consignee` string, " +
                "    `consignee_tel` string, " +
                "    `total_amount` string, " +
                "    `order_status` string, " +
                "    `user_id` string, " +
                "    `payment_way` string, " +
                "    `delivery_address` string, " +
                "    `order_comment` string, " +
                "    `out_trade_no` string, " +
                "    `trade_body` string, " +
                "    `operate_time` string, " +
                "    `expire_time` string, " +
                "    `process_status` string, " +
                "    `tracking_no` string, " +
                "    `parent_order_id` string, " +
                "    `province_id` string, " +
                "    `activity_reduce_amount` string, " +
                "    `coupon_reduce_amount` string, " +
                "    `original_total_amount` string, " +
                "    `feight_fee` string, " +
                "    `feight_fee_reduce` string, " +
                "    `refundable_time` string, " +
                "    `order_detail_activity_id` string, " +
                "    `activity_id` string, " +
                "    `activity_rule_id` string, " +
                "    `order_detail_coupon_id` string, " +
                "    `coupon_id` string, " +
                "    `coupon_use_id` string, " +
                "    `type` string, " +
                "    `old` map<string, string>, " +
                "    row_op_ts TIMESTAMP_LTZ(3) "+
                ") "+ KafkaUtil.getKafkaDDL("dwd_trade_order_pre_process","dwdtradeorderdetail"));
        //TODO 3. 过滤出下单数据，即新增数据
        Table orderTable = tableEnv.sqlQuery("select " +
                "    `id`,  " +
                "    `order_id`,  " +
                "    `order_price`, " +
                "    `user_id`,  " +
                "    `sku_id`,  " +
                "    `sku_name`,  " +
                "    `sku_num`,  " +
                "    `province_id`,  " +
                "    `activity_id`,  " +
                "    `activity_rule_id`,  " +
                "    `coupon_id`,  " +
                "    `create_time`,  " +
                "    `source_id`,  " +
                "    `source_type_id`,  " +
                "    `source_type_name`,  " +
                "    `split_activity_amount`,  " +
                "    `split_coupon_amount`,  " +
                "    `split_total_amount`, " +
                "    `row_op_ts` " +
                "from order_detail_pre_table " +
                "where `type` = 'insert'");
        tableEnv.createTemporaryView("order_table",orderTable);
        //TODO 4. 创建DDL下单事实表
        tableEnv.executeSql("create table dwd_trade_order_detail( " +
                "    `id` string,  " +
                "    `order_id` string,  " +
                "    `order_price` string, " +
                "    `user_id` string,  " +
                "    `sku_id` string,  " +
                "    `sku_name` string,  " +
                "    `sku_num` string,  " +
                "    `province_id` string,  " +
                "    `activity_id` string,  " +
                "    `activity_rule_id` string,  " +
                "    `coupon_id` string,  " +
                "    `create_time` string,  " +
                "    `source_id` string,  " +
                "    `source_type_id` string,  " +
                "    `source_type_name` string,  " +
                "    `split_activity_amount` string,  " +
                "    `split_coupon_amount` string,  " +
                "    `split_total_amount` string, " +
                "    `row_op_ts` TIMESTAMP_LTZ(3) " +
                ") " + KafkaUtil.getKafkaSinkDDL("dwd_trade_order_detail"));
        //TODO 5. 将数据写入kafka
        tableEnv.executeSql("insert into dwd_trade_order_detail select * from order_table").print();
        //TODO 6. 执行环境
        env.execute("dwdtradeorderdetail");
    }

}
