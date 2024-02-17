package com.gmall.realtime.app.dwd.db;

import com.gmall.realtime.utils.KafkaUtil;
import com.gmall.realtime.utils.MysqlUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class DwdTradeOrderPreProcess {
    public static void main(String[] args) throws Exception {
        //TODO 1. 获取执行环境
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
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));
        //TODO 2. 创建topic_表
        tableEnv.executeSql(KafkaUtil.getTopicDb("dwdtradeorderpreprocess"));
        //TODO 3. 过滤出订单明细数据
        Table orderDetailTable = tableEnv.sqlQuery("select  " +
                "  `data`['id'] id, " +
                "  `data`['order_id'] order_id, " +
                "  `data`['sku_id'] sku_id, " +
                "  `data`['sku_name'] sku_name, " +
                "  `data`['order_price'] order_price, " +
                "  `data`['sku_num'] sku_num, " +
                "  `data`['create_time'] create_time, " +
                "  `data`['source_type'] source_type, " +
                "  `data`['source_id'] source_id, " +
                "  `data`['split_total_amount'] split_total_amount, " +
                "  `data`['split_activity_amount'] split_activity_amount, " +
                "  `data`['split_coupon_amount'] split_coupon_amount, " +
                "  `pt` pt " +
                "from topic_db " +
                "where `database` = 'gmall' " +
                "and `table` = 'order_detail' " +
                "and `type` = 'insert'");
        tableEnv.createTemporaryView("order_detail_table",orderDetailTable);
        //TODO 4. 过滤出订单数据
        Table orderInfoTable = tableEnv.sqlQuery("select " +
                "    `data`['id'] id, " +
                "    `data`['consignee'] consignee, " +
                "    `data`['consignee_tel'] consignee_tel, " +
                "    `data`['total_amount'] total_amount, " +
                "    `data`['order_status'] order_status, " +
                "    `data`['user_id'] user_id, " +
                "    `data`['payment_way'] payment_way, " +
                "    `data`['delivery_address'] delivery_address, " +
                "    `data`['order_comment'] order_comment, " +
                "    `data`['out_trade_no'] out_trade_no, " +
                "    `data`['trade_body'] trade_body, " +
                "    `data`['create_time'] create_time, " +
                "    `data`['operate_time'] operate_time, " +
                "    `data`['expire_time'] expire_time, " +
                "    `data`['process_status'] process_status, " +
                "    `data`['tracking_no'] tracking_no, " +
                "    `data`['parent_order_id'] parent_order_id, " +
                "    `data`['province_id'] province_id, " +
                "    `data`['activity_reduce_amount'] activity_reduce_amount, " +
                "    `data`['coupon_reduce_amount'] coupon_reduce_amount, " +
                "    `data`['original_total_amount'] original_total_amount, " +
                "    `data`['feight_fee'] feight_fee, " +
                "    `data`['feight_fee_reduce'] feight_fee_reduce, " +
                "    `data`['refundable_time'] refundable_time, " +
                "    `type`, " +
                "    `old` " +
                "from topic_db " +
                "where `database` = 'gmall' " +
                "and `table` = 'order_info' " +
                "and (`type` = 'insert' or `type` = 'update')");
        tableEnv.createTemporaryView("order_info_table",orderInfoTable);
        //TODO 5. 过滤出订单明细活动关联数据
        Table orderDetailActivityTable = tableEnv.sqlQuery("select " +
                "    `data`['id'] id, " +
                "    `data`['order_id'] order_id, " +
                "    `data`['order_detail_id'] order_detail_id, " +
                "    `data`['activity_id'] activity_id, " +
                "    `data`['activity_rule_id'] activity_rule_id, " +
                "    `data`['sku_id'] sku_id, " +
                "    `data`['create_time'] create_time " +
                "from topic_db " +
                "where `database` = 'gmall' " +
                "and `table` = 'order_detail_activity' " +
                "and `type` = 'insert'");
        tableEnv.createTemporaryView("order_activity_table",orderDetailActivityTable);
        //TODO 6. 过滤出订单明细购物券关联数据
        Table orderDetailCouponTable = tableEnv.sqlQuery("select " +
                "    `data`['id'] id, " +
                "    `data`['order_id'] order_id, " +
                "    `data`['order_detail_id'] order_detail_id, " +
                "    `data`['coupon_id'] coupon_id, " +
                "    `data`['coupon_use_id'] coupon_use_id, " +
                "    `data`['sku_id'] sku_id, " +
                "    `data`['create_time'] create_time " +
                "from topic_db " +
                "where `database` = 'gmall' " +
                "and `table` = 'order_detail_coupon' " +
                "and `type` = 'insert'");
        tableEnv.createTemporaryView("order_coupon_table",orderDetailCouponTable);
        //TODO 7. 创建base_dic look_up表
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());
        //TODO 8. 关联这5张表
        Table resultDataTable = tableEnv.sqlQuery("select  " +
                "    od.id, " +
                "    od.order_id, " +
                "    od.sku_id, " +
                "    od.sku_name, " +
                "    od.order_price, " +
                "    od.sku_num, " +
                "    od.create_time, " +
                "    od.source_type, " +
                "    od.source_id, " +
                "    od.split_total_amount, " +
                "    od.split_activity_amount, " +
                "    od.split_coupon_amount, " +
                "    dic.dic_code source_type_id, " +
                "    dic.dic_name source_type_name, " +
                "    oi.consignee, " +
                "    oi.consignee_tel, " +
                "    oi.total_amount, " +
                "    oi.order_status, " +
                "    oi.user_id, " +
                "    oi.payment_way, " +
                "    oi.delivery_address, " +
                "    oi.order_comment, " +
                "    oi.out_trade_no, " +
                "    oi.trade_body, " +
                "    oi.operate_time, " +
                "    oi.expire_time, " +
                "    oi.process_status, " +
                "    oi.tracking_no, " +
                "    oi.parent_order_id, " +
                "    oi.province_id, " +
                "    oi.activity_reduce_amount, " +
                "    oi.coupon_reduce_amount, " +
                "    oi.original_total_amount, " +
                "    oi.feight_fee, " +
                "    oi.feight_fee_reduce, " +
                "    oi.refundable_time, " +
                "    oa.id order_detail_activity_id, " +
                "    oa.activity_id, " +
                "    oa.activity_rule_id, " +
                "    oc.id order_detail_coupon_id, " +
                "    oc.coupon_id, " +
                "    oc.coupon_use_id, " +
                "    oi.`type`, " +
                "    oi.`old`, " +
                "    current_row_timestamp() row_op_ts " +
                "from order_detail_table od " +
                "join order_info_table oi " +
                "on od.order_id = oi.id " +
                "left join order_activity_table oa " +
                "on od.id = oa.order_detail_id " +
                "left join order_coupon_table oc " +
                "on od.id = oc.order_detail_id " +
                "join base_dic FOR SYSTEM_TIME AS OF od.pt as dic " +
                "on od.source_type = dic.dic_code");
        tableEnv.createTemporaryView("result_data_table",resultDataTable);
        //TODO 9. 创建upsert-kafka表
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
                "     row_op_ts TIMESTAMP_LTZ(3), " +
                "     PRIMARY KEY (order_id) NOT ENFORCED " +
                ") " + KafkaUtil.getKafkaUpsertSinkDDL("dwd_trade_order_pre_process"));
        //TODO 10. 将数据写出
        tableEnv.executeSql("insert into order_detail_pre_table select * from result_data_table").print();
        //TODO 11. 启动任务
        env.execute("dwdtradeorderpreprocess");
    }
}
