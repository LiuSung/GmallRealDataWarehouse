package com.gmall.realtime.app.dwd.db;

import com.gmall.realtime.utils.KafkaUtil;
import com.gmall.realtime.utils.MysqlUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradePayDetail {
    public static void main(String[] args) throws Exception {
        //TODO: 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.setStateBackend(new HashMapStateBackend());
//        env.enableCheckpointing(5 * 60000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10 * 60000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://192.168.141.100:9820/flink/ck");
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000L));
//        System.setProperty("HADOOP_USER_NAME","root");
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //TODO: 2. 获取kafka topic_db表数据
        tableEnv.executeSql(KafkaUtil.getTopicDb("dwdtradepaydeatil"));
        //TODO: 3. 过滤出支付成功数据
        Table paymentInfo = tableEnv.sqlQuery("select " +
                "    `data`['order_id'] order_id, " +
                "    `data`['user_id'] user_id, " +
                "    `data`['payment_type'] payment_type, " +
                "    `data`['callback_time'] callback_time, " +
                "    `pt` " +
                "from topic_db " +
                "where `database` = 'gmall' " +
                "and `table` = 'payment_info' " +
                "and `type` = 'update' " +
                "and `data`['payment_status'] = '1602'");
        tableEnv.createTemporaryView("payment_info",paymentInfo);
        //TODO: 4. 获取kafka dwd_trade_order_detail表数据
        tableEnv.executeSql("create table dwd_trade_order_detail( " +
                "    `id` string,  " +
                "    `order_id` string,  " +
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
                "    `split_total_amount` string " +
                ") " + KafkaUtil.getKafkaDDL("dwd_trade_order_detail","dwdtradepaydeatil"));
        //TODO: 5. 获取mysql base_dic数据
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());
        //TODO: 6. 三表关联
        Table resultTable = tableEnv.sqlQuery("" +
                "select " +
                "od.id order_detail_id, " +
                "od.order_id, " +
                "od.user_id, " +
                "od.sku_id, " +
                "od.sku_name, " +
                "od.province_id, " +
                "od.activity_id, " +
                "od.activity_rule_id, " +
                "od.coupon_id, " +
                "pi.payment_type payment_type_code, " +
                "dic.dic_name payment_type_name, " +
                "pi.callback_time, " +
                "od.source_id, " +
                "od.source_type_id, " +
                "od.source_type_name, " +
                "od.sku_num, " +
                "od.split_activity_amount, " +
                "od.split_coupon_amount, " +
                "od.split_total_amount split_payment_amount " +
                "from payment_info pi " +
                "join dwd_trade_order_detail od " +
                "on pi.order_id = od.order_id " +
                "join `base_dic` for system_time as of pi.pt as dic " +
                "on pi.payment_type = dic.dic_code");
        tableEnv.createTemporaryView("result_table", resultTable);
        //TODO: 7. 创建支付事实表DDL
        tableEnv.executeSql("create table dwd_trade_pay_detail( " +
                "    order_detail_id string, " +
                "    order_id string, " +
                "    user_id string, " +
                "    sku_id string, " +
                "    sku_name string, " +
                "    province_id string, " +
                "    activity_id string, " +
                "    activity_rule_id string, " +
                "    coupon_id string, " +
                "    payment_type_code string, " +
                "    payment_type_name string, " +
                "    callback_time string, " +
                "    source_id string, " +
                "    source_type_id string, " +
                "    source_type_name string, " +
                "    sku_num string, " +
                "    split_activity_amount string, " +
                "    split_coupon_amount string, " +
                "    split_payment_amount string " +
                ")" + KafkaUtil.getKafkaSinkDDL("dwd_trade_pay_detail"));
        //TODO: 8. 支付数据写入
        tableEnv.executeSql("insert into dwd_trade_pay_detail select * from result_table").print();
        //TODO: 9. 执行程序
        env.execute("dwdtradepaydetail");
    }
}
