package com.gmall.realtime.app.dwd.db;

import com.gmall.realtime.utils.KafkaUtil;
import com.gmall.realtime.utils.MysqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

public class DwdTradeRefundDetail {
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
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));
        //TODO 2. 建立look_up字典信息表&topic_db数据
        tableEnv.executeSql(KafkaUtil.getTopicDb("dwdtraderefunddetail"));
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());
        //TODO 3. 读取退款表数据，获取退款成功数据
        Table refund_payment = tableEnv.sqlQuery("select  " +
                "    `data`['id'] id, " +
                "    `data`['out_trade_no'] out_trade_no, " +
                "    `data`['order_id'] order_id, " +
                "    `data`['sku_id'] sku_id, " +
                "    `data`['payment_type'] payment_type, " +
                "    `data`['trade_no'] trade_no, " +
                "    `data`['total_amount'] total_amount, " +
                "    `data`['subject'] subject, " +
                "    `data`['refund_status'] refund_status, " +
                "    `data`['create_time'] create_time, " +
                "    `data`['callback_time'] callback_time, " +
                "    `data`['callback_content'] callback_content, " +
                "    `pt` pt " +
                "from topic_db " +
                "where `database` = 'gmall' " +
                "and `table` = 'refund_payment' " +
                "and `type` = 'update' " +
                "and `data`['refund_status'] = '0705' " +
                "and `old`['refund_status'] is not null ");
        tableEnv.createTemporaryView("refund_payment", refund_payment);
        tableEnv.toAppendStream(refund_payment, Row.class).print("refund_payment");
        //TODO 4. 读取订单表，过滤出退款成功订单拿到user_id、province_id
        Table order_info = tableEnv.sqlQuery("select " +
                "    `data`['id'] id, " +
                "    `data`['user_id'] user_id, " +
                "    `data`['province_id'] province_id " +
                "from topic_db " +
                "where `database` = 'gmall' " +
                "and `table` = 'order_info' " +
                "and `type` = 'update' " +
                "and `data`['order_status'] = '1006' " +
                "and `old`['order_status'] is not null");
        tableEnv.createTemporaryView("order_info",order_info);
        tableEnv.toAppendStream(order_info, Row.class).print("order_info");
        //TODO 5. 读取退单明细表，筛选退单成功的明细数据获取refund_num
        Table order_refund_info = tableEnv.sqlQuery("select  " +
                "  `data`['order_id'] order_id, " +
                "  `data`['refund_num'] refund_num " +
                "from topic_db " +
                "where `database` = 'gmall' " +
                "and `table` = 'order_refund_info' " +
                "and `type` = 'update' " +
                "and `data`['refund_status'] = '0705' " +
                "and `old`['refund_status'] is not null");
        tableEnv.createTemporaryView("order_refund_info",order_refund_info);
        tableEnv.toAppendStream(order_refund_info, Row.class).print("order_refund_info");
        //TODO 6. 四张表Join
        Table resultTable = tableEnv.sqlQuery("select " +
                "    rp.id, " +
                "    rp.out_trade_no, " +
                "    rp.order_id, " +
                "    rp.sku_id, " +
                "    rp.payment_type, " +
                "    dic.dic_code payment_name, " +
                "    rp.trade_no, " +
                "    rp.total_amount, " +
                "    rp.subject, " +
                "    rp.refund_status, " +
                "    rp.create_time, " +
                "    rp.callback_time, " +
                "    rp.callback_content, " +
                "    oi.user_id, " +
                "    oi.province_id, " +
                "    ori.refund_num " +
                "from refund_payment rp " +
                "join order_info oi " +
                "on rp.order_id = oi.id " +
                "join order_refund_info ori " +
                "on rp.order_id = ori.order_id " +
                "join base_dic FOR SYSTEM_TIME AS OF rp.pt as dic " +
                "on rp.payment_type = dic.dic_code");
        tableEnv.createTemporaryView("resultTable",resultTable);
        tableEnv.toAppendStream(resultTable, Row.class).print("resultTable");
        //TODO 7. 创建退款成功事实表
        tableEnv.executeSql("create table dwd_trade_refund_detail( " +
                "    `id` string, " +
                "    `out_trade_no` string, " +
                "    `order_id` string, " +
                "    `sku_id` string, " +
                "    `payment_type` string, " +
                "    `payment_name` string, " +
                "    `trade_no` string, " +
                "    `total_amount` string, " +
                "    `subject` string, " +
                "    `refund_status` string, " +
                "    `create_time` string, " +
                "    `callback_time` string, " +
                "    `callback_content` string, " +
                "    `user_id` string, " +
                "    `province_id` string, " +
                "    `refund_num` string " +
                ")" + KafkaUtil.getKafkaSinkDDL("dwd_trade_refund_detail"));
        //TODO 8. 写入数据
        tableEnv.executeSql("insert into dwd_trade_refund_detail select * from resultTable");
        //TODO 9. 执行程序
        env.execute("dwdtraderefunddetail");
    }
}
