package com.gmall.realtime.app.dwd.db;

import com.gmall.realtime.utils.KafkaUtil;
import com.gmall.realtime.utils.MysqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

public class DwdTradeOrderRefund {
    public static void main(String[] args) throws Exception {
        //TODO 1. 获取执行环境
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
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));
        //TODO 2. 创建kafka topic_db表，过滤出退单信息表
        tableEnv.executeSql(KafkaUtil.getTopicDb("dwdtradeorderrefund"));
        Table orderRefundTable = tableEnv.sqlQuery("select  " +
                "    `data`['id'] id, " +
                "    `data`['user_id'] user_id, " +
                "    `data`['order_id'] order_id, " +
                "    `data`['sku_id'] sku_id, " +
                "    `data`['refund_type'] refund_type, " +
                "    `data`['refund_num'] refund_num, " +
                "    `data`['refund_amount'] refund_amount, " +
                "    `data`['refund_reason_type'] refund_reason_type, " +
                "    `data`['refund_reason_txt'] refund_reason_txt, " +
                "    `data`['refund_status'] refund_status, " +
                "    `data`['create_time'] create_time, " +
                "    `pt` pt " +
                "from topic_db " +
                "where `database` = 'gmall' " +
                "and `table` = 'order_refund_info' " +
                "and `type` = 'insert'");
        tableEnv.createTemporaryView("order_refund_info",orderRefundTable);
        //TODO 3. 读取订单表信息，筛选退单数据
        Table orderInfoRefund = tableEnv.sqlQuery("select " +
                "data['id'] id, " +
                "data['province_id'] province_id, " +
                "`old` " +
                "from topic_db " +
                "where `table` = 'order_info' " +
                "and `type` = 'update' " +
                "and data['order_status']='1005' " +
                "and `old`['order_status'] is not null");
        tableEnv.createTemporaryView("order_info_refund",orderInfoRefund);
        //TODO 4. 建议Mysql-LookUp字典表
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());
        //TODO 5. 三表关联获得订单宽表
        Table resultTable = tableEnv.sqlQuery("select  " +
                "    ori.id, " +
                "    ori.user_id, " +
                "    ori.order_id, " +
                "    ori.sku_id, " +
                "    oir.province_id, " +
                "    ori.refund_type, " +
                "    type_dic.dic_name, " +
                "    ori.refund_num, " +
                "    ori.refund_amount, " +
                "    ori.refund_reason_type, " +
                "    reason_dic.dic_name, " +
                "    ori.refund_reason_txt, " +
                "    ori.refund_status, " +
                "    ori.create_time " +
                "from order_refund_info ori " +
                "join order_info_refund oir " +
                "on ori.order_id = oir.id " +
                "join base_dic for system_time as of ori.pt as type_dic " +
                "on ori.refund_type = type_dic.dic_code " +
                "join base_dic for system_time as of ori.pt as reason_dic " +
                "on ori.refund_reason_type = reason_dic.dic_code");
        tableEnv.createTemporaryView("result_table", resultTable);
        //TODO 6. 创建订单事实表DDL
        tableEnv.executeSql("create table dwd_trade_order_refund( " +
                "    id string, " +
                "    user_id string, " +
                "    order_id string, " +
                "    sku_id string, " +
                "    province_id string, " +
                "    refund_type string, " +
                "    type_dic_name string, " +
                "    refund_num string, " +
                "    refund_amount string, " +
                "    refund_reason_type string, " +
                "    reason_dic_name string, " +
                "    refund_reason_txt string, " +
                "    refund_status string, " +
                "    create_time string " +
                ")" + KafkaUtil.getKafkaSinkDDL("dwd_trade_order_refund"));
        //TODO 7. 写入数据
        tableEnv.executeSql("insert into dwd_trade_order_refund select * from result_table").print();
        //TODO 8. 执行程序
        env.execute("dwdtradeorderrefund");
    }
}
