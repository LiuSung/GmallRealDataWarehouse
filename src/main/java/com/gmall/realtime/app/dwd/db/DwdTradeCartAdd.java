package com.gmall.realtime.app.dwd.db;

import com.gmall.realtime.utils.KafkaUtil;
import com.gmall.realtime.utils.MysqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeCartAdd {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //TODO 2.使用DDL方式读取topic_db主题的数据创建表
        tableEnv.executeSql(KafkaUtil.getTopicDb("dwdtradecartadd"));
        //TODO 3.过滤出加购数据
        Table cartAddTable = tableEnv.sqlQuery("select  " +
                "  `data`['id'] id, " +
                "  `data`['user_id'] user_id, " +
                "  `data`['sku_id'] sku_id, " +
                "  `data`['cart_price'] cart_price, " +
                "  if(`type` = 'insert' ,`data`['sku_num'], cast((cast(`data`['sku_num'] as int) - cast(`old`['sku_num'] as int)) as string )) sku_num, " +
                "  `data`['sku_name'] sku_name, " +
                "  `data`['is_checked'] is_checked, " +
                "  `data`['create_time'] create_time, " +
                "  `data`['operate_time'] operate_time, " +
                "  `data`['is_ordered'] is_ordered, " +
                "  `data`['order_time'] order_time, " +
                "  `data`['source_type'] source_type, " +
                "  `data`['source_id'] source_id, " +
                "  `pt` pt " +
                "from topic_db " +
                "where `database` = 'gmall' " +
                "and `table` = 'cart_info' " +
                "and `type` = 'insert'  " +
                "or (`type` = 'update' " +
                "  and `old`['sku_num'] is not null " +
                "  and cast(`data`['sku_num'] as int) > cast(`old`['sku_num'] as int))");
        tableEnv.createTemporaryView("cart_info_table",cartAddTable);
        //TODO 4.读取MySQL的base_dic表作为lookup表
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());
        //TODO 5.关联两张表
        Table cartAddWithDicTable = tableEnv.sqlQuery("select " +
                "  ci.id, " +
                "  ci.user_id, " +
                "  ci.sku_id, " +
                "  ci.cart_price, " +
                "  ci.sku_num, " +
                "  ci.sku_name, " +
                "  ci.is_checked, " +
                "  ci.create_time, " +
                "  ci.operate_time, " +
                "  ci.is_ordered, " +
                "  ci.order_time, " +
                "  ci.source_type source_type_id, " +
                "  ci.source_id , " +
                "  dic.dic_name source_type_name " +
                "from cart_info_table ci " +
                "join base_dic FOR SYSTEM_TIME AS OF ci.pt as dic " +
                "on ci.source_type = dic.dic_code");
        tableEnv.createTemporaryView("cart_add_dic_table",cartAddWithDicTable);
        //TODO 6.使用DDL方式创建事实表
        tableEnv.executeSql("create table dwd_cart_add( " +
                "    id STRING, " +
                "    user_id STRING, " +
                "    sku_id STRING, " +
                "    cart_price STRING, " +
                "    sku_num STRING, " +
                "    sku_name STRING, " +
                "    is_checked STRING, " +
                "    create_time STRING, " +
                "    operate_time STRING, " +
                "    is_ordered STRING, " +
                "    order_time STRING, " +
                "    source_type STRING, " +
                "    source_type_id STRING, " +
                "    source_type_name STRING " +
                ")"+KafkaUtil.getKafkaSinkDDL("dwd_trade_cart_add"));
        //TODO 7.将数据写出
        tableEnv.executeSql("insert into dwd_cart_add select * from cart_add_dic_table").print();
        //TODO 8.启动任务
        env.execute("dwdtradecartadd");
    }
}
