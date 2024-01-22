package com.gmall.realtime.app.dws;

import com.gmall.realtime.app.func.SplitFunction;
import com.gmall.realtime.bean.KeywordBean;
import com.gmall.realtime.utils.KafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwsTrafficSourceKeywordPageViewWindow {
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
        //TODO 2. 使用DDL方式读取kafka page_log 主题数据创建表并提取时间生成WaterMark
        tableEnv.executeSql("create table page_log(" +
                "    `page` MAP<STRING,STRING>," +
                "    `ts` BIGINT," +
                "    `rt` as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000))," +
                "    WATERMARK FOR rt AS rt - INTERVAL '2' SECOND" +
                ") "+ KafkaUtil.getKafkaDDL("dwd_traffic_page_log","dwstrafficsourcekeywordpageviewwindow"));
        //TODO 3. 过滤出搜索数据
        Table filterTable = tableEnv.sqlQuery("select " +
                "    page['item'] item," +
                "    rt " +
                "from page_log " +
                "where page['last_page_id'] = 'search' " +
                "and page['item_type'] = 'keyword' " +
                "and page['item'] is not null");
        tableEnv.createTemporaryView("filter_table",filterTable);
        //TODO 4. 注册UDTF&切词
        tableEnv.createTemporarySystemFunction("SplitFunction", SplitFunction.class);
        Table splitTable = tableEnv.sqlQuery("select " +
                "    word, " +
                "    rt " +
                "from filter_table, " +
                "LATERAL TABLE(SplitFunction(item))");
        tableEnv.createTemporaryView("splitTable",splitTable);
        //TODO 5. 分组、开窗、聚合
        Table countTable = tableEnv.sqlQuery("select " +
                "    DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt, " +
                "    DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt, " +
                "    'search' source, "+
                "    word keyword, " +
                "    count(*) keyword_count, " +
                "    UNIX_TIMESTAMP()*1000 ts " +
                "from splitTable " +
                "group by word,TUMBLE(rt, INTERVAL '10' SECOND)");
        //TODO 6. 将动态表转换为流
        DataStream<KeywordBean> KeywordBeanAppendStream = tableEnv.toAppendStream(countTable, KeywordBean.class);
        //TODO 7. 将数据写出到ClikHouse
        KeywordBeanAppendStream.addSink()
        //TODO 8. 启动任务
        env.execute("dwstrafficsourcekeywordpageviewwindow");
    }
}
