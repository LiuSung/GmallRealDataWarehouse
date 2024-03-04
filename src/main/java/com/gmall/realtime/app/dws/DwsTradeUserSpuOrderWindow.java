package com.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gmall.realtime.bean.TradeUserSpuOrderBean;
import com.gmall.realtime.utils.DateFormatUtil;
import com.gmall.realtime.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.HashSet;

public class DwsTradeUserSpuOrderWindow {
    public static void main(String[] args) {
        //TODO 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        env.setStateBackend(new HashMapStateBackend());
//        env.enableCheckpointing(10 * 60000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setCheckpointTimeout(10 * 60000L);
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://192.168.141.100:9820/flink/ck");
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000L));
//        System.setProperty("HADOOP_USER_NAME","root");
        //TODO 2. 读取Kafka DWD层 下单主题数据
        String topic = "dwd_trade_order_detail";
        String groupid = "trade_user_spu_order_74033";
        DataStreamSource<String> orderDetailDs = env.fromSource(KafkaUtil.getKafkaSource(topic, groupid), WatermarkStrategy.noWatermarks(), "dwstradeorderwindow");
        //TODO 3. 转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonUnDrity = orderDetailDs.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    System.out.println("false data: " + s);
                }
            }
        });
        //TODO 4.按照order_detail_id进行keyby
        KeyedStream<JSONObject, String> keyByOrderDetail = jsonUnDrity.keyBy(json -> json.getString("id"));
        //TODO 5. 去重
        SingleOutputStreamOperator<JSONObject> filterDs = keyByOrderDetail.filter(new RichFilterFunction<JSONObject>() {
            private ValueState<Integer> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<Integer>("is-exist", Integer.class);
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.seconds(5))
                        .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                        .build();
                stateDescriptor.enableTimeToLive(ttlConfig);
                valueState = getRuntimeContext().getState(stateDescriptor);

            }

            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                if (valueState.value() == null) {
                    valueState.update(1);
                    return true;
                } else {
                    return false;
                }
            }
        });
        //TODO 6. 将数据转换为JjavaBean对象
        SingleOutputStreamOperator<TradeUserSpuOrderBean> TradeUserSpuOrderBeadDs = filterDs.map(json -> {
            HashSet<String> orderIds = new HashSet<>();
            orderIds.add(json.getString("order_id"));
            return TradeUserSpuOrderBean.builder()
                    .skuId(json.getString("sku_id"))
                    .userId(json.getString("user_id"))
                    .orderAmount(json.getDouble("split_total_amount"))
                    .orderIdSet(orderIds)
                    .ts(DateFormatUtil.toTs(json.getString("create_time"), true))
                    .build();
        });
        //TODO 7. 关联sku_info维表，补充spu_id,tm_id，category3_id
        //TODO 8. 提取事件事件生成Watermark
        //TODO 9. 分组开创聚合
        //TODO 10. 关联sup,tm,category维表补充相应的信息
        //TODO 11. 将数据写到ClickHouse
        //TODO 12. 启动
    }
}
