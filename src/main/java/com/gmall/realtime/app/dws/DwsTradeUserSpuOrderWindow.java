package com.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gmall.realtime.bean.TradeUserSpuOrderBean;
import com.gmall.realtime.utils.ClickHouseUtil;
import com.gmall.realtime.utils.DateFormatUtil;
import com.gmall.realtime.utils.DimAsyncFunction;
import com.gmall.realtime.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.Tuple4;

import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class DwsTradeUserSpuOrderWindow {
    public static void main(String[] args) throws Exception {
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
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(org.apache.flink.api.common.time.Time.seconds(5))
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
        SingleOutputStreamOperator<TradeUserSpuOrderBean> tradeUserSpuWithkuDs = AsyncDataStream.unorderedWait(TradeUserSpuOrderBeadDs,
                new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(TradeUserSpuOrderBean input) {
                        return input.getSkuId();
                    }
                    @Override
                    public void join(TradeUserSpuOrderBean input, JSONObject diminfo) {
                        input.setSpuId(diminfo.getString("SPU_ID"));
                        input.setSpuId(diminfo.getString("TM_ID"));
                        input.setSpuId(diminfo.getString("CATEGORY3_ID"));
                    }
                },
                100,
                TimeUnit.SECONDS);
        //TODO 8. 提取事件事件生成Watermark
        SingleOutputStreamOperator<TradeUserSpuOrderBean> tradeUserSpuWithWmDs = tradeUserSpuWithkuDs.assignTimestampsAndWatermarks(WatermarkStrategy.<TradeUserSpuOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<TradeUserSpuOrderBean>() {
            @Override
            public long extractTimestamp(TradeUserSpuOrderBean tradeUserSpuOrderBean, long l) {
                return tradeUserSpuOrderBean.getTs();
            }
        }));
        //TODO 9. 分组开创聚合
        KeyedStream<TradeUserSpuOrderBean, Tuple4<String, String, String, String>> tradeUserSpuKeyByDs = tradeUserSpuWithWmDs.keyBy(new KeySelector<TradeUserSpuOrderBean, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(TradeUserSpuOrderBean value) throws Exception {
                return new Tuple4<>(value.getUserId(),
                        value.getSpuId(),
                        value.getTrademarkId(),
                        value.getCategory3Id()
                );
            }
        });
        SingleOutputStreamOperator<TradeUserSpuOrderBean> resultDs = tradeUserSpuKeyByDs.window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<TradeUserSpuOrderBean>() {
                    @Override
                    public TradeUserSpuOrderBean reduce(TradeUserSpuOrderBean value1, TradeUserSpuOrderBean value2) throws Exception {
                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        value1.setOrderAmount(value1.getOrderAmount() + value2.getOrderAmount());
                        return value1;
                    }
                }, new WindowFunction<TradeUserSpuOrderBean, TradeUserSpuOrderBean, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow timeWindow, Iterable<TradeUserSpuOrderBean> iterable, Collector<TradeUserSpuOrderBean> collector) throws Exception {
                        TradeUserSpuOrderBean next = iterable.iterator().next();
                        next.setTs(System.currentTimeMillis());
                        next.setOrderCount((long) next.getOrderIdSet().size());
                        next.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                        next.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));
                        collector.collect(next);
                    }
                });
        //TODO 10. 关联sup,tm,category维表补充相应的信息
        //10.1关联sup_name
        SingleOutputStreamOperator<TradeUserSpuOrderBean> redultWithSpu = AsyncDataStream.unorderedWait(resultDs, new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_SUP_INFO") {
            @Override
            public String getKey(TradeUserSpuOrderBean input) {
                return input.getSpuId();
            }

            @Override
            public void join(TradeUserSpuOrderBean input, JSONObject diminfo) {
                input.setSpuName(diminfo.getString("SPU_NAME"));
            }
        }, 100, TimeUnit.SECONDS);
        //10.2 关联tm_name
        SingleOutputStreamOperator<TradeUserSpuOrderBean> resultWithSpuTMDs = AsyncDataStream.unorderedWait(redultWithSpu, new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_BASE_TRADEMARK") {
            @Override
            public String getKey(TradeUserSpuOrderBean input) {
                return input.getTrademarkId();
            }

            @Override
            public void join(TradeUserSpuOrderBean input, JSONObject diminfo) {
                input.setTrademarkName(diminfo.getString("TM_NAME"));
            }
        }, 100, TimeUnit.SECONDS);
        //10.3关联category3_name
        SingleOutputStreamOperator<TradeUserSpuOrderBean> resultWithSpuTMC3Ds = AsyncDataStream.unorderedWait(resultWithSpuTMDs, new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_BASE_CATEGORY3") {
            @Override
            public String getKey(TradeUserSpuOrderBean input) {
                return input.getCategory3Id();
            }

            @Override
            public void join(TradeUserSpuOrderBean input, JSONObject diminfo) {
                input.setCategory3Name(diminfo.getString("NAME"));
                input.setCategory2Id(diminfo.getString("CATEGORY2_ID"));
            }
        }, 100, TimeUnit.SECONDS);
        //10.4关联category2_name
        SingleOutputStreamOperator<TradeUserSpuOrderBean> resultWithSpuTMC3C2Ds = AsyncDataStream.unorderedWait(resultWithSpuTMC3Ds, new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_BASE_CATEGORY2") {
            @Override
            public String getKey(TradeUserSpuOrderBean input) {
                return input.getCategory2Id();
            }

            @Override
            public void join(TradeUserSpuOrderBean input, JSONObject diminfo) {
                input.setCategory2Name(diminfo.getString("NAME"));
                input.setCategory1Id(diminfo.getString("CATEGORY1_ID"));
            }
        }, 100, TimeUnit.SECONDS);
        //10.5关联category1_name
        SingleOutputStreamOperator<TradeUserSpuOrderBean> resultWithAllDs = AsyncDataStream.unorderedWait(resultWithSpuTMC3C2Ds, new DimAsyncFunction<TradeUserSpuOrderBean>() {
            @Override
            public String getKey(TradeUserSpuOrderBean input) {
                return input.getCategory1Id();
            }

            @Override
            public void join(TradeUserSpuOrderBean input, JSONObject diminfo) {
                input.setCategory1Name("NAME");
            }
        }, 100, TimeUnit.SECONDS);

        //TODO 11. 将数据写到ClickHouse
        resultWithAllDs.addSink(ClickHouseUtil.getSinkFuction("insert into dws_trade_user_spu_order_window values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));
        //TODO 12. 启动
        env.execute("dwstradeuserspuorderwindow");
    }
}
