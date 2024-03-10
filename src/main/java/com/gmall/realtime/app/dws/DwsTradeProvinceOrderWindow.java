package com.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gmall.realtime.bean.TradeProvinceOrderWindow;
import com.gmall.realtime.utils.*;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class DwsTradeProvinceOrderWindow {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
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
        String groupid = "trade_province_order_74033";
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
        //TODO 4.按照order_detail_id进行keyby(取最后一条数据)
        KeyedStream<JSONObject, String> keyByOrderDetail = jsonUnDrity.keyBy(json -> json.getString("id"));
        //TODO 4.去重
        SingleOutputStreamOperator<JSONObject> DistinctDs = keyByOrderDetail.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
            private ValueState<JSONObject> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<JSONObject> ValueStateDes = new ValueStateDescriptor<JSONObject>("ValueStateDes", JSONObject.class);
                valueState = getRuntimeContext().getState(ValueStateDes);
            }

            @Override
            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                JSONObject lastvalue = valueState.value();
                if (lastvalue == null) {
                    valueState.update(jsonObject);
                    context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime() + 5000L);
                } else {

                    //取出状态数据以及当前数据中的时间
                    String lastTs = lastvalue.getString("row_op_ts");
                    String currentTs = jsonObject.getString("row_op_ts");
                    if (TimestampLtz3CompareUtil.compare(lastTs, currentTs) != 1) {
                        valueState.update(jsonObject);
                    }
                }
            }

            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
                out.collect(valueState.value());
                valueState.clear();
            }
        });
        //TODO 5.将JSON转换成Bean对象
        SingleOutputStreamOperator<TradeProvinceOrderWindow> TradeProvinceOrderDs = DistinctDs.map(line -> {
            HashSet<String> orderset = new HashSet<>();
            orderset.add(line.getString("order_id"));
            return new TradeProvinceOrderWindow("", "",
                    line.getString("province_id"),
                    "",
                    0L,
                    orderset,
                    line.getDouble("split_total_amount"),
                    DateFormatUtil.toTs(line.getString("create_time"), true));
        });
        //TODO 6.设置水位线
        SingleOutputStreamOperator<TradeProvinceOrderWindow> tradeProWmDs = TradeProvinceOrderDs.assignTimestampsAndWatermarks(WatermarkStrategy.<TradeProvinceOrderWindow>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<TradeProvinceOrderWindow>() {
            @Override
            public long extractTimestamp(TradeProvinceOrderWindow tradeProvinceOrderWindow, long l) {
                return tradeProvinceOrderWindow.getTs();
            }
        }));
        //TODO 7.开窗聚合
        SingleOutputStreamOperator<TradeProvinceOrderWindow> reduceDs = tradeProWmDs.windowAll(TumblingEventTimeWindows.of(Time.seconds(10))).reduce(new ReduceFunction<TradeProvinceOrderWindow>() {
            @Override
            public TradeProvinceOrderWindow reduce(TradeProvinceOrderWindow value1, TradeProvinceOrderWindow value2) throws Exception {
                value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                value1.setOrderAmount(value1.getOrderAmount() + value2.getOrderAmount());
                return value1;
            }
        }, new AllWindowFunction<TradeProvinceOrderWindow, TradeProvinceOrderWindow, TimeWindow>() {
            @Override
            public void apply(TimeWindow timeWindow, Iterable<TradeProvinceOrderWindow> iterable, Collector<TradeProvinceOrderWindow> collector) throws Exception {
                TradeProvinceOrderWindow next = iterable.iterator().next();
                next.setTs(System.currentTimeMillis());
                next.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                next.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));
                next.setOrderCount((long) next.getOrderIdSet().size());
                collector.collect(next);
            }
        });
        SingleOutputStreamOperator<TradeProvinceOrderWindow> RedultDs = AsyncDataStream.unorderedWait(reduceDs, new DimAsyncFunction<TradeProvinceOrderWindow>("DIM_BASE_PROVINCE") {
            @Override
            public String getKey(TradeProvinceOrderWindow input) {
                return input.getProvinceId();
            }

            @Override
            public void join(TradeProvinceOrderWindow input, JSONObject diminfo) {
                input.setProvinceName(diminfo.getString("NAME"));
            }
        }, 100, TimeUnit.SECONDS);
        //TODO 8.写入Clikhouse
        RedultDs.addSink(ClickHouseUtil.getSinkFuction("insert into dws_trade_province_order_window values(?,?,?,?,?,?,?)"));
        //TODO 9.执行环境
        env.execute("dwstradeprovinceorderwindow");
    }
}