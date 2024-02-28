package com.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gmall.realtime.bean.TradeOrderBean;
import com.gmall.realtime.utils.ClickHouseUtil;
import com.gmall.realtime.utils.DateFormatUtil;
import com.gmall.realtime.utils.KafkaUtil;
import com.gmall.realtime.utils.TimestampLtz3CompareUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsTradeOrderWindow {
    public static void main(String[] args) throws Exception {
        //TODO 1.环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.setStateBackend(new HashMapStateBackend());
//        env.enableCheckpointing(5*60000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setCheckpointTimeout(10*60000L);
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://192.168.141.100:9820/flink/ck");
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000L));
//        System.setProperty("HADOOP_USER_NAME","root");
        //TODO 2.读取DWD层下单主题数据
        String topic = "dwd_trade_order_detail";
        String groupid = "trade_order_detail_74033";
        DataStreamSource<String> orderDetailDs = env.fromSource(KafkaUtil.getKafkaSource(topic, groupid), WatermarkStrategy.noWatermarks(), "dwstradeorderwindow");
        //TODO 3.脏数据去除
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
        //TODO 5.根据order_detail_id进行去重
        SingleOutputStreamOperator<JSONObject> DistinctOrderDeatiliDDs = keyByOrderDetail.process(new ProcessFunction<JSONObject, JSONObject>() {
            private ValueState<JSONObject> lastOrderDetailValue;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastOrderDetailValue = getRuntimeContext().getState(new ValueStateDescriptor<>("lastOrderDetailValue", JSONObject.class));
            }

            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                if (lastOrderDetailValue.value() == null) {
                    lastOrderDetailValue.update(jsonObject);
                    context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime() + 5000L);
                } else {
                    String lastRt = lastOrderDetailValue.value().getString("row_op_ts");
                    String currentrt = jsonObject.getString("row_op_ts");
                    int compare = TimestampLtz3CompareUtil.compare(lastRt, currentrt);
                    if (compare != 1) {
                        lastOrderDetailValue.update(jsonObject);
                    }
                }
            }

            @Override
            public void onTimer(long timestamp, ProcessFunction<JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
                JSONObject value = lastOrderDetailValue.value();
                out.collect(value);
                lastOrderDetailValue.clear();
            }
        });
        //TODO 6.设置水位线
        SingleOutputStreamOperator<JSONObject> WarterMarkDs = DistinctOrderDeatiliDDs.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject jsonObject, long l) {
                return DateFormatUtil.toTs(jsonObject.getString("create_time"), true);
            }
        }));
        //TODO 7.按照用户id进行keyby
        KeyedStream<JSONObject, String> keyByUserId = WarterMarkDs.keyBy(json -> json.getString("user_id"));
        //TODO 8.将JSON封装到JavaBean对象
        SingleOutputStreamOperator<TradeOrderBean> TradeOrderDs = keyByUserId.flatMap(new RichFlatMapFunction<JSONObject, TradeOrderBean>() {
            private ValueState<String> lastDate;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> lastDateDesc = new ValueStateDescriptor<String>("lastDateDesc", String.class);
                lastDate = getRuntimeContext().getState(lastDateDesc);
            }

            @Override
            public void flatMap(JSONObject jsonObject, Collector<TradeOrderBean> collector) throws Exception {
                long orderUniqueUserCount = 0L;
                long orderNewUserCount = 0L;
                String currDate = jsonObject.getString("create_time").split(" ")[0];
                if (lastDate == null) {
                    orderUniqueUserCount = 1L;
                    orderNewUserCount = 1L;
                    lastDate.update(currDate);
                } else {
                    if (lastDate.value().equals(currDate)) {
                        orderUniqueUserCount = 1L;
                    }
                }
                Integer skuNum = jsonObject.getInteger("sku_num");
                Double orderPrice = jsonObject.getDouble("order_price");
                TradeOrderBean tradeOrderBean = new TradeOrderBean("", "", orderUniqueUserCount, orderNewUserCount,
                        jsonObject.getDouble("split_activity_amount"),
                        jsonObject.getDouble("split_coupon_amount"),
                        skuNum * orderPrice, null
                );
                collector.collect(tradeOrderBean);
            }
        });
        //TODO 9.开窗聚合
        AllWindowedStream<TradeOrderBean, TimeWindow> winAllDs = TradeOrderDs.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)));
        SingleOutputStreamOperator<TradeOrderBean> resultDs = winAllDs.reduce(new ReduceFunction<TradeOrderBean>() {
            @Override
            public TradeOrderBean reduce(TradeOrderBean value1, TradeOrderBean value2) throws Exception {
                value1.setOrderUniqueUserCount(value1.getOrderUniqueUserCount() + value2.getOrderUniqueUserCount());
                value1.setOrderNewUserCount(value1.getOrderNewUserCount() + value2.getOrderNewUserCount());
                value1.setOrderActivityReduceAmount(value1.getOrderActivityReduceAmount() + value2.getOrderActivityReduceAmount());
                value1.setOrderCouponReduceAmount(value1.getOrderCouponReduceAmount() + value2.getOrderCouponReduceAmount());
                value1.setOrderOriginalTotalAmount(value1.getOrderOriginalTotalAmount() + value2.getOrderOriginalTotalAmount());
                return value1;
            }
        }, new AllWindowFunction<TradeOrderBean, TradeOrderBean, TimeWindow>() {
            @Override
            public void apply(TimeWindow timeWindow, Iterable<TradeOrderBean> iterable, Collector<TradeOrderBean> collector) throws Exception {
                TradeOrderBean next = iterable.iterator().next();
                next.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                next.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));
                next.setTs(System.currentTimeMillis());
            }
        });
        //TODO 10.写到Clickhouse
        resultDs.addSink(ClickHouseUtil.getSinkFuction("insert into insert into dws_trade_order_window values(?,?,?,?,?,?,?,?)"));
        //TODO 11.执行环境
        env.execute("dwstradeorderwindow");
    }
}
