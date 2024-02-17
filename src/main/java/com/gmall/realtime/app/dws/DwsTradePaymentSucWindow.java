package com.gmall.realtime.app.dws;

import com.alibaba.fastjson.*;
import com.gmall.realtime.bean.TradePaymentWindowBean;
import com.gmall.realtime.utils.ClickHouseUtil;
import com.gmall.realtime.utils.DateFormatUtil;
import com.gmall.realtime.utils.KafkaUtil;
import com.gmall.realtime.utils.TimestampLtz3CompareUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
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
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsTradePaymentSucWindow {
    public static void main(String[] args) throws Exception {
        //TODO 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.setStateBackend(new HashMapStateBackend());
        env.enableCheckpointing(10 * 60000L,CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10 * 60000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://192.168.141.100:9820/flink/ck");
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000L));
        System.setProperty("HADOOP_USER_NAME","root");
        //TODO 2. 获取dwd层支付成功数据
        String topic = "dwd_trade_pay_detail";
        String group_id = "dwstradepaymentsucwindow74033";
        DataStreamSource<String> paymentDs = env.fromSource(KafkaUtil.getKafkaSource(topic, group_id), WatermarkStrategy.noWatermarks(), "PaymentSUcCnt");
        //TODO 3. 将String类型转换成JSON类型，并且按照order_detail_id分组
        SingleOutputStreamOperator<JSONObject> JsonDs = paymentDs.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject valueJson = JSON.parseObject(value);
                    collector.collect(valueJson);
                } catch (Exception e) {
                    System.out.println(">>>>>");
                }
            }
        });
        KeyedStream<JSONObject, String> keybyOrderDetailIdDs = JsonDs.keyBy(json -> json.getString("order_detail_id"));
        //TODO 4. 使用状态编程进行去重(因为上游left join的存在需要对数据流进行去重，保留后来的数据流)
        SingleOutputStreamOperator<JSONObject> DistinctByOrderDetailIdDs = keybyOrderDetailIdDs.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
            private ValueState<JSONObject> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<>("JSONObjectValueState", JSONObject.class));

            }

            @Override
            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                JSONObject value = valueState.value();
                if (value == null) {
                    valueState.update(jsonObject);
                    context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime() + 5000L);
                } else {
                    String lastRt = valueState.value().getString("row_op_ts");
                    String currentRt = jsonObject.getString("row_op_ts");
                    int compare = TimestampLtz3CompareUtil.compare(lastRt, currentRt);
                    if (compare != 1) {
                        valueState.update(jsonObject);
                    }
                }
            }

            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
                JSONObject value = valueState.value();
                out.collect(value);
                valueState.clear();
            }
        });
        //TODO 5. 指定水位线
        SingleOutputStreamOperator<JSONObject> WaterMarkDs = DistinctByOrderDetailIdDs.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject jsonObject, long l) {
                String callbackTime = jsonObject.getString("callback_time");
                Long ts = DateFormatUtil.toTs(callbackTime,true);
                return ts;
            }
        }));
        //TODO 5. 按照uid分组
        KeyedStream<JSONObject, String> KeyByUserIdDs = WaterMarkDs.keyBy(json -> json.getString("user_id"));
        //TODO 6. 使用状态编程进行过滤
        SingleOutputStreamOperator<TradePaymentWindowBean> SingleUserDs = KeyByUserIdDs.flatMap(new RichFlatMapFunction<JSONObject, TradePaymentWindowBean>() {
            private ValueState<String> lastValueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastValueState = getRuntimeContext().getState(new ValueStateDescriptor<>("lastValueState", String.class));
            }

            @Override
            public void flatMap(JSONObject jsonObject, Collector<TradePaymentWindowBean> collector) throws Exception {
                String lastDt = lastValueState.value();
                String currentDt = jsonObject.getString("callback_time").split(" ")[0];
                Long paymentSucUniqueUserCount = 0L;
                Long paymentSucNewUserCount = 0L;
                if (lastDt == null) {
                    paymentSucUniqueUserCount = 1L;
                    paymentSucNewUserCount = 1L;
                    lastValueState.update(currentDt);
                } else {
                    if (!lastDt.equals(currentDt)) {
                        paymentSucUniqueUserCount = 1L;
                        lastValueState.update(currentDt);
                    }
                }
                if (paymentSucUniqueUserCount == 1L){
                    collector.collect(new TradePaymentWindowBean("", "", paymentSucUniqueUserCount, paymentSucNewUserCount, 0L));
                }
            }
        });
        //TODO 7. 开窗进行聚合（增量&全量结合方式进行计算）
        AllWindowedStream<TradePaymentWindowBean, TimeWindow> windowsAllDs = SingleUserDs.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)));
        SingleOutputStreamOperator<TradePaymentWindowBean> resultDs = windowsAllDs.reduce(new ReduceFunction<TradePaymentWindowBean>() {
            @Override
            public TradePaymentWindowBean reduce(TradePaymentWindowBean value1, TradePaymentWindowBean value2) throws Exception {
                value1.setPaymentSucNewUserCount(value1.getPaymentSucNewUserCount() + value2.getPaymentSucNewUserCount());
                value1.setPaymentSucUniqueUserCount(value1.getPaymentSucUniqueUserCount() + value2.getPaymentSucUniqueUserCount());
                return value1;
            }
        }, new AllWindowFunction<TradePaymentWindowBean, TradePaymentWindowBean, TimeWindow>() {
            @Override
            public void apply(TimeWindow timeWindow, Iterable<TradePaymentWindowBean> iterable, Collector<TradePaymentWindowBean> collector) throws Exception {
                TradePaymentWindowBean next = iterable.iterator().next();
                next.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                next.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));
                next.setTs(System.currentTimeMillis());
            }
        });
        //TODO 8. 将数据写入到clickhouse
        resultDs.addSink(ClickHouseUtil.getSinkFuction("insert into dws_trade_payment_suc_window values(?,?,?,?,?)"));
        //TODO 9. 执行
        env.execute("dwstradepaymentsucwindow");
    }
}
