package com.gmall.realtime.app.dws;


import com.alibaba.fastjson.JSON;
import com.gmall.realtime.bean.CartAddUuBean;
import com.gmall.realtime.utils.ClickHouseUtil;
import com.gmall.realtime.utils.DateFormatUtil;
import com.gmall.realtime.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 求每日加购物车的独立用户数
 */
public class DwsTradeCartAddUuWindow {
    public static void main(String[] args) throws Exception {
        //TODO 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
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
        //TODO 2. 读取dwd_trade_cart_add主题数据设置水位线
        String topic = "dwd_trade_cart_add";
        String groupId = "dws_trade_cart_add_uu_window";
        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<String>() {
            @Override
            public long extractTimestamp(String s, long l) {
                String operateTime = JSON.parseObject(s).getString("operate_time");
                String createTime = JSON.parseObject(s).getString("create_time");
                if (operateTime != null) {
                    return DateFormatUtil.toTs(operateTime,true);
                } else {
                    return DateFormatUtil.toTs(createTime,true);
                }
            }
        });
        DataStreamSource<String> cartAddDs = env.fromSource(KafkaUtil.getKafkaSource(topic, groupId), watermarkStrategy, "cartAdd_log_74033");
        //TODO 3. 按照uid进行keyby
        KeyedStream<String, String> keybyDs = cartAddDs.keyBy(line -> JSON.parseObject(line).getString("user_id"));
        //TODO 4. 使用状态编程进行去重操作
        SingleOutputStreamOperator<CartAddUuBean> CartAddBeanDs = keybyDs.flatMap(new RichFlatMapFunction<String, CartAddUuBean>() {
            private ValueState<String> cartAddlast;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> cartAddlastDes = new ValueStateDescriptor<String>("cartAddlast", String.class);
                cartAddlastDes.enableTimeToLive(new StateTtlConfig.Builder(Time.days(1)).setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).build());
                cartAddlast = getRuntimeContext().getState(cartAddlastDes);
            }

            @Override
            public void flatMap(String s, Collector<CartAddUuBean> collector) throws Exception {
                Long cartAddUuCt = 0L;
                String operateTime = JSON.parseObject(s).getString("operate_time");
                String createTime = JSON.parseObject(s).getString("create_time");
                if (operateTime != null) {
                    Long ts = DateFormatUtil.toTs(operateTime,true);
                    if (cartAddlast.value() == null || !cartAddlast.value().equals(DateFormatUtil.toDate(ts))) {
                        cartAddUuCt = 1L;
                        cartAddlast.update(DateFormatUtil.toDate(ts));
                    }
                } else {
                    Long ts = DateFormatUtil.toTs(createTime,true);
                    if (cartAddlast.value() == null || !cartAddlast.value().equals(DateFormatUtil.toDate(ts))) {
                        cartAddUuCt = 1L;
                        cartAddlast.update(DateFormatUtil.toDate(ts));
                    }
                }
                if (cartAddUuCt != 0L) {
                    collector.collect(new CartAddUuBean("", "", cartAddUuCt, 0L));
                }
            }
        });
        //TODO 5. 开窗聚合
        AllWindowedStream<CartAddUuBean, TimeWindow> windowAllDs = CartAddBeanDs.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));
        SingleOutputStreamOperator<CartAddUuBean> resultDs = windowAllDs.reduce(new ReduceFunction<CartAddUuBean>() {
            @Override
            public CartAddUuBean reduce(CartAddUuBean value1, CartAddUuBean value2) throws Exception {
                value1.setCartAddUuCt(value1.getCartAddUuCt() + value2.getCartAddUuCt());
                return value1;
            }
        }, new AllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>() {
            @Override
            public void apply(TimeWindow timeWindow, Iterable<CartAddUuBean> iterable, Collector<CartAddUuBean> collector) throws Exception {
                CartAddUuBean next = iterable.iterator().next();
                next.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                next.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));
                next.setTs(System.currentTimeMillis());
                collector.collect(next);
            }
        });
        //TODO 6. 写入clickhouse
        resultDs.addSink(ClickHouseUtil.getSinkFuction("insert into dws_trade_cart_add_uu_window values(?,?,?,?)"));
        //TODO 7. 执行环境
        env.execute("dwstradecartadduuwindow");
    }
}
