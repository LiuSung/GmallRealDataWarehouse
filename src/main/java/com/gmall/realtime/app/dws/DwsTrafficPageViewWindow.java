package com.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gmall.realtime.bean.TrafficHomeDetailPageViewBean;
import com.gmall.realtime.utils.ClickHouseUtil;
import com.gmall.realtime.utils.DateFormatUtil;
import com.gmall.realtime.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
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

public class DwsTrafficPageViewWindow {
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
        //TODO 2. 获取页面数据并指定水位线
        String topic = "dwd_traffic_page_log";
        String groupId = "dws_traffic_page_view_window";
        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<String>() {
            @Override
            public long extractTimestamp(String s, long l) {
                return JSON.parseObject(s).getLong("ts");
            }
        });
        DataStreamSource<String> pageDs = env.fromSource(KafkaUtil.getKafkaSource(topic, groupId), watermarkStrategy, "page_logSource");
        //TODO 3. 过滤数据(首页与商品详情页)并将数据转换成Json
        SingleOutputStreamOperator<JSONObject> JsonPageDs = pageDs.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                String page_id = jsonObject.getJSONObject("page").getString("page_id");
                if ("home".equals(page_id) || "good_detail".equals(page_id)) {
                    out.collect(jsonObject);
                }
            }
        });
        //TODO 4. 以mid做keyby进行数据去重，使用状态编程
        KeyedStream<JSONObject, String> keyByDs = JsonPageDs.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> SingleDs = keyByDs.flatMap(new RichFlatMapFunction<JSONObject, TrafficHomeDetailPageViewBean>() {
            //定义两个值状态
            private ValueState<String> homeLastState;
            private ValueState<String> goodDetailState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //设置状态TTL以及更新规则为写更新
                StateTtlConfig StateTtlconfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                //定义状态描述器
                ValueStateDescriptor<String> homeStateDes = new ValueStateDescriptor<>("home-state", String.class);
                homeStateDes.enableTimeToLive(StateTtlconfig);
                ValueStateDescriptor<String> goodDetailStateDes = new ValueStateDescriptor<>("goodDetail-state", String.class);
                goodDetailStateDes.enableTimeToLive(StateTtlconfig);
                homeLastState = getRuntimeContext().getState(homeStateDes);
                goodDetailState = getRuntimeContext().getState(goodDetailStateDes);
            }

            @Override
            public void flatMap(JSONObject value, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                String homelast = homeLastState.value();
                String goodDetail = goodDetailState.value();
                Long ts = value.getLong("ts");
                String date = DateFormatUtil.toDate(ts);
                Long homeUvCt = 0L;
                Long detailUvCt = 0L;
                String page_id = value.getJSONObject("page").getString("page_id");
                if ("home".equals(page_id)) {
                    if (homelast == null || !date.equals(homelast)) {
                        homeUvCt = 1L;
                        homeLastState.update(date);
                    }
                } else if ("good_detail".equals(page_id)) {
                    if (goodDetail == null || !date.equals(goodDetail)) {
                        detailUvCt = 1L;
                        goodDetailState.update(date);
                    }
                }
                if (homeUvCt != 0L || detailUvCt != 0L) {
                    out.collect(new TrafficHomeDetailPageViewBean("", "", homeUvCt, detailUvCt, 0L));
                }
            }
        });
        //TODO 5. 全窗口开窗聚合
        AllWindowedStream<TrafficHomeDetailPageViewBean, TimeWindow> windowAllDs = SingleDs.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> redultDs = windowAllDs.reduce(new ReduceFunction<TrafficHomeDetailPageViewBean>() {
            @Override
            public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1, TrafficHomeDetailPageViewBean value2) throws Exception {
                value1.setHomeUvCt(value1.getHomeUvCt() + value2.getHomeUvCt());
                value1.setGoodDetailUvCt(value1.getGoodDetailUvCt() + value2.getGoodDetailUvCt());
                return value1;
            }
        }, new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
            @Override
            public void apply(TimeWindow timeWindow, Iterable<TrafficHomeDetailPageViewBean> iterable, Collector<TrafficHomeDetailPageViewBean> collector) throws Exception {
                TrafficHomeDetailPageViewBean next = iterable.iterator().next();
                next.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                next.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));
                next.setTs(System.currentTimeMillis());
                collector.collect(next);
            }
        });
        redultDs.print();
        //TODO 6. 将数据写入到clickhouse
        redultDs.addSink(ClickHouseUtil.getSinkFuction("insert into dws_traffic_page_view_window values(?,?,?,?,?)"));
        //TODO 7. 执行环境
        env.execute("dwstrafficpageviewwindow");
    }
}
