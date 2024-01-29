package com.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gmall.realtime.bean.TrafficPageViewBean;
import com.gmall.realtime.utils.ClickHouseUtil;
import com.gmall.realtime.utils.DateFormatUtil;
import com.gmall.realtime.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.Tuple4;

import java.time.Duration;

public class DwsTrafficVcChArIsNewPageViewWindow {
    public static void main(String[] args) throws Exception {
        //TODO 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.setStateBackend(new HashMapStateBackend());
//        env.enableCheckpointing(5 * 60000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setCheckpointTimeout(10 * 60000L);
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://192.168.141.100:9820/flink/ck");
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000L));
//        System.setProperty("HADOOP_USER_NAME","root");
        //TODO 2. 读取三个主题的数据创建流
        String uvTopic = "dwd_traffic_unique_visitor_detail";
        String ujTopic = "dwd_traffic_user_jump_detail";
        String pageTopic = "dwd_traffic_page_log";
        String groupid = "vc_charisnew_pageview";
        DataStreamSource<String> uvSource = env.fromSource(KafkaUtil.getKafkaSource(uvTopic, groupid), WatermarkStrategy.noWatermarks(), "uvSource");
        DataStreamSource<String> ujSource = env.fromSource(KafkaUtil.getKafkaSource(ujTopic, groupid), WatermarkStrategy.noWatermarks(), "ujSource");
        DataStreamSource<String> pageSource = env.fromSource(KafkaUtil.getKafkaSource(pageTopic, groupid), WatermarkStrategy.noWatermarks(), "PageSource");

        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewWithUv = uvSource.map(line -> {
            JSONObject uvJson = JSON.parseObject(line);
            JSONObject common = uvJson.getJSONObject("common");
            return new TrafficPageViewBean("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    1L, 0L, 0L, 0L, 0L,
                    common.getLong("ts"));
        });
        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewWithUj = ujSource.map(line -> {
            JSONObject ujJson = JSON.parseObject(line);
            JSONObject common = ujJson.getJSONObject("common");
            return new TrafficPageViewBean("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 0L, 0L, 0L, 1L,
                    common.getLong("ts"));
        });
        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewWithPage = pageSource.map(line -> {
            JSONObject pageJson = JSON.parseObject(line);
            JSONObject common = pageJson.getJSONObject("common");

            JSONObject page = pageJson.getJSONObject("page");
            String lastPageId = page.getString("last_page_id");
            Long svCt = 0L;
            if (lastPageId == null) {
                svCt = 1L;
            }
            return new TrafficPageViewBean("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, svCt, 1L, page.getLong("during_time"), 0L,
                    common.getLong("ts"));
        });
        //TODO 3. 统一数据格式进行union
        DataStream<TrafficPageViewBean> union = trafficPageViewWithUv.union(trafficPageViewWithUj).union(trafficPageViewWithPage);
        //TODO 4. 提取事件时间生成WarterMark
        WatermarkStrategy<TrafficPageViewBean> trafficPageViewBeanWatermarkStrategy = WatermarkStrategy.<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(14)).withTimestampAssigner(new SerializableTimestampAssigner<TrafficPageViewBean>() {
            @Override
            public long extractTimestamp(TrafficPageViewBean trafficPageViewBean, long l) {
                return trafficPageViewBean.getTs();
            }
        });
        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewDs = union.assignTimestampsAndWatermarks(trafficPageViewBeanWatermarkStrategy);
        //TODO 5. 分组开窗聚合
        KeyedStream<TrafficPageViewBean, Tuple4<String, String, String, String>> keyByDs = trafficPageViewDs.keyBy(new KeySelector<TrafficPageViewBean, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(TrafficPageViewBean value) throws Exception {
                return new Tuple4<>(value.getAr(), value.getVc(), value.getCh(), value.getIsNew());
            }
        });
        WindowedStream<TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow> windowDs = keyByDs.window(TumblingEventTimeWindows.of(Time.seconds(10L)));
        SingleOutputStreamOperator<TrafficPageViewBean> Resultline = windowDs.reduce(new ReduceFunction<TrafficPageViewBean>() {
            @Override
            public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
                value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                value1.setUjCt(value1.getUjCt() + value2.getUjCt());
                value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                return value1;
            }
        }, new WindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> key, TimeWindow timeWindow, Iterable<TrafficPageViewBean> input, Collector<TrafficPageViewBean> out) throws Exception {
                //获取数据
                TrafficPageViewBean next = input.iterator().next();
                //补充窗口时间
                next.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                next.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));
                //更新TS
                next.setTs(System.currentTimeMillis());
                //写出数据
                out.collect(next);
            }
        });
        //TODO 6. 讲数据写到clickhouse
        Resultline.addSink(ClickHouseUtil.getSinkFuction("insert into dws_traffic_vc_ch_ar_is_new_page_view_window values(?,?,?,?,?,?,?,?,?,?,?,?)"));
        //TODO 7. 启动任务
        env.execute("dwstrafficvccharisnewpageviewwindow");
    }
}
