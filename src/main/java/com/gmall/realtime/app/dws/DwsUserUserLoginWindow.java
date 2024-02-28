package com.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gmall.realtime.bean.UserLoginBean;
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

public class DwsUserUserLoginWindow {
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
        //TODO 2. 读取Kafka page_log数据定义水位线
        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<String>() {
            @Override
            public long extractTimestamp(String s, long l) {
                return JSON.parseObject(s).getLong("ts");
            }
        });
        String topic = "dwd_traffic_page_log";
        String groupId = "dws_user_user_login_window";
        DataStreamSource<String> pageDs = env.fromSource(KafkaUtil.getKafkaSource(topic, groupId), watermarkStrategy, "page_log_source_userlogin");
        //TODO 3. 按照uid分组
        KeyedStream<String, String> keyByDs = pageDs.keyBy(line -> JSON.parseObject(line).getJSONObject("common").getString("uid"));
        //TODO 4. 使用键控值状态登陆数据以及回流数据
        SingleOutputStreamOperator<UserLoginBean> userLoginBeanDs = keyByDs.flatMap(new RichFlatMapFunction<String, UserLoginBean>() {
            private ValueState<String> userLastLoginState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor userLastLoginStateDes = new ValueStateDescriptor<>("userLastLoginStateDes", String.class);
                userLastLoginStateDes.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).build());
                userLastLoginState = getRuntimeContext().getState(userLastLoginStateDes);
            }

            @Override
            public void flatMap(String s, Collector<UserLoginBean> collector) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                Long uuCt = 0L;
                Long backCt = 0L;
                String date = DateFormatUtil.toDate(jsonObject.getLong("ts"));
                //过滤登陆数据，登陆分为自动登陆和手动登陆，前者的uid!=null && last_page_id==null，后者last_page_id=login
                if ((jsonObject.getJSONObject("common").getString("uid") != null && jsonObject.getJSONObject("page").getString("last_page_id") == null)
                        || jsonObject.getJSONObject("page").getString("last_page_id").equals("login")) {
                    if (userLastLoginState.value() == null) {
                        uuCt = 1L;
                        userLastLoginState.update(date);
                    } else {
                        if (!date.equals(userLastLoginState.value())) {
                            uuCt = 1L;
                            Long StateTs = DateFormatUtil.toTs(userLastLoginState.value());
                            Long ts = jsonObject.getLong("ts");
                            Long cutDays = (ts - StateTs) / 1000 / 3600 / 24;
                            if (cutDays > 8) {
                                backCt = 1L;
                            }
                            userLastLoginState.update(date);
                        }
                    }
                }
                collector.collect(new UserLoginBean("", "", backCt, uuCt, 0L));
            }
        });
        //TODO 5. 全窗口开窗
        AllWindowedStream<UserLoginBean, TimeWindow> winAllDs = userLoginBeanDs.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));
        //TODO 6. reduce中增量&全量结合方式进行聚合
        SingleOutputStreamOperator<UserLoginBean> resultDs = winAllDs.reduce(new ReduceFunction<UserLoginBean>() {
            @Override
            public UserLoginBean reduce(UserLoginBean value1, UserLoginBean value2) throws Exception {
                value1.setUuCt(value1.getUuCt() + value2.getUuCt());
                value1.setBackCt(value1.getBackCt() + value2.getBackCt());
                return value1;
            }
        }, new AllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
            @Override
            public void apply(TimeWindow timeWindow, Iterable<UserLoginBean> iterable, Collector<UserLoginBean> collector) throws Exception {
                UserLoginBean next = iterable.iterator().next();
                next.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                next.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));
                next.setTs(System.currentTimeMillis());
                collector.collect(next);
            }
        });
        //TODO 7. 数据写入clickhouse
        resultDs.addSink(ClickHouseUtil.getSinkFuction("insert into dws_user_user_login_window values(?,?,?,?,?)"));
        //TODO 8. 启动任务
        env.execute("dwsuseruserloginwindow");
    }
}
