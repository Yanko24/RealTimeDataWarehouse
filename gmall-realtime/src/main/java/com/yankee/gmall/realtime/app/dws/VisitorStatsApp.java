package com.yankee.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yankee.gmall.realtime.bean.VisitorStats;
import com.yankee.gmall.realtime.utils.DateTimeUtil;
import com.yankee.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;

/**
 * 数据流：Mock --> Nginx --> Logger --> kafka(ods_base_log) --> LogBaseApp --> kafka(dwd_page_log dwd_start_log
 * dwd_display_log) --> DayUVApp,UserJumpDetailApp --> kafka(dwm_unique_visit dwm_user_jump_detail) --> VisitorStatsApp
 */
public class VisitorStatsApp {
    // 常量配置信息
    // pv，进入页面数，访问时长
    private static final String DWD_PAGE_LOG_TOPIC = "dwd_page_log";
    // uv
    private static final String DWM_UNIQUE_VISIT = "dwm_unique_visit";
    // 跳出数
    private static final String DWM_USER_JUMP_DETAIL = "dwm_user_jump_detail";

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);
        // 设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://supercluster/gmall/flink/dws_log/checkpoint"));
        // 设置开启checkpoint
        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        // 设置重启策略
        env.setRestartStrategy(RestartStrategies.noRestart());

        // 2.读取kafka主题的数据
        DataStreamSource<String> pageLogDS = env.addSource(MyKafkaUtil.getKafkaSource(DWD_PAGE_LOG_TOPIC,
                "dws_log"));
        DataStreamSource<String> uniqueVisitDS = env.addSource(MyKafkaUtil.getKafkaSource(DWM_UNIQUE_VISIT,
                "dws_log"));
        DataStreamSource<String> userJumpDetailDS = env.addSource(MyKafkaUtil.getKafkaSource(DWM_USER_JUMP_DETAIL,
                "dws_log"));

        // 测试打印
        // pageLogDS.print("Page>>>>>");
        // uniqueVisitDS.print("UniqueVisit>>>>>");
        // userJumpDetailDS.print("UserJumpDetail>>>>>");

        // 3.格式化流数据，使其字段统一（JavaBean）
        // 3.1 将页面数据流格式化为VisitorStats，主要字段是PV和during_time
        SingleOutputStreamOperator<VisitorStats> pvAndDtDS = pageLogDS.map(jsonStr -> {
            // 将数据转换为JSON对象
            JSONObject jsonObject = JSON.parseObject(jsonStr);
            JSONObject commonObj = jsonObject.getJSONObject("common");

            return new VisitorStats("", "", commonObj.getString("vc"), commonObj.getString("ch"),
                    commonObj.getString("ar"), commonObj.getString("is_new"), 0L, 1L, 0L, 0L,
                    jsonObject.getJSONObject("page").getLong("during_time"), jsonObject.getLong("ts"));
        });

        // 3.2 将页面数据流先过滤后格式化为VisitorStats，主要字段是sv_ct
        SingleOutputStreamOperator<VisitorStats> svCountDS = pageLogDS.process(new ProcessFunction<String, VisitorStats>() {
            @Override
            public void processElement(String value, Context context, Collector<VisitorStats> collector) throws Exception {
                // 将数据转换为JSON对象
                JSONObject jsonObject = JSON.parseObject(value);

                // 获取上一跳页面数据
                String lastPage = jsonObject.getJSONObject("page").getString("last_page_id");

                if (lastPage == null || lastPage.length() <= 0) {
                    JSONObject commonObj = jsonObject.getJSONObject("common");
                    collector.collect(new VisitorStats("", "", commonObj.getString("vc"), commonObj.getString("ch"),
                            commonObj.getString("ar"), commonObj.getString("is_new"), 0L, 0L, 1L, 0L, 0L,
                            jsonObject.getLong("ts")));
                }

            }
        });

        // 3.3 将uniqueVisitDS格式化为VisitorStats，主要字段是uv
        SingleOutputStreamOperator<VisitorStats> uniqueVisitCountDS = uniqueVisitDS.map(jsonStr -> {
            // 将数据转换为JSON对象
            JSONObject jsonObject = JSON.parseObject(jsonStr);
            JSONObject commonObj = jsonObject.getJSONObject("common");

            return new VisitorStats("", "", commonObj.getString("vc"), commonObj.getString("ch"),
                    commonObj.getString("ar"), commonObj.getString("is_new"), 1L, 0L, 0L, 0L, 0L, jsonObject.getLong(
                    "ts"));
        });

        // 3.4 将userJumpDetailDS格式化为VisitorStats对象，主要字段为uj_ct
        SingleOutputStreamOperator<VisitorStats> userJumpDetailCountDS = userJumpDetailDS.map(jsonStr -> {
            // 将数据转换为JSON对象
            JSONObject jsonObject = JSON.parseObject(jsonStr);
            JSONObject commonObj = jsonObject.getJSONObject("common");

            return new VisitorStats("", "", commonObj.getString("vc"), commonObj.getString("ch"),
                    commonObj.getString("ar"), commonObj.getString("is_new"), 0L, 0L, 0L, 1L, 0L, jsonObject.getLong(
                    "ts"));
        });

        // 4.将多个流进行union
        DataStream<VisitorStats> unionDS = pvAndDtDS.union(svCountDS, uniqueVisitCountDS, userJumpDetailCountDS);

        // 5.分组，聚合计算
        SingleOutputStreamOperator<VisitorStats> visitorStatsSingleOutputStreamOperator =
                unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(1)).withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                    @Override
                    public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));

        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedStream =
                visitorStatsSingleOutputStreamOperator.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4<String, String, String, String> getKey(VisitorStats value) throws Exception {
                        return Tuple4.of(value.getVc(), value.getCh(), value.getAr(), value.getIs_new());
                    }
                });

        // 开窗
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowedStream =
                keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        // 聚合操作
        SingleOutputStreamOperator<VisitorStats> result = windowedStream.reduce(new ReduceFunction<VisitorStats>() {
            @Override
            public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
                return new VisitorStats("", "", value1.getVc(), value1.getCh(), value1.getAr(), value1.getIs_new(),
                        value1.getUv_ct() + value2.getUv_ct(), value1.getPv_ct() + value2.getPv_ct(),
                        value1.getSv_ct() + value2.getSv_ct(), value1.getUj_ct() + value2.getUj_ct(),
                        value1.getDur_sum() + value2.getDur_sum(), System.currentTimeMillis());
            }
        }, new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4,
                              TimeWindow window, Iterable<VisitorStats> input, Collector<VisitorStats> out) throws Exception {
                // 取出数据
                VisitorStats visitorStats = input.iterator().next();

                // 取出窗口的开始及结束时间
                long start = window.getStart();
                long end = window.getEnd();

                String startTime = DateTimeUtil.toYmdHms(new Date(start));
                String endTime = DateTimeUtil.toYmdHms(new Date(end));

                // 设置时间数据
                visitorStats.setStt(startTime);
                visitorStats.setEdt(endTime);

                // 将数据写出
                out.collect(visitorStats);
            }
        });

        // 打印测试
        result.print("result>>>>>");

        // 不开窗打印
        // keyedStream.reduce(new ReduceFunction<VisitorStats>() {
        //     @Override
        //     public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
        //         return new VisitorStats("", "", value1.getVc(), value1.getCh(), value1.getAr(), value1.getIs_new(),
        //                 value1.getUv_ct() + value2.getUv_ct(), value1.getPv_ct() + value2.getPv_ct(),
        //                 value1.getSv_ct() + value2.getSv_ct(), value1.getUj_ct() + value2.getUj_ct(),
        //                 value1.getDur_sum() + value2.getDur_sum(), System.currentTimeMillis());
        //     }
        // }).print(">>>>>>>>>>");

        // 6.将聚合之后的数据写入ClickHouse

        // 7.执行任务
        env.execute();
    }
}
