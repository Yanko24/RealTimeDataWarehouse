package com.yankee.gmall.realtime.app.dws;

import com.yankee.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

public class VisitorStatsApp {
    // 常量配置信息
    // pv，进入页面数，访问时长
    private static final String DWD_PAGE_LOG_TOPIC = "dwd_page_log";
    // uv
    private static final String DWM_UNIQUE_VISIT = "dwm_unique_visit";
    // 跳出数
    private static final String DWM_USER_JUMP_DETAIL = "dwm_user_jump_detail";

    public static void main(String[] args) throws Exception {
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
        FlinkKafkaConsumer<String> pageLogKafkaDS = MyKafkaUtil.getKafkaSource(DWD_PAGE_LOG_TOPIC, "dws_log");
        FlinkKafkaConsumer<String> uniqueVisitKafkaDS = MyKafkaUtil.getKafkaSource(DWM_UNIQUE_VISIT, "dws_log");
        FlinkKafkaConsumer<String> userJumpDetailKafkaDS = MyKafkaUtil.getKafkaSource(DWM_USER_JUMP_DETAIL, "dws_log");

        // 3.格式化流数据，使其字段统一（JavaBean）

        // 4.将多个流进行union

        // 5.分组，聚合计算

        // 6.将聚合之后的数据写入ClickHouse

        // 7.执行任务
        env.execute();
    }
}
