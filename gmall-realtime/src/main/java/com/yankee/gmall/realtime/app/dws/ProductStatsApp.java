package com.yankee.gmall.realtime.app.dws;

import com.yankee.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description 商品主题宽表加工
 * @date 2021/5/17 17:14
 */
public class ProductStatsApp {
    // 常量配置信息
    private static final String DWD_PAGE_LOG_TOPIC = "dwd_page_log";
    private static final String DWM_ORDER_WIDE_TOPIC = "dwm_order_wide";
    private static final String DWM_PAYMENT_WIDE_TOPIC = "dwm_payment_wide";
    private static final String DWD_CART_INFO_TOPIC = "dwd_cart_info";
    private static final String DWD_FAVOR_INFO_TOPIC = "dwd_favor_info";
    private static final String DWD_ORDER_REFUND_INFO_TOPIC = "dwd_order_refund_info";
    private static final String DWD_COMMENT_INFO_TOPIC = "dwd_comment_info";

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);
        // 设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://supercluster/gmall/flink/dws_product/checkpoint"));
        // 开启checkpoint
        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        // 设置重启策略
        env.setRestartStrategy(RestartStrategies.noRestart());

        // 2.读取kafka主题的数据 7个主题
        DataStreamSource<String> pageLogDS = env.addSource(MyKafkaUtil.getKafkaSource(DWD_PAGE_LOG_TOPIC,
                "dws_product"));
        DataStreamSource<String> favorInfoDS = env.addSource(MyKafkaUtil.getKafkaSource(DWD_FAVOR_INFO_TOPIC,
                "dws_product"));
        DataStreamSource<String> cartInfoDS = env.addSource(MyKafkaUtil.getKafkaSource(DWD_CART_INFO_TOPIC,
                "dws_product"));
        DataStreamSource<String> orderWideDS = env.addSource(MyKafkaUtil.getKafkaSource(DWM_ORDER_WIDE_TOPIC,
                "dws_product"));
        DataStreamSource<String> paymentWideDS = env.addSource(MyKafkaUtil.getKafkaSource(DWM_PAYMENT_WIDE_TOPIC,
                "dws_product"));
        DataStreamSource<String> refundInfoDS = env.addSource(MyKafkaUtil.getKafkaSource(DWD_ORDER_REFUND_INFO_TOPIC,
                "dws_product"));
        DataStreamSource<String> commentInfoDS = env.addSource(MyKafkaUtil.getKafkaSource(DWD_COMMENT_INFO_TOPIC,
                "dws_product"));

        // 3.将7个流转换为统一格式

        // 4.Union

        // 5.提取时间戳生成watermark

        // 6.分组，开窗，聚合

        // 7.关联维度信息

        // 8.写入clickhouse

        // 9.执行任务
        env.execute();
    }
}
