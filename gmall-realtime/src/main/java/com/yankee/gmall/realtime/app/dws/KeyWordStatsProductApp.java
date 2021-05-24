package com.yankee.gmall.realtime.app.dws;

import com.yankee.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description 商品行为关键词宽表
 * @date 2021/5/21 14:11
 */
public class KeyWordStatsProductApp {
    // 常量配置
    private static final String DWD_PAGE_LOG_TOPIC = "dwd_page_log";
    private static final String DWM_ORDER_WIDE_TOPIC = "dwm_order_wide";
    private static final String DWD_CART_INFO_TOPIC = "dwd_cart_info";

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        // 1.创建执行环境
        // 1.1 流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);
        // 设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://supercluster/gmall/flink/dws_keywordproduct/checkpoint"));
        // 开启checkpoint
        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
        // 设置checkpoint超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        // 设置checkpoint重启策略
        env.setRestartStrategy(RestartStrategies.noRestart());
        // 1.2 表环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 2.读取kafka数据并创建动态表
        tableEnv.executeSql("CREATE TABLE PAGE_VIEW (" +
                "common MAP<STRING, STRING>, " +
                "page MAP<STRING, STRING>, " +
                "ts BIGINT, " +
                "rowtime AS TO_TIMESTAMP(FROM_UNIXTIME(ts / 1000, 'yyyy-MM-dd HH:mm:ss')), " +
                "WATERMARK FOR rowtime AS rowtime - INTERVAL '2' SECOND) " +
                "WITH (" + MyKafkaUtil.getKafkaDDL(DWD_PAGE_LOG_TOPIC, "dws_keywordproduct"));
        tableEnv.executeSql("CREATE TABLE ORDER_VIEW (" +
                "sku_id BIGINT, " +
                "spu_id BIGINT, " +
                "order_id BIGINT, " +
                "ts BIGINT, " +
                "rowtime AS TO_TIMESTAMP(FROM_UNIXTIME(ts / 1000, 'yyyy-MM-dd HH:mm:ss')), " +
                "WATERMARK FOR rowtime AS rowtime - INTERVAL '2' SECOND) " +
                "WITH (" + MyKafkaUtil.getKafkaDDL(DWM_ORDER_WIDE_TOPIC, "dws_keywordproduct"));
        tableEnv.executeSql("CREATE TABLE CART_VIEW (" +
                "sku_id BIGINT, " +
                "create_time STRING, " +
                "rowtime AS TO_TIMESTAMP(create_time), " +
                "WATERMARK FOR rowtime AS rowtime - INTERVAL '2' SECOND) " +
                "WITH (" + MyKafkaUtil.getKafkaDDL(DWD_CART_INFO_TOPIC, "dws_keywordproduct"));
        // 从hbase中加载SPU信息维表
        // tableEnv.executeSql("CREATE TABLE SPU_INFO (" +
        //         "id BIGINT, " +
        //         "spu_name STRING, " +
        //         "PRIMARY KEY (id) NOT ENFORCED) " +
        //         "WITH ( " +
        //         "'connector' = 'jdbc', " +
        //         "'url' = '" + GmallConfig.PHOENIX_SERVER + "', " +
        //         "'table-name' = 'GMALL_REALTIME.DIM_SPU_INFO')");
        tableEnv.executeSql("CREATE TABLE SPU_VIEW (" +
                "rowkey INT, " +
                // "`0` ROW<spu_name STRING, description STRING, category3_id STRING, tm_id STRING>, " +
                // "baseinfo ROW<name STRING, sex STRING>, " +
                "`0` ROW<`\\x00\\x00\\x00\\x00` STRING, `\\x80\\x0B` STRING, `\\x80\\x0C` STRING>, " +
                "PRIMARY KEY (rowkey) NOT ENFORCED) " +
                "WITH ( " +
                "'connector' = 'hbase-2.2', " +
                // "'table-name' = 'GMALL_REALTIME:DIM_SPU_INFO', " +
                // "'table-name' = 'student', " +
                "'table-name' = 'phoenix_hbase', " +
                "'zookeeper.quorum' = 'hadoop01:2181,hadoop02:2181,hadoop03:2181')");

        // tableEnv.executeSql("select rowkey,`0`.spu_name from SPU_VIEW").print();
        Table table = tableEnv.sqlQuery("select rowkey, `0`.`\\x00\\x00\\x00\\x00` from SPU_VIEW");
        tableEnv.toAppendStream(table, Row.class).print(">>>");

        // 3.加工获取点击次数、订单次数以及加入购物车次数
        // 注册函数
        // tableEnv.createTemporarySystemFunction("ik_analyze", KeyWordUDTF.class);
        // tableEnv.createTemporarySystemFunction("keywordProduct", KeyWordProductUDTF.class);

        // Table reduceTable = tableEnv.sqlQuery("SELECT " +
        //         "DATE_FORMAT(TUMBLE_START(pv.rowtime, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') AS stt, " +
        //         "DATE_FORMAT(TUMBLE_END(pv.rowtime, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') AS edt, " +
        //         "source, " +
        //         "word, " +
        //         "word_count, " +
        //         "UNIX_TIMESTAMP() * 1000 AS ts " +
        //         "FROM PAGE_VIEW AS pv, " +
        //         "LATERAL TABLE(ik_analyze(spu_name)) AS T(word), " +
        //         "LATERAL TALBE(keywordProduct(click_count, cart_count, order_count)) AS T2(word_count, source)");

        // 4.转换为数据流
        // DataStream<KeyWordStats> keyWordStatsProductDataStream = tableEnv.toAppendStream(reduceTable, KeyWordStats.class);

        // 打印测试
        // keyWordStatsProductDataStream.print(">>>>>");

        // 5.写入ClickHouse
        // keyWordStatsProductDataStream.addSink(ClickHouseUtil.getSink("insert into key_word_stats_product values(?,?,?,?,?,?)"));

        // 提交任务
        env.execute();
    }
}
