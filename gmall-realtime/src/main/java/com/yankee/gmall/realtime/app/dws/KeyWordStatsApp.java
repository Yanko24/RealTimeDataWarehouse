package com.yankee.gmall.realtime.app.dws;

import com.yankee.gmall.realtime.app.fun.KeyWordUDTF;
import com.yankee.gmall.realtime.bean.KeyWordStats;
import com.yankee.gmall.realtime.common.GmallConstant;
import com.yankee.gmall.realtime.utils.ClickHouseUtil;
import com.yankee.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description 关键词主题宽表
 * @date 2021/5/21 10:27
 */
public class KeyWordStatsApp {
    // 常量配置
    private static final String DWD_PAGE_LOG_TOPIC = "dwd_page_log";

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        // 1.创建执行环境
        // 1.1 流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);
        // 设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://supercluster/gmall/flink/dws_keyword/checkpoint"));
        // 开启checkpoint
        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
        // checkpoint超时设置
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        // checkpoint重启策略
        env.setRestartStrategy(RestartStrategies.noRestart());
        // 1.2 表环境
        // 表环境配置
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 1.2 读取kafka数据创建动态表
        tableEnv.executeSql("CREATE TABLE PAGE_VIEW (" +
                "common MAP<STRING, STRING>, " +
                "page MAP<STRING, STRING>, " +
                "ts BIGINT, " +
                "rowtime AS TO_TIMESTAMP(FROM_UNIXTIME(ts / 1000, 'yyyy-MM-dd HH:mm:ss')), " +
                "WATERMARK FOR rowtime AS rowtime - INTERVAL '2' SECOND) " +
                "WITH (" + MyKafkaUtil.getKafkaDDL(DWD_PAGE_LOG_TOPIC, "dws_keyword"));

        // 1.3 过滤数据，只需要搜索的数据，搜索的关键词不能为空
        Table filterTable = tableEnv.sqlQuery("SELECT " +
                "page['item'] fullWord," +
                "rowtime " +
                "FROM PAGE_VIEW " +
                "WHERE page['item_type'] = 'keyword' AND page['item'] IS NOT NULL");

        // tableEnv.createTemporaryView("filterTable", filterTable);

        // 1.4 使用UDTF进行切词处理
        tableEnv.createTemporarySystemFunction("ik_analyze", KeyWordUDTF.class);
        Table wordTable = tableEnv.sqlQuery("SELECT " +
                "word, " +
                "rowtime " +
                "FROM " + filterTable + ", LATERAL TABLE(ik_analyze(fullWord)) AS T(word)");

        // 1.5 分组、开窗、聚合
        tableEnv.createTemporaryView("wordTable", wordTable);
        Table reduceTable = tableEnv.sqlQuery("SELECT " +
                "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '2' SECOND), 'yyyy-MM-dd HH:mm:ss') AS stt, " +
                "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '2' SECOND), 'yyyy-MM-dd HH:mm:ss') AS edt, " +
                "'" + GmallConstant.KEYWORD_SEARCH + "' AS source, " +
                "word, " +
                "count(*) word_count, " +
                "UNIX_TIMESTAMP() * 1000 AS ts " +
                "FROM wordTable " +
                "GROUP BY word, TUMBLE(rowtime, INTERVAL '2' SECOND)");

        // 转换成流
        DataStream<KeyWordStats> rowDataStream = tableEnv.toAppendStream(reduceTable, KeyWordStats.class);
        // 打印测试
        rowDataStream.print(">>>>>");

        // 1.6 将数据写入ClickHouse
        rowDataStream.addSink(ClickHouseUtil.getSink("insert into key_word_stats values(?,?,?,?,?,?)"));

        // 1.7 执行
        env.execute();
    }
}
