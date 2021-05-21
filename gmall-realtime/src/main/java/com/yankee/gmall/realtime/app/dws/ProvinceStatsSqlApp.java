package com.yankee.gmall.realtime.app.dws;

import com.yankee.gmall.realtime.bean.ProvinceStats;
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
 * @description 地区主题宽表
 * @date 2021/5/20 15:51
 */
public class ProvinceStatsSqlApp {
    // 常量配置
    private static final String DWM_ORDER_WIDE_TOPIC = "dwm_order_wide";

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        // 1.获取执行环境（流、表）
        // 1.1 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并发
        env.setParallelism(1);
        // 设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://supercluster/gmall/flink/dws_province/checkpoint"));

        // 1.2 开启checkpoint
        // 开启checkpoint
        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
        // checkpint设置
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        // 设置重启策略
        env.setRestartStrategy(RestartStrategies.noRestart());

        // 1.3 获取表执行环境
        // 表执行环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 2.读取kafka数据创建动态表
        tableEnv.executeSql("CREATE TABLE ORDER_WIDE (" +
                "province_id BIGINT, " +
                "province_name STRING, " +
                "province_area_code STRING, " +
                "province_iso_code STRING, " +
                "province_3166_2_code STRING, " +
                "order_id STRING, " +
                "split_total_amount DOUBLE, " +
                "create_time STRING, " +
                "rowtime AS TO_TIMESTAMP(create_time), " +
                "WATERMARK FOR rowtime as rowtime) " +
                "WITH (" + MyKafkaUtil.getKafkaDDL(DWM_ORDER_WIDE_TOPIC, "dws_province"));

        // 3.分组、开窗、聚合
        Table reduceTable = tableEnv.sqlQuery("SELECT " +
                "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') as stt," +
                "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') as edt," +
                "province_id," +
                "province_name," +
                "province_area_code," +
                "province_iso_code," +
                "province_3166_2_code," +
                "sum(split_total_amount) order_amount," +
                "count(*) order_count," +
                "UNIX_TIMESTAMP() * 1000 ts " +
                "FROM ORDER_WIDE " +
                "GROUP BY province_id,province_name,province_area_code,province_iso_code,province_3166_2_code,TUMBLE" +
                "(rowtime, INTERVAL '10' SECOND)");

        // 4.将动态表转换成追加流
        DataStream<ProvinceStats> rowDataStream = tableEnv.toAppendStream(reduceTable, ProvinceStats.class);

        // 打印测试
        rowDataStream.print(">>>>>>");

        // 5.写入ClickHouse
        rowDataStream.addSink(ClickHouseUtil.getSink("insert into province_stats values(?,?,?,?,?,?,?,?,?,?)"));

        // 6.执行任务
        env.execute();
    }
}
