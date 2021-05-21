package com.yankee.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.yankee.gmall.realtime.app.fun.DimAsyncFunction;
import com.yankee.gmall.realtime.bean.OrderWide;
import com.yankee.gmall.realtime.bean.PaymentWide;
import com.yankee.gmall.realtime.bean.ProductStats;
import com.yankee.gmall.realtime.common.GmallConstant;
import com.yankee.gmall.realtime.utils.ClickHouseUtil;
import com.yankee.gmall.realtime.utils.DateTimeUtil;
import com.yankee.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description 商品主题宽表
 * @date 2021/5/17 17:14
 *
 * 数据流：mockLog -> Nginx -> Logger -> Kafka(ods_base_log) -> FlinkApp(LogBaseApp) -> Kafka(dwd_page_log)
 * 数据流：mockDB -> MySQL -> MaxWell -> Kafka(ods_base_db_m) -> FlinkApp(DBBaseApp) -> Kafka(HBase) -> FlinkApp
 * (OrderWideApp, PaymentWideApp,Redis,Phoenix) -> Kafka(dwm_order_wide,dwm_payment_wide) -> FlinkApp(ProductStatsApp)
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
        // 3.1 处理点击和曝光数据
        SingleOutputStreamOperator<ProductStats> clickAndDisplayDS = pageLogDS.process(new ProcessFunction<String, ProductStats>() {
            @Override
            public void processElement(String pageLog, Context context, Collector<ProductStats> collector) throws Exception {
                // 转换为JSON对象
                JSONObject jsonObject = JSON.parseObject(pageLog);
                // 获取page
                JSONObject pageObj = jsonObject.getJSONObject("page");
                String pageId = pageObj.getString("page_id");

                // 获取时间戳字段
                Long ts = jsonObject.getLong("ts");

                // 判断是good_detail页面，则为点击数据
                if ("good_detail".equals(pageId)) {
                    ProductStats productStats = ProductStats
                            .builder()
                            .sku_id(pageObj.getLong("item"))
                            .click_ct(1L)
                            .ts(ts)
                            .build();
                    collector.collect(productStats);
                }

                // 取出曝光数据
                JSONArray displays = jsonObject.getJSONArray("displays");
                if (displays != null && displays.size() > 0) {
                    for (int i = 0; i < displays.size(); i++) {
                        JSONObject displayObj = displays.getJSONObject(i);

                        if ("sku_id".equals(jsonObject.getString("item_type"))) {
                            ProductStats productStats = ProductStats
                                    .builder()
                                    .sku_id(displayObj.getLong("item"))
                                    .display_ct(1L)
                                    .ts(ts)
                                    .build();
                            collector.collect(productStats);
                        }
                    }
                }
            }
        });

        // 打印测试
        // clickAndDisplayDS.print("clickAndDisplayDS>>>>>");

        // 3.2 处理收藏数据
        SingleOutputStreamOperator<ProductStats> favorDS = favorInfoDS.map(json -> {
            JSONObject jsonObject = JSON.parseObject(json);
            Long ts = DateTimeUtil.toTs(jsonObject.getString("create_time"));
            return ProductStats.builder().sku_id(jsonObject.getLong("sku_id")).favor_ct(1L).ts(ts).build();
        });

        // 打印测试
        // favorDS.print("favorDS>>>>>");

        // 3.3 处理购物车数据
        SingleOutputStreamOperator<ProductStats> cartDS = cartInfoDS.map(json -> {
            JSONObject jsonObject = JSON.parseObject(json);
            Long ts = DateTimeUtil.toTs(jsonObject.getString("create_time"));
            return ProductStats.builder().sku_id(jsonObject.getLong("sku_id")).cart_ct(1L).ts(ts).build();
        });

        // 打印测试
        // cartDS.print("cartDS>>>>>");

        // 3.4 处理下单数据
        SingleOutputStreamOperator<ProductStats> orderDS = orderWideDS.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String value) throws Exception {
                // 转换为OrderWide对象
                OrderWide orderWide = JSON.parseObject(value, OrderWide.class);

                // 获取ts
                Long ts = DateTimeUtil.toTs(orderWide.getCreate_time());

                // HashSet<Long> hashSet = new HashSet<>();
                // hashSet.add(orderWide.getOrder_id());

                return ProductStats
                        .builder()
                        .sku_id(orderWide.getSku_id())
                        .order_sku_num(orderWide.getSku_num())
                        .order_amount(orderWide.getSplit_total_amount())
                        .orderIdSet(new HashSet<>(Collections.singleton(orderWide.getOrder_id())))
                        .ts(ts)
                        .build();
            }
        });

        // 打印测试
        // orderDS.print("orderDS>>>>>");

        // 3.5 处理支付数据
        SingleOutputStreamOperator<ProductStats> paymentDS = paymentWideDS.map(json -> {
            PaymentWide paymentWide = JSON.parseObject(json, PaymentWide.class);
            Long ts = DateTimeUtil.toTs(paymentWide.getPayment_create_time());
            return ProductStats.builder()
                    .sku_id(paymentWide.getSku_id())
                    .payment_amount(paymentWide.getSplit_total_amount())
                    .paidOrderIdSet(new HashSet<>(Collections.singleton(paymentWide.getOrder_id())))
                    .ts(ts)
                    .build();
        });

        // 打印测试
        // paymentDS.print("paymentDS>>>>>");

        // 3.6 处理退款数据
        SingleOutputStreamOperator<ProductStats> refundDS = refundInfoDS.map(json -> {
            JSONObject jsonObject = JSON.parseObject(json);
            Long ts = DateTimeUtil.toTs(jsonObject.getString("create_time"));
            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .refund_amount(jsonObject.getBigDecimal("refund_amount"))
                    .refundOrderIdSet(new HashSet<>(Collections.singleton(jsonObject.getLong("order_id"))))
                    .ts(ts)
                    .build();
        });

        // 打印测试
        // refundDS.print("refundDS>>>>>");

        // 3.7 处理评价数据
        SingleOutputStreamOperator<ProductStats> commentDS = commentInfoDS.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String value) throws Exception {
                // 将数据转换为json对象
                JSONObject jsonObject = JSON.parseObject(value);

                Long ts = DateTimeUtil.toTs(jsonObject.getString("create_time"));

                Long goodCommentCt = 0L;
                if (GmallConstant.APPRAISE_GOOD.equals(jsonObject.getString("appraise"))) {
                    goodCommentCt = 1L;
                }

                return ProductStats.builder().sku_id(jsonObject.getLong("sku_id")).comment_ct(1L).good_comment_ct(goodCommentCt).ts(ts).build();
            }
        });

        // 打印测试
        // commentDS.print("commentDS>>>>>");

        // 4.Union
        DataStream<ProductStats> unionDS = clickAndDisplayDS.union(favorDS, cartDS, orderDS, paymentDS, refundDS, commentDS);

        // 打印测试
        // unionDS.print("unionDS>>>>>>");

        // 5.提取时间戳生成watermark
        SingleOutputStreamOperator<ProductStats> productStatsWithWatermarkDS =
                unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.<ProductStats>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<ProductStats>() {
            @Override
            public long extractTimestamp(ProductStats element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        // 打印测试
        // productStatsWithWatermarkDS.print("productStatsWithWatermarkDS>>>>");

        // 6.分组，开窗，聚合
        SingleOutputStreamOperator<ProductStats> reduceDS = productStatsWithWatermarkDS.keyBy(ProductStats::getSku_id)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<ProductStats>() {
                    @Override
                    public ProductStats reduce(ProductStats value1, ProductStats value2) throws Exception {
                        value1.setDisplay_ct(value1.getDisplay_ct() + value2.getDisplay_ct());
                        value1.setClick_ct(value1.getClick_ct() + value2.getClick_ct());
                        value1.setCart_ct(value1.getCart_ct() + value2.getCart_ct());
                        value1.setFavor_ct(value1.getFavor_ct() + value2.getFavor_ct());
                        value1.setOrder_sku_num(value1.getOrder_sku_num() + value2.getOrder_sku_num());

                        value1.setOrder_amount(value1.getOrder_amount().add(value2.getOrder_amount()));
                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        value1.setOrder_ct((long) value1.getOrderIdSet().size());

                        value1.setPayment_amount(value1.getPayment_amount().add(value2.getPayment_amount()));
                        value1.getPaidOrderIdSet().addAll(value2.getPaidOrderIdSet());
                        value1.setPaid_order_ct((long) value1.getPaidOrderIdSet().size());

                        value1.setRefund_amount(value1.getRefund_amount().add(value2.getRefund_amount()));
                        value1.getRefundOrderIdSet().addAll(value2.getRefundOrderIdSet());
                        value1.setRefund_order_ct((long) value1.getRefundOrderIdSet().size());

                        value1.setComment_ct(value1.getComment_ct() + value2.getComment_ct());
                        value1.setGood_comment_ct(value1.getGood_comment_ct() + value2.getGood_comment_ct());

                        return value1;
                    }
                }, new WindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                    @Override
                    public void apply(Long aLong, TimeWindow window, Iterable<ProductStats> input, Collector<ProductStats> out) throws Exception {
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                        long start = window.getStart();
                        long end = window.getEnd();

                        String stt = simpleDateFormat.format(start);
                        String edt = simpleDateFormat.format(end);

                        // 取出聚合以后的数据
                        ProductStats productStats = input.iterator().next();
                        productStats.setStt(stt);
                        productStats.setEdt(edt);

                        out.collect(productStats);
                    }
                });

        // 打印测试
        // reduceDS.print("reduceDS>>>>>");

        // 7.关联维度信息
        // 7.1 关联SKU信息
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuDS = AsyncDataStream.unorderedWait(reduceDS,
                new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {
            @Override
            public String getKey(ProductStats input) {
                return input.getSku_id().toString();
            }

            @Override
            public void join(ProductStats input, JSONObject dimInfo) throws ParseException {
                // 获取维度中的信息
                input.setSku_name(dimInfo.getString("SKU_NAME"));
                input.setSku_price(dimInfo.getBigDecimal("PRICE"));
                input.setSpu_id(dimInfo.getLong("SPU_ID"));
                input.setTm_id(dimInfo.getLong("TM_ID"));
                input.setCategory3_id(dimInfo.getLong("CATEGORY3_ID"));
            }
        }, 300, TimeUnit.SECONDS);

        // 7.2 关联SPU信息
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDS =
                AsyncDataStream.unorderedWait(productStatsWithSkuDS, new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {
            @Override
            public String getKey(ProductStats input) {
                return input.getSpu_id().toString();
            }

            @Override
            public void join(ProductStats input, JSONObject dimInfo) throws ParseException {
                // 获取维度中的信息
                input.setSpu_name(dimInfo.getString("SPU_NAME"));
            }
        }, 300, TimeUnit.SECONDS);

        // 7.3 关联品类维度
        SingleOutputStreamOperator<ProductStats> productStatsWithCategoryDS =
                AsyncDataStream.unorderedWait(productStatsWithSpuDS, new DimAsyncFunction<ProductStats>(
                        "DIM_BASE_CATEGORY3") {
            @Override
            public String getKey(ProductStats input) {
                return input.getCategory3_id().toString();
            }

            @Override
            public void join(ProductStats input, JSONObject dimInfo) throws ParseException {
                // 获取维度中的信息
                input.setCategory3_name(dimInfo.getString("NAME"));
            }
        }, 300, TimeUnit.SECONDS);

        // 7.4 关联品牌维度
        SingleOutputStreamOperator<ProductStats> productStatsWithTmDS = AsyncDataStream.unorderedWait(productStatsWithCategoryDS, new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {
            @Override
            public String getKey(ProductStats input) {
                return input.getTm_id().toString();
            }

            @Override
            public void join(ProductStats input, JSONObject dimInfo) throws ParseException {
                // 获取维度信息
                input.setTm_name(dimInfo.getString("TM_NAME"));
            }
        }, 300, TimeUnit.SECONDS);

        // 打印测试
        productStatsWithTmDS.print(">>>>>>>");

        // 8.写入clickhouse
        productStatsWithTmDS.addSink(ClickHouseUtil.getSink("insert into product_stats values(?,?,?,?,?,?,?,?,?,?,?," +
                "?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        // 9.执行任务
        env.execute();
    }
}
