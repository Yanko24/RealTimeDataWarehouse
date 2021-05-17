package com.yankee.gmall.realtime.bean;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description 商品主题宽表
 * @date 2021/5/17 17:00
 */
@Data
@Builder
public class ProductStats {
    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;
    // sku编号
    Long sku_id;
    // sku名称
    String sku_name;
    // sku单价
    BigDecimal sku_price;
    // spu编号
    Long spu_id;
    // spu名称
    String spu_name;
    // 品牌编号
    Long tm_id;
    // 品牌名称
    String tm_name;
    // 品类编号
    Long category3_id;
    // 品类名称
    String category3_name;

    // 曝光数
    @Builder.Default
    Long display_ct = 0L;

    // 点击数
    @Builder.Default
    Long click_ct = 0L;

    // 收藏数
    @Builder.Default
    Long favor_ct = 0L;

    // 添加购物车
    @Builder.Default
    Long cart_ct = 0L;

    // 下单商品个数
    @Builder.Default
    Long order_sku_num = 0L;

    // 下单商品金额
    @Builder.Default
    BigDecimal order_amount = BigDecimal.ZERO;

    // 订单数
    @Builder.Default
    Long order_ct = 0L;

    // 支付金额
    @Builder.Default
    BigDecimal payment_amount = BigDecimal.ZERO;

    // 支付订单数
    @Builder.Default
    Long paid_order_ct = 0L;

    // 退款订单数
    @Builder.Default
    Long refund_order_ct = 0L;

    // 退款金额
    @Builder.Default
    BigDecimal refund_amount = BigDecimal.ZERO;

    // 订单评论数
    @Builder.Default
    Long comment_ct = 0L;

    // 好评订单数
    @Builder.Default
    Long good_comment_ct = 0L;

    // 用于统计订单数
    @Builder.Default
    @TransientSink
    Set orderIdSet = new HashSet();

    // 用于统计支付订单数
    @Builder.Default
    @TransientSink
    Set paidOrderIdSet = new HashSet();

    // 用于统计退款支付订单数
    @Builder.Default
    @TransientSink
    Set refundOrderIdSet = new HashSet();

    // 统计时间戳
    Long ts;
}
