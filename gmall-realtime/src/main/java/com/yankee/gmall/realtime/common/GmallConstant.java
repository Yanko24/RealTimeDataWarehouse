package com.yankee.gmall.realtime.common;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description 评价码表
 * @date 2021/5/18 8:39
 */
public class GmallConstant {
    // 07 退款状态
    // 审批中
    public static final String REFUND_STATUS_APPROVAL = "0701";
    // 审批通过
    public static final String REFUND_STATUS_APPROVE_AGREE = "0702";
    // 审批不通过
    public static final String REFUND_STATUS_APPROVE_DISAGREE = "0703";
    // 已发货
    public static final String REFUND_STATUS_SHIPPED = "0704";
    // 退款完成
    public static final String REFUND_STATUS_REFUND_COMPLETE = "0705";
    // 退款中
    public static final String REFUND_STATUS_REFUNDING = "0706";
    // 退款失败
    public static final String REFUND_STATUS_REFUND_FAILED = "0707";

    // 08 支付状态
    // 支付中
    public static final String PAYMENT_STATUS_PAYMENTS = "0801";
    // 已支付
    public static final String PAYMENT_STATUS_PAID = "0802";
    // 支付失败
    public static final String PAYMENT_STATUS_PAYMENT_FAILED = "0803";
    // 已关闭
    public static final String PAYMENT_STATUS_CLOSED = "0804";

    // 09 订单进度状态
    // 未支付
    public static final String ORDER_PROGRESS_STATUS_UNPAID = "0901";
    // 已支付
    public static final String ORDER_PROGRESS_STATUS_PAID = "0902";
    // 已通知仓储
    public static final String ORDER_PROGRESS_STATUS_WAREHOUSING = "0903";
    // 待发货
    public static final String ORDER_PROGRESS_STATUS_DELIVERED = "0904";
    // 库存异常
    public static final String ORDER_PROGRESS_STATUS_ABNORMAL = "0905";
    // 已发货
    public static final String ORDER_PROGRESS_STATUS_SHIPPED = "0906";
    // 已关闭
    public static final String ORDER_PROGRESS_STATUS_CLOSED = "0907";
    // 已签收
    public static final String ORDER_PROGRESS_STATUS_RECEIVER = "0908";
    // 已完结
    public static final String ORDER_PROGRESS_STATUS_FINISHED = "0909";
    // 已评价
    public static final String ORDER_PROGRESS_STATUS_COMMENTED = "0910";
    // 支付失败
    public static final String ORDER_PROGRESS_STATUS_PAYMENT_FAILED = "0911";
    // 订单已拆分
    public static final String ORDER_PROGRESS_STATUS_ORDER_SPLIT = "0912";

    // 10 单据状态
    // 未支付
    public static final String ORDER_STATUS_UNPAID = "1001";
    // 已支付
    public static final String ORDER_STATUS_PAID = "1002";
    // 已取消
    public static final String ORDER_STATUS_CANCEL = "1003";
    // 已完成
    public static final String ORDER_STATUS_FINISH = "1004";
    // 退款中
    public static final String ORDER_STATUS_REFUND = "1005";
    // 退款完成
    public static final String ORDER_STATUS_REFUND_DONE = "1006";

    // 11 支付类型
    // 支付宝
    public static final String PAYMENT_TYPE_ALIPAY = "1101";
    // 微信
    public static final String PAYMENT_TYPE_WECHAT = "1102";
    // 银联
    public static final String PAYMENT_TYPE_UNION = "1103";

    // 12 评价
    // 差评
    public static final String APPRAISE_BAD = "1201";
    // 中评
    public static final String APPRAISE_SOSO = "1202";
    // 好评
    public static final String APPRAISE_GOOD = "1203";
    // 自动
    public static final String APPRAISE_AUTO = "1204";

    // 13 退货原因
    // 质量问题
    public static final String REFUND_REASON_BAD_GOODS = "1301";
    // 商品描述与实际描述不一样
    public static final String REFUND_REASON_WRONG_DESC = "1302";
    // 缺货
    public static final String REFUND_REASON_SALE_OUT = "1303";
    // 号码不合适
    public static final String REFUND_REASON_SIZE_ISSUE = "1304";
    // 拍错
    public static final String REFUND_REASON_MISTAKE = "1305";
    // 不想要了
    public static final String REFUND_REASON_NO_REASON = "1306";
    // 其他
    public static final String REFUND_REASON_OTHER = "1307";

    // 14 购物券状态
    // 未使用
    public static final String COUPON_STATUS_UNUSED = "1401";
    // 使用中
    public static final String COUPON_STATUS_USING = "1402";
    // 已使用
    public static final String COUPON_STATUS_USED = "1403";

    // 15 退款类型
    // 仅退款
    public static final String REFUND_TYPE_ONLY_MONEY = "1501";
    // 退货退款
    public static final String REFUND_TYPE_WITH_GOODS = "1502";

    // 24 来源类型
    // 用户查询
    public static final String SOURCE_TYPE_QUERY = "2401";
    // 商品推广
    public static final String SOURCE_TYPE_PROMOTION = "2402";
    // 智能推荐
    public static final String SROUCE_TYPE_AUTO_RECOMMEND = "2403";
    // 促销活动
    public static final String SOURCE_TYPE_ACTIVITY = "2404";

    // 31 活动类型
    // 满额减价
    public static final String ACTIVITY_RULE_TYPE_MJ = "3101";
    // 满量打折
    public static final String ACTIVITY_RULE_TYPE_DZ = "3102";
    // 直接折扣
    public static final String ACTIVITY_RULE_TYPE_ZK = "3103";

    // 32 购物券类型
    // 满额减价
    public static final String COUPON_TYPE_MJ = "3201";
    // 满量打折
    public static final String COUPON_TYPE_DZ = "3202";
    // 代金券
    public static final String COUPON_TYPE_DJ = "3203";
    // 折扣券
    public static final String COUPON_TYPE_ZK = "3204";

    // 33 购物券范围
    // 品类券
    public static final String COUPON_RANCE_TYPE_CATEGORY3 = "3301";
    // 品牌券
    public static final String COUPON_RANCE_TYPE_TRADEMARK = "3302";
    // 单品
    public static final String COUPON_RANCE_TYPE_SPU = "3303";

    // 关键词搜索
    public static final String KEYWORD_SEARCH = "SEARCH";
    // 关键词点击
    public static final String KEYWORD_CLICK = "CLICK";
    // 关键词购物车
    public static final String KEYWORD_CART = "CART";
    // 关键词订单
    public static final String KEYWORD_ORDER = "ORDER";
}
