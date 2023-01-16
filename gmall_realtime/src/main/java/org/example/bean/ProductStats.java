package org.example.bean;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;

/**
 * 构造者设计模式
 */
@Data
@Builder
public class ProductStats {
    private String stt; //窗口起始时间
    private String edt;//窗口结束时间
    private Long skuId;//sku编号
    private String skuName;
    private BigDecimal skuPrice;//商品单价
    private Long spuId;
    private String spuName;
    private Long tmId;
    private String tmName;
    private Long category3Id;
    private String category3Name;

    @Builder.Default
    private Long displayCt = 0L; //曝光数

    @Builder.Default
    private Long clickCt = 0L; //点击数
    @Builder.Default
    private Long favorCt = 0L;//收藏数
    @Builder.Default
    private Long cartCt = 0L;//添加购物车数
    @Builder.Default
    private Long orderSkuNum = 0L;//下单商品数
    @Builder.Default
    private BigDecimal orderAmount = BigDecimal.ZERO;//下单商品金额
    @Builder.Default
    private Long orderCt = 0L; //订单数
    @Builder.Default
    private BigDecimal paymentAmount = BigDecimal.ZERO;//支付金额
    @Builder.Default
    private Long paidOrderCt = 0L;//支付订单数
    @Builder.Default
    private Long refundOrderCt = 0L;//退款订单数
    @Builder.Default
    private BigDecimal refundAmount = BigDecimal.ZERO;//退款数
    @Builder.Default
    private Long commentCt = 0L;//评论订单数
    @Builder.Default
    private Long goodCommentCt = 0L;//好评订单数
    @Builder.Default
    @TransientSink
    private Set orderIdSet = new HashSet();//用于统计订单数,不进行序列化，只用于统计分析
    @Builder.Default
    @TransientSink
    private Set paidOrderIdSet = new HashSet();//用于统计支付订单数，不进行序列化，只用于统计分析
    @Builder.Default
    @TransientSink
    private Set refundOrderIdSet = new HashSet();//用于退款支付订单数，不进行序列化，只用于统计分析

    private Long ts;//统计时间戳


}
