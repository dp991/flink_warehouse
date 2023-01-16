package org.example.bean;

import lombok.Data;
import org.apache.commons.lang3.ObjectUtils;

import java.math.BigDecimal;

/**
 * 订单宽表：订单表+订单明细表+所有维度表（字段去重之后）
 */
@Data
public class OrderWide {

    private Long detailId;
    private Long orderId;
    private Long skuId;
    private BigDecimal orderPrice;
    private Long skuNum;
    private String skuName;
    private Long provinceId;
    private String orderStatus;
    private Long userId;

    private BigDecimal totalAmount;
    private BigDecimal activityReduceAmount;
    private BigDecimal couponReduceAmount;
    private BigDecimal originalTotalAmount;
    private BigDecimal feightFee;
    private BigDecimal splitActivityAmount;
    private BigDecimal splitCouponAmount;
    private BigDecimal splitTotalAmount;

    private String expireTime;
    private String createTime;
    private String operateTime;
    private String createDate;
    private String createHour;

    private String provinceName; //查询维度
    private String provinceAreaCode;
    private String provinceISOCode;
    private String province31662Code;

    private Integer userAge;
    private String userGender;

    private Long spuId;
    private Long tmId;
    private Long category3Id;
    private String spuName;
    private String tmName;
    private String category3Name;

    public OrderWide(OrderInfo orderInfo, OrderDetail orderDetail) {
        mergeOrderInfo(orderInfo);
        mergeOrderDetail(orderDetail);
    }

    public void mergeOrderInfo(OrderInfo orderInfo) {
        if (orderInfo != null) {
            this.orderId = orderInfo.getId();
            this.orderStatus = orderInfo.getOrderStatus();
            this.createTime = orderInfo.getCreateTime();
            this.createDate = orderInfo.getCreateDate();
            this.createHour = orderInfo.getCreateHour();
            this.activityReduceAmount = orderInfo.getActivityReduceAmount();
            this.couponReduceAmount = orderInfo.getCouponReduceAmount();
            this.originalTotalAmount = orderInfo.getOriginalTotalAmount();
            this.feightFee = orderInfo.getFeightFee();
            this.totalAmount = orderInfo.getTotalAmount();
            this.provinceId = orderInfo.getProvinceId();
            this.userId = orderInfo.getUserId();
        }

    }

    public void mergeOrderDetail(OrderDetail orderDetail) {
        if (orderDetail != null) {
            this.detailId = orderDetail.getId();
            this.skuId = orderDetail.getSkuId();
            this.skuName = orderDetail.getSkuName();
            this.orderPrice = orderDetail.getOrderPrice();
            this.skuNum = orderDetail.getSkuNum();
            this.splitActivityAmount = orderDetail.getSplitActivityAmount();
            this.splitCouponAmount = orderDetail.getSplitCouponAmount();
            this.splitTotalAmount = orderDetail.getSplitTotalAmount();
        }
    }

    public void mergeOtherOrderWide(OrderWide orderWide) {
        this.orderStatus = ObjectUtils.firstNonNull(this.orderStatus, orderWide.getOrderStatus());
        this.createTime = ObjectUtils.firstNonNull(this.createTime, orderWide.getCreateTime());
        this.createDate = ObjectUtils.firstNonNull(this.createDate, orderWide.getCreateDate());
        this.couponReduceAmount = ObjectUtils.firstNonNull(this.couponReduceAmount, orderWide.getCouponReduceAmount());
        this.activityReduceAmount = ObjectUtils.firstNonNull(this.activityReduceAmount, orderWide.getActivityReduceAmount());
        this.originalTotalAmount = ObjectUtils.firstNonNull(this.originalTotalAmount, orderWide.getOriginalTotalAmount());
        this.feightFee = ObjectUtils.firstNonNull(this.feightFee, orderWide.getFeightFee());
        this.totalAmount = ObjectUtils.firstNonNull(this.totalAmount, orderWide.getTotalAmount());
        this.userId = ObjectUtils.firstNonNull(this.userId, orderWide.getUserId());
        this.skuId = ObjectUtils.firstNonNull(this.skuId, orderWide.getSkuId());
        this.skuName = ObjectUtils.firstNonNull(this.skuName, orderWide.getSkuName());
        this.orderPrice = ObjectUtils.firstNonNull(this.orderPrice, orderWide.getOrderPrice());
        this.skuNum = ObjectUtils.firstNonNull(this.skuNum, orderWide.getSkuNum());
        this.splitTotalAmount = ObjectUtils.firstNonNull(this.splitTotalAmount, orderWide.getSplitTotalAmount());
        this.splitCouponAmount = ObjectUtils.firstNonNull(this.splitCouponAmount, orderWide.getSplitCouponAmount());
        this.splitActivityAmount = ObjectUtils.firstNonNull(this.splitActivityAmount, orderWide.getSplitActivityAmount());
    }


}
