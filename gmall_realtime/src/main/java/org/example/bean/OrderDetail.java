package org.example.bean;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class OrderDetail {
    private Long id;
    private Long orderId;
    private Long skuId;
    private BigDecimal orderPrice;
    private Long skuNum;
    private String skuName;
    private String createTime;
    private BigDecimal splitTotalAmount;
    private BigDecimal splitActivityAmount;
    private BigDecimal splitCouponAmount;
    private Long createTs;
}
