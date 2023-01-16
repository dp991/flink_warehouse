package org.example.bean;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class PaymentInfo {

    private Long id;
    private Long orderId;
    private Long userId;
    private BigDecimal totalAmount;
    private String subject;
    private String paymentType;
    private String createTime;
    private String callbackTime;

}
