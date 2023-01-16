package org.example.bean;

import lombok.Data;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;

@Data
public class PaymentWide {
    private Long paymentId;
    private String subject;
    private String paymentType;
    private String paymentCreateTime;
    private String callbackTime;
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
    private BigDecimal splitFeightFee;
    private BigDecimal splitActivityAmount;
    private BigDecimal splitCouponAmount;
    private String orderCreateTime;

    private String provinceName;
    private String provinceAreaCode;
    private String provinceIsoCode;
    private String province31662Code;
    private Integer userAge;
    private String userGender;

    private Long spuId;
    private Long tmId;
    private Long category3Id;
    private String spuName;
    private String tmName;
    private String category3Name;

    public PaymentWide(PaymentInfo info, OrderWide orderWide) {
        mergeOrderWide(orderWide);
        mergePaymentInfo(info);
    }

    private void mergePaymentInfo(PaymentInfo info) {
        if (info != null) {
            try {
                BeanUtils.copyProperties(this, info);
                this.paymentCreateTime = info.getCreateTime();
                this.paymentId = info.getId();

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void mergeOrderWide(OrderWide orderWide) {
        if (orderWide != null) {
            try {
                BeanUtils.copyProperties(this, orderWide);
                this.orderCreateTime = orderWide.getCreateTime();
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            } catch (InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        }
    }


}
