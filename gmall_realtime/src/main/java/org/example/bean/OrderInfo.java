package org.example.bean;
import lombok.Data;

import java.math.BigDecimal;

/**
 * 订单实体类
 */
@Data
public class OrderInfo {
    private Long id;
    private Long provinceId;
    private String orderStatus;
    private Long userId;
    private BigDecimal totalAmount;
    private BigDecimal activityReduceAmount;
    private BigDecimal couponReduceAmount;
    private BigDecimal originalTotalAmount;
    private BigDecimal feightFee;
    private String expireTime;
    private String createTime;//yyyy-MM-dd HH:mm:ss
    private String operateTime;
    private String createDate;
    private String createHour;
    private Long createTs;

}
