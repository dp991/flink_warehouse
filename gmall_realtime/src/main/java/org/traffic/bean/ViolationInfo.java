package org.traffic.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 违法车辆
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ViolationInfo {
    private String carId;
    private String violation;
    private String createTime;
    private String description;

}
