package org.traffic.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SpeedLimitInfo {
    private String areaId;
    private String roadId;
    private String monitorId;
    private Double speedLimit;
}
