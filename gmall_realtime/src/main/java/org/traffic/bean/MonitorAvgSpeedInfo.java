package org.traffic.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MonitorAvgSpeedInfo {
    private Long start;
    private Long end;
    private String monitorId;
    private Double avgSpeed;
    private Long carCount;
}
