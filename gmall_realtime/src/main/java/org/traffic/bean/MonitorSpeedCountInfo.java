package org.traffic.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 最通畅的卡口信息
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class MonitorSpeedCountInfo {
    private String start;
    private String end;
    private String monitorId;
    private Long highSpeedCount;
    private Long middleSpeedCount;
    private Long normalSpeedCount;
    private Long lowSpeedCount;
}
