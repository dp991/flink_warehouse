package org.traffic.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class OverSpeedInfo {
    private String carId;
    private String monitorId;
    private String roadId;
    private Double realSpeed;
    private Double limitSpeed;
    private Long actionTime;

}
