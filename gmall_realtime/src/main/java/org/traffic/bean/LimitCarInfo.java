package org.traffic.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class LimitCarInfo {
    private String areaId;
    private String roadId;
    private String monitorId;
    private String cameraId;
    private String actionTime;
    private String carId;
    private Double speed;
    private Double limitSpeed;
}
