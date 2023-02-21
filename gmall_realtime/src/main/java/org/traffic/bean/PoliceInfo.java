package org.traffic.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 出警信息
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class PoliceInfo {

    private String policeId;
    private String carId;
    private String actionTime;
    private String status;

}
