package org.liwei_data.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class BusInfo {
    private String province;
    private String city;
    private String adcode;
    private String district;
    private String town;

    private String line_name;
    private String bus_line_name;
    private String forward;
    private String station_name;
    private String address;
    private String run_time;
    private String run_time_desc;
    private String company;
    private String price_info;
    private Integer station_id;
    private Double lon;
    private Double lat;
}
