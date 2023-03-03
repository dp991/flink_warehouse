package org.liwei_data.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class HotelInfo {
    private String province;
    private String city;
    private String adcode;
    private String district;
    private String town;

    private String hotel_name;
    private String hotel_type;
    private Double score;
    private String comment;
    private Integer comment_count;
    private String address;
    private String analysis_address;
    private Double price;
    private String picture;
    private Double lon;
    private Double lat;
}
