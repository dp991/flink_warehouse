package org.liwei_data.bean;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SightInfo {
    private String city;
    private String name;
    private String rank_class;
    private Double heat_score;
    private Double comment_score;
    private Integer comment_count;
    private String rank_info;
    private String address;
    private String open_info;
    private String phone;
    private Double lon;
    private Double lat;

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
