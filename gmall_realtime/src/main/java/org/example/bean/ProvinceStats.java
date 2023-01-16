package org.example.bean;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.Date;

@Data
@NoArgsConstructor
public class ProvinceStats {

    private String stt;
    private String edt;
    private Long province_id;
    private String province_name;
    private String province_area_code;
    private String province_iso_code;
    private String province_3166_2_code;
    private Long order_count;
    private BigDecimal order_amount;
    private Long ts;

    public ProvinceStats(OrderWide orderWide) {
        this.province_id = orderWide.getProvinceId();
        this.order_amount = orderWide.getSplitTotalAmount();
        this.province_name = orderWide.getProvinceName();
        this.province_area_code = orderWide.getProvinceAreaCode();
        this.province_iso_code = orderWide.getProvinceISOCode();
        this.province_3166_2_code = orderWide.getProvince31662Code();

        this.order_count = 1L;
        this.ts = new Date().getTime();

    }
}
