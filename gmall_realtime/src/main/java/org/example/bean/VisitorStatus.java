package org.example.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class VisitorStatus {
    //统计开始时间
    private String stt;
    //统计结束时间
    private String edt;
    //维度：版本
    private String vc;
    //维度：渠道
    private String ch;
    //维度：地区
    private String ar;
    //维度：新老用户标识
    private String isNew;
    //度量：独立访客数
    private Long uvCt=0L;
    //度量：页面访问数
    private Long pvCt=0L;
    //度量：进入次数
    private Long svCt=0L;
    //度量：跳出次数
    private Long ujCt=0L;
    //度量：持续访问时间
    private Long durCt=0L;
    //统计时间
    private Long ts;
}
