package org.traffic.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Comparator;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SpeedCountInfo implements Comparator<SpeedCountInfo> {
    private Long highSpeedCount;
    private Long middleSpeedCount;
    private Long normalSpeedCount;
    private Long lowSpeedCount;

    @Override
    public int compare(SpeedCountInfo o1, SpeedCountInfo o2) {
        //先比较高速
        if (o1.highSpeedCount != o2.highSpeedCount){
            return (int)(o1.highSpeedCount - o2.highSpeedCount);
        } else if (o1.middleSpeedCount != o2.middleSpeedCount){
            return (int)(o1.middleSpeedCount - o2.middleSpeedCount);
        } else if (o1.normalSpeedCount != o2.normalSpeedCount) {
            return (int)(o1.normalSpeedCount - o2.normalSpeedCount);
        }else {
            return (int)(o1.lowSpeedCount-o2.lowSpeedCount);
        }
    }

//    @Override
//    public int compare(SpeedCountInfo that) {
//        //先比较高速
//        if (this.highSpeedCount != that.highSpeedCount){
//            return (int)(this.highSpeedCount - that.highSpeedCount);
//        } else if (this.middleSpeedCount != that.middleSpeedCount){
//            return (int)(this.middleSpeedCount - that.middleSpeedCount);
//        } else if (this.normalSpeedCount != that.normalSpeedCount) {
//            return (int)(this.normalSpeedCount - that.normalSpeedCount);
//        }else {
//            return (int)(this.lowSpeedCount-that.lowSpeedCount);
//        }
//    }
}
