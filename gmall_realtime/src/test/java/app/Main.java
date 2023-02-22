package app;

import org.apache.commons.lang3.StringUtils;
import org.liwei_data.util.CommonUtil;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {
    public static void main(String[] args) {
//        String s = "大井巷60 ".replace(" ","");
//        String newAddress=CommonUtil.getAddress("杭州", s).replaceAll(" ", "");
//        String xyUrl = String.format("https://apis.map.qq.com/jsapi?qt=geoc&addr=%s&key=TU5BZ-MKD3W-L43RW-O3ZBW-GWMZK-QBB25&output=jsonp&pf=jsapi&ref=jsapi&cb=qq.maps._svcb3.geocoder0", newAddress);
//
//        System.out.println(xyUrl);
        String city="杭州";
        String address="";
//        String url = "https://apis.map.qq.com/jsapi?qt=geoc&addr=%s&key=TU5BZ-MKD3W-L43RW-O3ZBW-GWMZK-QBB25&output=jsonp&pf=jsapi&ref=jsapi";
//        String regExp = "[\n`~!@#$%^&*()+=|{}':;',\\[\\].<>/?~！@#￥%……&*（）——+|{}【】‘；\r\t：”“’。， 、？]";
//        String newAddress = address
//                .replace(" ", "")
//                .replace(" ", "")
//                .replaceAll(regExp, "")
//                .replace(" ", "")
//                .replaceAll("\"", "");
//        String xyURL = String.format(url, newAddress);
//        System.out.println(newAddress);
        if (StringUtils.isAnyBlank(city,address)){
            System.out.println("blank");
        }
    }
}

//3> {"address":"杭州市建德市南面24公里处大慈岩景区","city":"杭州","comment_count":1445,"comment_score":4.5,"heat_score":5.7,"lat":29.313263,"lon":119.291817,"name":"大慈岩风景区","open_info":"开园中；08:00-16:00开放（16:00停止入园）","phone":"0571-64549418,0571-64549279","rank_class":"4A","rank_info":""}
//        出错了：{"address":"杭州市拱墅区东文街90号（新天地园区内）新天地太阳剧场","city":"杭州","comment_count":496,"comment_score":4.7,"heat_score":5.5,"lat":0.0,"lon":0.0,"name":"杭州X秀","open_info":"未开园；今日19:30开放","phone":"4006-818-888","rank_class":"N","rank_info":"杭州亲子乐园景点榜 No.9"}
//        error:sleep interrupted
//        出错了：{"address":"湖州市德清县下渚湖街道莫干山开元森泊度假乐园","city":"杭州","comment_count":1386,"comment_score":5.0,"heat_score":6.2,"lat":0.0,"lon":0.0,"name":"莫干山森泊幻想岛儿童乐园","open_info":"开园中；10:00-20:00开放","phone":"","rank_class":"N","rank_info":"德清亲子乐园景点榜 No.4"}
//        error:sleep interrupted
//        出错了：{"address":"杭州市下城区西湖文化广场E区","city":"杭州","comment_count":167,"comment_score":4.6,"heat_score":5.5,"lat":0.0,"lon":0.0,"name":"浙江省博物馆武林馆区","open_info":"开园中；09:00-17:00开放（16:30停止入园）","phone":"0571-85391628,0571-88272913","rank_class":"N","rank_info":"杭州9大展览馆 No.5"}
//        error:sleep interrupted
//        2023-02-20 12:58:17,710 ERROR [org.apache.flink.runtime.taskexecutor.TaskExecutor] - Task did not exit gracefully within 180 + seconds.
//        org.apache.flink.util.FlinkRuntimeException: Task did not exit gracefully within 180 + seconds.
//        at org.apache.flink.runtime.taskmanager.Task
