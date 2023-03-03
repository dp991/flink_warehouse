package org.liwei_data.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import kong.unirest.Unirest;
import kong.unirest.UnirestException;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.util.EntityUtils;
import org.liwei_data.bean.BusInfo;
import org.liwei_data.bean.HotelInfo;
import org.liwei_data.bean.SightInfo;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Supplier;

@Slf4j
public class CommonUtil {

    public static Integer getHotelCommentCount(String comment) {
        comment = comment.trim();
        if (comment.contains("共") && comment.contains("条评论")) {
            String str = comment.replace("共", "").replace("条评论", "").trim();
            return Integer.valueOf(str);
        } else {
            return 0;
        }
    }

    public static Double getHotelScore(String score) {
        if (score == null || score.length() == 0) {
            return 0.0;
        } else if (score.contains("分")) {
            return Double.valueOf(score.replace("分", "").trim());
        } else {
            return Double.valueOf(score.trim());
        }
    }

    public static Double getPrice(String price) {
        if (price == null || price.length() == 0) {
            return 0.0;
        } else if (price.contains("起") && price.contains("¥")) {
            return Double.valueOf(price.replace("起", "").replace("¥", "").trim());
        } else {
            return 0.0;
        }
    }

    public static String getHotelAddress(String city, String address) {
        //近永盛路地铁站 · 萧山国际机场
        address = address
                .replaceAll("'", "")
                .replaceAll("\"", "")
                .replaceAll("\r", "")
                .replaceAll("\t", "")
                .replaceAll("\n", "");
        if (address != null && address.contains(" · ")) {
            String[] split = address.split(" · ");
            address = split[0].trim();
        }

        if (address.startsWith("近")) {
            int len = address.length();
            address = address.substring(1, len).trim();
        }

        if (!city.contains("市")) {
            if (!address.contains(city)) {
                return city + address;
            } else {
                return address;
            }
        } else {
            if (address.contains(city)) {
                return address;
            } else {
                return city + address;
            }
        }
    }

    public static String getRankClass(String rank) {
        if (rank.contains("5A")) {
            return "5";
        } else if (rank.contains("4A")) {
            return "4";
        } else if (rank.contains("3A")) {
            return "3";
        } else if (rank.contains("2A")) {
            return "2";
        } else if (rank.contains("1A")) {
            return "1";
        } else {
            return "0";
        }
    }

    public static String nameExcludeRankInfo(String name) {
        if (name.contains("5A")) {
            return name.replace("5A", "");
        } else if (name.contains("4A")) {
            return name.replace("4A", "");
        } else if (name.contains("3A")) {
            return name.replace("3A", "");
        } else if (name.contains("2A")) {
            return name.replace("2A", "");
        } else if (name.contains("1A")) {
            return name.replace("1A", "");
        } else {
            return name;
        }

    }

    public static Integer getCommentCount(String comment) {

        if (comment.contains("条点评")) {
            String cnt = comment.replace("条点评", "").trim();
            return Integer.parseInt(cnt);
        } else {
            return 0;
        }

    }

    public static Double getCommentScore(String comment) {

        if (comment.contains("/5分")) {
            String cnt = comment.replace("/5分", "").trim();
            return Double.parseDouble(cnt);
        } else {
            return 0.0;
        }

    }

    public static String getAddress(String city, String address) {

        int index = address.indexOf("(");
        if (index != -1) {
            address = address.substring(0, index);
        }
        index = address.indexOf(" ");

        if (index != -1) {
            address = address.substring(0, index);
        }
        index = address.indexOf("（");
        if (index != -1) {
            address = address.substring(0, index);
        }
        index = address.indexOf(",");
        if (index != -1) {
            address = address.substring(0, index);
        }
        index = address.indexOf("、");
        if (index != -1) {
            address = address.substring(0, index);
        }

        if (address.startsWith("市")) {
            address = city + "市" + address;
        } else if (address.contains("市")) {
            address = address.trim();

        } else if (address.endsWith("省")) {
            address = address.trim() + city + "市";

        } else {
            address = city + "市" + address;
        }
        return address.replaceAll(" ", "");
    }

    public static void getRequest(BusInfo bus, String newAddress) throws Exception {
        String url = "https://apis.map.qq.com/jsapi?qt=geoc&addr=%s&key=TU5BZ-MKD3W-L43RW-O3ZBW-GWMZK-QBB25&output=jsonp&pf=jsapi&ref=jsapi";
        String xyURL = String.format(url, newAddress);
        kong.unirest.HttpResponse<String> response = null;
        try {
            Thread.sleep(1000);
            response = Unirest.get(xyURL)
                    .asString();
        } catch (UnirestException e) {
            log.error("Unirest error: {}", xyURL);
            throw e;
        }

        if (response != null && response.getStatus() == 200) {
            JSONObject jsonObject = JSON.parseObject(response.getBody());
            JSONObject detail = jsonObject.getJSONObject("detail");
            if (detail != null) {
                String province = detail.getString("province");
                String adcode = detail.getString("adcode");
                String city = detail.getString("city");
                Double x = detail.getDouble("pointx");
                Double y = detail.getDouble("pointy");
                String district = detail.getString("district");
                String town = detail.getString("town");
                String analysisAddress = detail.getString("analysis_address");
                //实体类信息初始化
                bus.setCity(city);
                bus.setLon(x);
                bus.setLat(y);
                bus.setProvince(province);
                bus.setAdcode(adcode);
                bus.setDistrict(district);
                bus.setTown(town);
                bus.setAddress(analysisAddress);
            }
        }
    }

    public static void getRequest(SightInfo sightInfo, String newAddress) throws Exception {
        String url = "https://apis.map.qq.com/jsapi?qt=geoc&addr=%s&key=TU5BZ-MKD3W-L43RW-O3ZBW-GWMZK-QBB25&output=jsonp&pf=jsapi&ref=jsapi";
        String xyURL = String.format(url, newAddress);
        kong.unirest.HttpResponse<String> response = null;
        try {
            Thread.sleep(1000);
            response = Unirest.get(xyURL)
                    .asString();
        } catch (UnirestException e) {
            log.error("Unirest error: {}", xyURL);
            throw e;
        }

        if (response != null && response.getStatus() == 200) {
            JSONObject jsonObject = JSON.parseObject(response.getBody());
            JSONObject detail = jsonObject.getJSONObject("detail");
            if (detail != null) {
                String province = detail.getString("province");
                String adcode = detail.getString("adcode");
                String city = detail.getString("city");
                Double x = detail.getDouble("pointx");
                Double y = detail.getDouble("pointy");
                String district = detail.getString("district");
                String town = detail.getString("town");
                String analysisAddress = detail.getString("analysis_address");
                //实体类信息初始化
                sightInfo.setCity(city);
                sightInfo.setLon(x);
                sightInfo.setLat(y);
                sightInfo.setProvince(province);
                sightInfo.setAdcode(adcode);
                sightInfo.setDistrict(district);
                sightInfo.setTown(town);
                sightInfo.setAddress(analysisAddress);
            }
        }
    }


    public static void getRequest(HotelInfo hotelInfo, String newAddress) throws Exception {
        String url = "https://apis.map.qq.com/jsapi?qt=geoc&addr=%s&key=TU5BZ-MKD3W-L43RW-O3ZBW-GWMZK-QBB25&output=jsonp&pf=jsapi&ref=jsapi";
        String xyURL = String.format(url, newAddress);
        kong.unirest.HttpResponse<String> response = null;
        try {
            Thread.sleep(1000);
            response = Unirest.get(xyURL)
                    .asString();
        } catch (UnirestException e) {
            log.error("Unirest error: {}", xyURL);
            throw e;
        }

        if (response != null && response.getStatus() == 200) {
            JSONObject jsonObject = JSON.parseObject(response.getBody());
            JSONObject detail = jsonObject.getJSONObject("detail");
            if (detail != null) {
                String province = detail.getString("province");
                String adcode = detail.getString("adcode");
                String city = detail.getString("city");
                Double x = detail.getDouble("pointx");
                Double y = detail.getDouble("pointy");
                String district = detail.getString("district");
                String town = detail.getString("town");
                String analysisAddress = detail.getString("analysis_address");
                //实体类信息初始化
                hotelInfo.setCity(city);
                hotelInfo.setLon(x);
                hotelInfo.setLat(y);
                hotelInfo.setProvince(province);
                hotelInfo.setAdcode(adcode);
                hotelInfo.setDistrict(district);
                hotelInfo.setTown(town);
                if (analysisAddress == null || analysisAddress.length() == 0) {
                    hotelInfo.setAnalysis_address(newAddress);
                } else {
                    hotelInfo.setAnalysis_address(analysisAddress);
                }
            }
        }
    }

    public static CompletableFuture<Tuple2<Double, Double>> getLonAndLat(CloseableHttpAsyncClient client, String newAddress) {
        String xyUrl = String.format("https://apis.map.qq.com/jsapi?qt=geoc&addr=%s&key=TU5BZ-MKD3W-L43RW-O3ZBW-GWMZK-QBB25&output=jsonp&pf=jsapi&ref=jsapi&cb=qq.maps._svcb3.geocoder0", newAddress);

        HttpGet httpGet = new HttpGet(xyUrl);
        Future<HttpResponse> future = client.execute(httpGet, null);

        CompletableFuture<Tuple2<Double, Double>> completableFuture = CompletableFuture.supplyAsync(new Supplier<Tuple2<Double, Double>>() {
            @Override
            public Tuple2<Double, Double> get() {
                Double x = 0.0;
                Double y = 0.0;
                try {
                    HttpResponse response = future.get();
                    if (response.getStatusLine().getStatusCode() == 200) {
                        //请求成功
                        String tmpResult = EntityUtils.toString(response.getEntity());
                        String result = tmpResult.replace("qq.maps._svcb3.geocoder0(", "").trim().replace(")", "");
                        JSONObject object = JSON.parseObject(result);
                        JSONObject detail = object.getJSONObject("detail");
                        if (detail != null && detail.containsKey("pointx")) {
                            x = detail.getDouble("pointx");
                        }
                        if (detail != null && detail.containsKey("pointy")) {
                            y = detail.getDouble("pointy");
                        }
                        if (x == 0.0 || y == 0.0) {
                            return null;
                        }
                    }
                } catch (Exception e) {
                    return null;
                }
                return new Tuple2<Double, Double>(x, y);
            }
        });
        return completableFuture;
    }
}
