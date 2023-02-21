package org.liwei_data.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.util.EntityUtils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Supplier;

public class CommonUtil {

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
