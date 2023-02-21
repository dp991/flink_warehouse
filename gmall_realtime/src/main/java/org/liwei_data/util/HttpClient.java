package org.liwei_data.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;
import org.example.utils.RedisUtil;
import org.liwei_data.bean.SightInfo;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Supplier;

/**
 * 使用异步方式调用open api接口请求
 */
public class HttpClient extends RichAsyncFunction<SightInfo, SightInfo> {
    private transient
    CloseableHttpAsyncClient client;

    private transient Jedis jedis;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        RequestConfig requestConfig = RequestConfig
                .custom()
                .setSocketTimeout(6000) //获取数据时间超时
                .setConnectTimeout(6000) //连接超时
                .build();
        client = HttpAsyncClients
                .custom()
                .setMaxConnTotal(20)
                .setDefaultRequestConfig(requestConfig)
                .build();
        //获取redis
        jedis = RedisUtil.getJedis();
        client.start();
    }

    @Override
    public void close() throws Exception {
        client.close();
        jedis.close();
    }

    @Override
    public void timeout(SightInfo input, ResultFuture<SightInfo> resultFuture) throws Exception {
        System.out.println("timeout:" + input.toString());
    }

    @Override
    public void asyncInvoke(SightInfo sightInfo, ResultFuture resultFuture) {
        String xyUrl = null;
        String newAddress = null;
        try {
            String city = sightInfo.getCity();
            String address = sightInfo.getAddress();
            String name = sightInfo.getAddress();
            String key = "Flink" + "_" + city + "_" + address + "_" + name;
            String value = jedis.get(key);
            if (value == null) {
                String newAddress1 = CommonUtil.getAddress(city, address);
                String regExp = "[\n`~!@#$%^&*()+=|{}':;',\\[\\].<>/?~！@#￥%……&*（）——+|{}【】‘；\r\t：”“’。， 、？]";
                newAddress = newAddress1.replace(" ", "").replaceAll(regExp, "");
                xyUrl = String.format("https://apis.map.qq.com/jsapi?qt=geoc&addr=%s&key=TU5BZ-MKD3W-L43RW-O3ZBW-GWMZK-QBB25&output=jsonp&pf=jsapi&ref=jsapi&cb=qq.maps._svcb3.geocoder0", newAddress);
                HttpGet httpGet = new HttpGet(xyUrl);
                Thread.sleep(500);

                Future<HttpResponse> future = client.execute(httpGet, null);

                CompletableFuture.supplyAsync(new Supplier<SightInfo>() {
                    @Override
                    public SightInfo get() {
                        try {
                            HttpResponse response = future.get();
                            if (response.getStatusLine().getStatusCode() == 200) {
                                //请求成功
                                String tmpResult = EntityUtils.toString(response.getEntity());
                                String result = tmpResult.replace("qq.maps._svcb3.geocoder0(", "").trim().replace(")", "");
                                JSONObject object = JSON.parseObject(result);
                                JSONObject detail = object.getJSONObject("detail");
                                Double x = 0.0;
                                Double y = 0.0;
                                if (detail != null && detail.containsKey("pointx")) {
                                    x = detail.getDouble("pointx");
                                }
                                if (detail != null && detail.containsKey("pointy")) {
                                    y = detail.getDouble("pointy");
                                }
                                sightInfo.setLon(x);
                                sightInfo.setLat(y);
                            }
                            return sightInfo;
                        } catch (Exception e) {
                            System.err.println("address: "+sightInfo.getAddress());
                            return null;
                        }
                    }
                }).thenAccept((SightInfo sightInfo1) -> {
                    if (sightInfo1 != null) {
                        jedis.set(key, "1");
                        //发射出去
                        resultFuture.complete(Collections.singletonList(sightInfo1));
                    }
                });
            }
        } catch (InterruptedException e) {
            JSONObject error = new JSONObject();
            error.put("message", e.getMessage());
            error.put("main", sightInfo);
            error.put("url", xyUrl);
            error.put("newAddress", newAddress);
            System.out.println(error);
        }
    }
}
