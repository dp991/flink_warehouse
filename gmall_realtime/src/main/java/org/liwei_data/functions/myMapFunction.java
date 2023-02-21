package org.liwei_data.functions;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;
import org.liwei_data.bean.SightInfo;
import org.liwei_data.util.CommonUtil;

import java.util.concurrent.Future;

public class myMapFunction extends RichMapFunction<SightInfo, SightInfo> {

    private transient
    CloseableHttpAsyncClient client;

    @Override
    public void open(Configuration parameters) throws Exception {
        RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(3000).setConnectTimeout(3000).build();
        client = HttpAsyncClients.custom().setMaxConnTotal(20).setDefaultRequestConfig(requestConfig).build();
        client.start();
        System.out.println("client started");
    }


    @Override
    public SightInfo map(SightInfo sightInfo) throws Exception {
        String city = sightInfo.getCity();
        String address = sightInfo.getAddress();
        String newAddress = CommonUtil.getAddress(city, address);

        String xyUrl = String.format("https://apis.map.qq.com/jsapi?qt=geoc&addr=%s&key=TU5BZ-MKD3W-L43RW-O3ZBW-GWMZK-QBB25&output=jsonp&pf=jsapi&ref=jsapi&cb=qq.maps._svcb3.geocoder0", newAddress.replaceAll(" ", ""));
        HttpGet httpGet = new HttpGet(xyUrl);
        Future<HttpResponse> future = client.execute(httpGet, null);
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
                y = detail.getDouble("pointy");

            } else {
                //请求失败
                System.err.println("请求失败,url=" + xyUrl);
            }

            sightInfo.setLon(x);
            sightInfo.setLat(y);
        }
        return sightInfo;
    }

    @Override
    public void close() throws Exception {
        client.close();
    }
}
