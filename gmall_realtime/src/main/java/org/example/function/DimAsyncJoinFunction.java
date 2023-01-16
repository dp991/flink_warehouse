package org.example.function;

import com.alibaba.fastjson.JSONObject;

public interface DimAsyncJoinFunction<T> {
    String getKey(T input);

    void join(T input, JSONObject dimInfo) throws Exception;
}
