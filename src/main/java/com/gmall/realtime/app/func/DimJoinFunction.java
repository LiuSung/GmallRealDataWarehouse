package com.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;

public interface DimJoinFunction<T> {
    //对于泛型对象的赋值有两种方法，一种是通过反射的方式赋值，另一种是通过抽象方法的方式赋值
    String getKey(T input);
    void join(T input, JSONObject diminfo);
}
