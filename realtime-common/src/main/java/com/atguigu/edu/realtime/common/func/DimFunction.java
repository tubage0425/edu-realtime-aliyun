package com.atguigu.edu.realtime.common.func;

import com.alibaba.fastjson.JSONObject;

/**
 * ClassName: DimFunction
 * Package: com.atguigu.edu.realtime.common.func
 * Description:
 *
 * @Author: tubage
 * @Create: 2024/5/7 19:57
 * @Version: 1.0
 */
public interface DimFunction<T> {
    // TODO 做维度关联必须实现的接口
    void addDims(T obj, JSONObject dimJsonObj);

    String getRowKey(T obj) ;
    String getTableName();
}
