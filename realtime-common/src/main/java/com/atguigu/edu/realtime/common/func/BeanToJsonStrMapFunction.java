package com.atguigu.edu.realtime.common.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * ClassName: BeanToJsonStrMapFunction
 * Package: com.atguigu.edu.realtime.common.func
 * Description:
 *
 * @Author: tubage
 * @Create: 2024/5/7 16:50
 * @Version: 1.0
 */
public class BeanToJsonStrMapFunction<T> implements MapFunction<T,String> {
    // TODO dws 实体Bean转JSON字符串
    @Override
    public String map(T bean) throws Exception {
        SerializeConfig config = new SerializeConfig();
        config.setPropertyNamingStrategy(PropertyNamingStrategy.SnakeCase);
        String jsonStr = JSON.toJSONString(bean, config);
        return jsonStr;
    }
}
