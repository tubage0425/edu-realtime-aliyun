package com.atguigu.edu.realtime.common.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.constant.Constant;
import com.atguigu.edu.realtime.common.util.HBaseUtil;
import com.atguigu.edu.realtime.common.util.RedisUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

/**
 * ClassName: DimMapFunction
 * Package: com.atguigu.edu.realtime.common.func
 * Description:
 *
 * @Author: tubage
 * @Create: 2024/5/7 19:56
 * @Version: 1.0
 */
public abstract class DimMapFunction<T> extends RichMapFunction<T,T> implements DimFunction<T> {
    Connection hbaseConn;
    Jedis jedis;
    @Override
    public void open(Configuration parameters) throws Exception {
        jedis = RedisUtil.getJedis();
        hbaseConn = HBaseUtil.getHbaseConnection();
    }

    @Override
    public void close() throws Exception {
        RedisUtil.closeJedis(jedis);
        HBaseUtil.closeHbaseConnection(hbaseConn);
    }
    @Override
    public T map(T obj) throws Exception {
        // 获取维度主键 先从Redis查 查不到再从HBASE查，查完写入HBASE（写入维度的时候，清空Redis 一致性）
        String key = getRowKey(obj);
        String tableName = getTableName();
        JSONObject dimJsonObj = RedisUtil.readDim(jedis, tableName, key);

        if(dimJsonObj != null) {
            // 缓存命中
            System.out.println("从Redis找到"+ getTableName()+ "的维度主键id:" + key + "的维度数据");
        }else {
            dimJsonObj = HBaseUtil.getRow(hbaseConn, Constant.HBASE_NAMESPACE,tableName,key, JSONObject.class);

            if(dimJsonObj != null) {
                System.out.println("从Hbase找到"+ getTableName()+ "的维度主键id:" + key + "的维度数据");
                RedisUtil.writeDim(jedis,"dim_course_info",key,dimJsonObj);
            }else {
                System.out.println("没有找到"+ getTableName()+ "的维度主键id:" + key + "的维度数据");
            }
        }

        // 补充
        if(dimJsonObj != null) {
            addDims(obj,dimJsonObj);
        }
        return obj;
    }

}
