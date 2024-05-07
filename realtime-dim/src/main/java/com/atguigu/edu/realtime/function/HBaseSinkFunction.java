package com.atguigu.edu.realtime.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.bean.TableProcessDim;
import com.atguigu.edu.realtime.common.constant.Constant;
import com.atguigu.edu.realtime.common.util.HBaseUtil;
import com.atguigu.edu.realtime.common.util.RedisUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

/**
 * ClassName: HBaseSinkFunction
 * Package: com.atguigu.edu.realtime.function
 * Description:
 *
 * @Author: tubage
 * @Create: 2024/5/6 15:39
 * @Version: 1.0
 */
public class HBaseSinkFunction extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>> {
    // TODO 自定义sink写入HBase
    Connection hbaseConn;
    Jedis jedis;
    @Override
    public void open(Configuration parameters) throws Exception {
        hbaseConn = HBaseUtil.getHbaseConnection();
        jedis = RedisUtil.getJedis();
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closeHbaseConnection(hbaseConn);
        RedisUtil.closeJedis(jedis);
    }

    @Override
    public void invoke(Tuple2<JSONObject, TableProcessDim> value, Context context) throws Exception {
        JSONObject jsonObj = value.f0;
        TableProcessDim tableProcessDim = value.f1;

        String type = jsonObj.getString("type");
        jsonObj.remove("type");

        String sinkTable = tableProcessDim.getSinkTable();
        String rowKey = jsonObj.getString(tableProcessDim.getSinkPk());

        if("delete".equals(type)) {
            // 从HBASE删除维度
            HBaseUtil.deleteRow(hbaseConn, Constant.HBASE_NAMESPACE,sinkTable,rowKey);
        }else{
            // put到hbase
            String sinkFamily = tableProcessDim.getSinkFamily();
            HBaseUtil.putRow(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable,rowKey,sinkFamily,jsonObj);
        }

        // TODO DWS下单汇总旁路缓存：如果业务数据库维度表发生了变化，清除Redis中的缓存
        if("delete".equals(type) || "update".equals(type)) {
            jedis.del(sinkTable + ":" + rowKey);
        }
    }
}
