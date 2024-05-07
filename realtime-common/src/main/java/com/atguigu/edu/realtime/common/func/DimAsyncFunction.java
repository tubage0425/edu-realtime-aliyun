package com.atguigu.edu.realtime.common.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.constant.Constant;
import com.atguigu.edu.realtime.common.util.HBaseUtil;
import com.atguigu.edu.realtime.common.util.RedisUtil;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.client.AsyncConnection;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * ClassName: DimAsyncFunction
 * Package: com.atguigu.edu.realtime.common.func
 * Description:
 *
 * @Author: tubage
 * @Create: 2024/5/7 20:13
 * @Version: 1.0
 */
// TODO DWS 优化 关联维度 异步IO 匿名内部类抽取模版
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T> implements DimFunction<T>{
    private AsyncConnection hbaseConnAsync;
    private StatefulRedisConnection<String,String> redisAsyncConn;

    @Override
    public void open(Configuration parameters) throws Exception {
        hbaseConnAsync = HBaseUtil.getHbaseConnectionAsync();
        redisAsyncConn = RedisUtil.getAsyncRedisConnection();
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closeHbaseConnectionAsync(hbaseConnAsync);
        RedisUtil.closeAsyncRedisConnection(redisAsyncConn);
    }
    @Override
    public void asyncInvoke(T obj, ResultFuture<T> resultFuture) throws Exception {
        // TODO 创建异步编排对象 执行线程任务，有返回结果，这个结果作为下一个线程任务的入参
        CompletableFuture.supplyAsync(
                new Supplier<JSONObject>() {
                    @Override
                    public JSONObject get() {
                        // 从Redis获取维度数据
                        JSONObject dimJsonObj = RedisUtil.readDimAsync(redisAsyncConn, getTableName(), getRowKey(obj));
                        return dimJsonObj;
                    }
                }
        ).thenApplyAsync(
                // 有入参 有返回值
                new Function<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject apply(JSONObject dimJsonObj) {
                        // 缓存命中
                        if(dimJsonObj != null) {
                            System.out.println("从Redis查到"+getTableName() + ":"+ getRowKey(obj) +"的维度");
                        }else{
                            // Hbase查询 放Redis缓存
                            dimJsonObj = HBaseUtil.getRowAsync(hbaseConnAsync, Constant.HBASE_NAMESPACE,getTableName(),getRowKey(obj));
                            if(dimJsonObj != null) {
                                System.out.println("从HBASE查到"+getTableName() + ":"+ getRowKey(obj) +"的维度");
                                RedisUtil.writeDimAsync(redisAsyncConn,getTableName(),getRowKey(obj),dimJsonObj);
                            }else {
                                System.out.println("没有找到"+getTableName() + ":"+ getRowKey(obj) +"的维度");
                            }
                        }
                        return dimJsonObj;
                    }
                }
        ).thenAcceptAsync(
                // 有入参 无返回
                new Consumer<JSONObject>() {
                    @Override
                    public void accept(JSONObject dimJsonObj) {
                        // 将维度属性补充到流中对象
                        if(dimJsonObj != null) {
                            addDims(obj, dimJsonObj);
                        }
                        // 补充完后向下传递
                        resultFuture.complete(Collections.singleton(obj));
                    }
                }
        );

    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        // 超时没有关联上 调用这个方法
        throw new RuntimeException("如果异步关联维度超时，可以做如下检查：\n"
                + "1. 检查相关组件是否启动：zk kf hdfs hbase redis\n"
                + "2. 查看Redis bind配置，是否注掉或者为0.0.0.0\n"
                + "3. 查看Hbase维度表中是否存在维度数据、脚本是否同步了维度历史数据");
    }


}

