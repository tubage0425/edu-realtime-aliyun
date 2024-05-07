package com.atguigu.edu.realtime.common.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.constant.Constant;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * ClassName: RedisUtil
 * Package: com.atguigu.edu.realtime.common.util
 * Description:
 *
 * @Author: tubage
 * @Create: 2024/5/7 19:25
 * @Version: 1.0
 */
public class RedisUtil {
    // TODO 获取Jedis
    private static String host = Constant.JEDIS_HOST;
    private static int port = Constant.JEDIS_PORT;
    private static JedisPool jedisPool;
    static {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMinIdle(5);
        jedisPoolConfig.setMaxTotal(100);
        jedisPoolConfig.setMaxIdle(5);
        jedisPoolConfig.setBlockWhenExhausted(true); // 资源耗尽 等待
        jedisPoolConfig.setMaxWaitMillis(2000); // 等待时间
        jedisPoolConfig.setTestOnBorrow(true);

        jedisPool = new JedisPool(jedisPoolConfig,host,port,10000);
    }

    public static Jedis getJedis() {
        System.out.println("jedis连接");
        return jedisPool.getResource();
    }
    public static void closeJedis(Jedis jedis) {
        System.out.println("jedis连接关闭");
        if(jedis != null) jedis.close();
    }

    // TODO dws 优化 异步操作Redis客户端连接
    public static StatefulRedisConnection<String,String> getAsyncRedisConnection() {
        System.out.println("redis异步连接");
        RedisClient redisClient = RedisClient.create(Constant.REDIS_URL);
        StatefulRedisConnection<String, String> redisAsyncConn = redisClient.connect();
        return redisAsyncConn;
    }


    // TODO dws 优化 关闭异步操作Redis客户端连接
    public static void closeAsyncRedisConnection(StatefulRedisConnection<String,String> redisAsyncConn){
        System.out.println("redis异步连接关闭");
        if(redisAsyncConn != null && redisAsyncConn.isOpen()) {
            redisAsyncConn.close();
        }

    }

    // TODO dws 优化 异步方式读取
    public static JSONObject readDimAsync(StatefulRedisConnection<String,String> redisAsyncConn, String tableName, String id) {
        String redisKey = getRedisKey(tableName, id);

        RedisAsyncCommands<String, String> asyncCommand = redisAsyncConn.async();
        try {
            String dimJsonStr = asyncCommand.get(redisKey).get();
            if(StringUtils.isNotEmpty(dimJsonStr)) {
                JSONObject dimJsonObj = JSON.parseObject(dimJsonStr);
                return dimJsonObj;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    // TODO dws 优化 异步方式写入
    public static void writeDimAsync( StatefulRedisConnection<String,String> redisAsyncConn, String tableName, String id,JSONObject dimJsonObj) {
        String redisKey = getRedisKey(tableName, id);
        RedisAsyncCommands<String, String> asyncCommand = redisAsyncConn.async();
        asyncCommand.setex(redisKey,24*3600,dimJsonObj.toJSONString());

    }


    // TODO 从redis查询数据
    public static JSONObject readDim(Jedis jedis, String tableName, String id) {
        String redisKey = getRedisKey(tableName,id);
        String dimJsonStr = jedis.get(redisKey);
        if(StringUtils.isNotEmpty(dimJsonStr)) {
            JSONObject dimJsonObj = JSON.parseObject(dimJsonStr);
            return dimJsonObj;
        }
        return null;
    }

    // TODO 向redis放数据
    public static void writeDim(Jedis jedis, String tableName, String id, JSONObject dimJsonObj){
        String redisKey = getRedisKey(tableName, id);
        jedis.setex(redisKey, 24*3600,dimJsonObj.toJSONString());
    }

    public static String getRedisKey(String tableName, String id) {
        return tableName + ":" + id;
    }

    // 测试Redis连接
    public static void main(String[] args) {
        // StatefulRedisConnection<String, String> asyncRedisConnection = getAsyncRedisConnection();
        // closeAsyncRedisConnection(asyncRedisConnection);

        Jedis jedis = getJedis();
        System.out.println("jedis = " + jedis);
        closeJedis(jedis);
    }
}
