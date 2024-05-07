package com.atguigu.edu.realtime.common.util;

import com.atguigu.edu.realtime.common.constant.Constant;

/**
 * ClassName: SQLUtil
 * Package: com.atguigu.edu.realtime.common.util
 * Description:
 *
 * @Author: tubage
 * @Create: 2024/5/7 08:39
 * @Version: 1.0
 */
public class SQLUtil {
    // TODO flink sql工具类
    // 获取kafka连接器相关属性
    public static String getKafkaDDL (String topic, String groupId) {
        return "WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '"+topic+"',\n" +
                "  'properties.bootstrap.servers' = '"+ Constant.KAFKA_BROKERS+"',\n" +
                "  'properties.group.id' = '"+groupId+"',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json'\n" +
                ")";
    }

    // TODO HBase连接器相关属性
    public static String getHBaseDDL(String tableName) {
        return "WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = '"+tableName+"',\n" +
                " 'zookeeper.quorum' = '"+Constant.ZK_BROKERS+"',\n" +
                " 'lookup.async' = 'true',\n" +
                " 'lookup.cache' = 'PARTIAL',\n" +
                " 'lookup.partial-cache.max-rows' = '200',\n" +
                " 'lookup.partial-cache.expire-after-write' = '1 hour',\n" +
                " 'lookup.partial-cache.expire-after-access' = '1 hour'\n" +
                ")";
    }

    // TODO upsert kafka连接器属性
    public static String getUpsertKafkaDDL(String topic) {
        return "WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '"+topic+"',\n" +
                "  'properties.bootstrap.servers' = '"+Constant.KAFKA_BROKERS+"',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")";
    }

    // TODO dws层 doris连接器属性
    public static String getDorisSinkDDL(String tableName) {
        return "WITH (\n" +
                "      'connector' = 'doris',\n" +
                "      'fenodes' = '"+Constant.DORIS_FE_NODES+"',\n" +
                "      'table.identifier' = '"+Constant.DORIS_DATABASE+"."+tableName+"',\n" +
                "      'username' = '"+Constant.DORIS_USER+"',\n" +
                "      'password' = '"+ Constant.DORIS_PASSWORD+"',\n" +
                "      'sink.properties.format' = 'json', \n" +
                "      'sink.buffer-count' = '4', \n" +
                "      'sink.buffer-size' = '4086',\n" +
                "      'sink.enable-2pc' = 'false',  \n" +
                "      'sink.properties.read_json_by_line' = 'true'\n" +
                ")";
    }

    // TODO dws层 doris 使用flink jdbc连接属性
    public static String getDorisJDBCSinkDDL(String tableName) {
        return "WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                "   'url' = '"+Constant.DORIS_JDBC_URL+"',\n" +
                "   'table-name' = '"+tableName+"',\n" +
                "   'username' = '"+Constant.DORIS_USER+"',\n" +
                "   'password' = '"+Constant.DORIS_PASSWORD+"'\n" +
                ")";
    }
}
