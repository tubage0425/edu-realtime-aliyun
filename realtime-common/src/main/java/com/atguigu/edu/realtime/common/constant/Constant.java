package com.atguigu.edu.realtime.common.constant;

/**
 * ClassName: Constant
 * Package: com.atguigu.edu.realtime.common.constant
 * Description:
 *
 * @Author: tubage
 * @Create: 2024/5/6 11:25
 * @Version: 1.0
 */
public class Constant {
    // public static final String CHECKPOINT_PATH  = "hdfs://hadoop102:8020/checkPoint/edu_realtime";
    public static final String CHECKPOINT_PATH  = "hdfs://localhost:18020/checkPoint/edu_realtime/";
    // public static final String KAFKA_BROKERS = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    public static final String KAFKA_BROKERS = "localhost:9092,localhost:9093,localhost:9094";
    // public static final String ZK_BROKERS = "hadoop102,hadoop103,hadoop104:2181";
    public static final String ZK_BROKERS = "localhost:2181";

    public static final String DORIS_FE_NODES = "hadoop102:7030,hadoop103:7030,hadoop104:7030";
    // public static final String DORIS_JDBC_URL = "jdbc:mysql://hadoop102:9030/"+Constant.DORIS_DATABASE+"?useUnicode=true&characterEncoding=UTF-8";
    public static final String DORIS_JDBC_URL = "jdbc:mysql://localhost:19030/"+Constant.DORIS_DATABASE+"?useUnicode=true&characterEncoding=UTF-8";
    public static final String DORIS_USER = "root";
    public static final String DORIS_PASSWORD = "aaaaaa";
    public static final String DORIS_DATABASE = "edu_realtime";
    // public static final String JEDIS_HOST = "hadoop102";
    public static final String JEDIS_HOST = "localhost";
    // public static final int JEDIS_PORT = 6379;
    public static final int JEDIS_PORT = 16379;
    // public static final String REDIS_URL = "redis://hadoop102:6379/0";
    public static final String REDIS_URL = "redis://localhost:16379/0";

    public static final String HADOOP_USER_NAME = "china";

    public static final String TOPIC_DB = "topic_db";
    public static final String TOPIC_LOG = "topic_log";

    // public static final String MYSQL_HOST = "hadoop102";
    public static final String MYSQL_HOST = "localhost";
    // public static final int MYSQL_PORT = 3306;
    public static final int MYSQL_PORT = 13306;
    public static final String MYSQL_USER_NAME = "root";
    public static final String MYSQL_PASSWORD = "123456";
    public static final String HBASE_NAMESPACE = "edu";

    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    public static final String MYSQL_URL = "jdbc:mysql://localhost:13306?useSSL=false";
    // public static final String MYSQL_URL = "jdbc:mysql://hadoop102:3306?useSSL=false";

    public static final String TOPIC_DWD_TRAFFIC_START = "dwd_traffic_start";
    public static final String TOPIC_DWD_TRAFFIC_ERR = "dwd_traffic_err";
    public static final String TOPIC_DWD_TRAFFIC_PAGE = "dwd_traffic_page";
    public static final String TOPIC_DWD_TRAFFIC_ACTION = "dwd_traffic_action";
    public static final String TOPIC_DWD_TRAFFIC_DISPLAY = "dwd_traffic_display";
    public static final String TOPIC_DWD_TRAFFIC_APPVIDEO = "dwd_traffic_appVideo";

    public static final String TOPIC_DWD_TRADE_CART_ADD = "dwd_trade_cart_add";
    public static final String TOPIC_DWD_TRADE_ORDER_DETAIL = "dwd_trade_order_detail";
    public static final String TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS = "dwd_trade_order_payment_success";

    public static final String TOPIC_DWD_INTERACTION_COURSE_COMMENT_INFO = "dwd_interaction_course_comment_info";

    // public static final String TOPIC_DWD_TRADE_ORDER_CANCEL = "dwd_trade_order_cancel_detail";
    // public static final String TOPIC_DWD_TRADE_ORDER_REFUND = "dwd_trade_order_refund";
    // public static final String TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS = "dwd_trade_refund_payment_success";

    public static final String TOPIC_DWD_USER_REGISTER = "dwd_user_register";
    public static final String TOPIC_DWD_USER_LOGIN = "dwd_user_login";

}
