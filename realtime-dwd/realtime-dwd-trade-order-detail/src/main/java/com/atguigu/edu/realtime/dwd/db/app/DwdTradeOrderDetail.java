package com.atguigu.edu.realtime.dwd.db.app;

import com.atguigu.edu.realtime.common.base.BaseSQLApp;
import com.atguigu.edu.realtime.common.constant.Constant;
import com.atguigu.edu.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * ClassName: DwdTradeOrderDetail
 * Package: com.atguigu.edu.realtime.dwd.db.app
 * Description:
 *
 * @Author: tubage
 * @Create: 2024/5/6 21:20
 * @Version: 1.0
 */
public class DwdTradeOrderDetail extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeOrderDetail().start(10013,4, Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 普通内外连接要设置失效时间
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(15L));
        // 读取业务数据，封装为 Flink SQL 表。
        readOdsDb(tEnv,Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
        // tEnv.executeSql("select * from topic_db");


        // 读取日志数据，封装为 Flink SQL 表。
        tEnv.executeSql("create table page_log(" +
                "`common` map<String, String>,\n" +
                "`page` map<String, String>,\n" +
                "`ts` String\n" +
                ")" + SQLUtil.getKafkaDDL(Constant.TOPIC_DWD_TRAFFIC_PAGE,Constant.TOPIC_DWD_TRADE_ORDER_DETAIL));
        // 筛选订单明细表数据。
        Table orderDetail = tEnv.sqlQuery("select \n" +
                "data['id'] id,\n" +
                "data['order_id'] order_id,\n" +
                "data['user_id'] user_id,\n" +
                "data['course_id'] course_id,\n" +
                "date_format(data['create_time'], 'yyyy-MM-dd') date_id,\n" +
                "data['create_time'] create_time,\n" +
                "data['origin_amount'] origin_amount,\n" +
                "data['coupon_reduce'] coupon_reduce,\n" +
                "data['final_amount'] final_amount,\n" +
                "ts\n" +
                "from `topic_db` where `table` = 'order_detail' " +
                "and `type` = 'insert'\n");
        tEnv.createTemporaryView("order_detail",orderDetail);
        // 筛选订单表数据。
        Table orderInfo = tEnv.sqlQuery("select \n" +
                "data['id'] id,\n" +
                "data['province_id'] province_id,\n" +
                "data['out_trade_no'] out_trade_no,\n" +
                "data['session_id'] session_id,\n" +
                "data['trade_body'] trade_body\n" +
                "from `topic_db`\n" +
                "where `table` = 'order_info'\n" +
                "and `type` = 'insert'");
        tEnv.createTemporaryView("order_info",orderInfo);
        // 筛选订单页中的引流来源信息。
        Table filteredLog = tEnv.sqlQuery("select " +
                "common['sid'] session_id,\n" +
                "common['sc'] source_id\n" +
                "from page_log\n" +
                "where page['page_id'] = 'order'");
        tEnv.createTemporaryView("filter_log",filteredLog);
        // TODO 关联三张表(内连接+外连接 有空消息和重复消息)
        Table result = tEnv.sqlQuery("select \n" +
                "od.id,\n" +
                "order_id,\n" +
                "user_id,\n" +
                "course_id,\n" +
                "province_id,\n" +
                "date_id,\n" +
                "oi.session_id,\n" +
                "source_id,\n" +
                "create_time,\n" +
                "origin_amount,\n" +
                "coupon_reduce coupon_reduce_amount,\n" +
                "final_amount,\n" +
                "out_trade_no,\n" +
                "trade_body,\n" +
                "ts,\n" +
                "current_row_timestamp() row_op_ts\n" +
                "from order_detail od\n" +
                "join order_info oi\n" +
                "on od.order_id = oi.id\n" +
                "left join filter_log fl\n" +
                "on oi.session_id = fl.session_id");
        tEnv.createTemporaryView("result_table", result);
        // tEnv.executeSql("select * from result_table")
        //         .print();

        // upsert kafka写入(创建动态表、写入)
        tEnv.executeSql("" +
                "create table dwd_trade_order_detail(\n" +
                "id string,\n" +
                "order_id string,\n" +
                "user_id string,\n" +
                "course_id string,\n" +
                "province_id string,\n" +
                "date_id string,\n" +
                "session_id string,\n" +
                "source_id string,\n" +
                "create_time string,\n" +
                "origin_amount string,\n" +
                "coupon_reduce_amount string,\n" +
                "final_amount string,\n" +
                "out_trade_no string,\n" +
                "trade_body string,\n" +
                "ts bigint,\n" +
                "row_op_ts TIMESTAMP_LTZ(3) ,\n" +
                "primary key(id) not enforced\n" +
                ")" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL));
        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);

    }

}
