package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.edu.realtime.common.base.BaseSQLApp;
import com.atguigu.edu.realtime.common.constant.Constant;
import com.atguigu.edu.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ClassName: DwdTradeOrderPaySucDetail
 * Package: com.atguigu.gmall.realtime.dwd.db.app
 * Description:
 *
 * @Author: tubage
 * @Create: 2024/5/7 10:16
 * @Version: 1.0
 */
public class DwdTradeOrderPaySucDetail extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeOrderPaySucDetail().start(10014,4, Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        // TODO interval join实现
        // TODO 读取下单事实表数据
        tableEnv.executeSql("" +
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
                "row_op_ts TIMESTAMP_LTZ(3), " +
                "et as to_timestamp_ltz(ts, 0), " +
                "watermark for et as et - interval '3' second " +
                ")" + SQLUtil.getKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL, Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS));
        // TODO 从topic_db读取数据
        readOdsDb(tableEnv,Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);
        // TODO 过滤出支付成功
        Table paymentSuc = tableEnv.sqlQuery("select\n" +
                        "data['alipay_trade_no'] alipay_trade_no,\n" +
                        "data['trade_body'] trade_body,\n" +
                        "data['order_id'] order_id,\n" +
                        "data['payment_type'] payment_type,\n" +
                        "data['payment_status'] payment_status,\n" +
                        "data['callback_time'] callback_time,\n" +
                        "data['callback_content'] callback_content,\n" +
                        "`pt`," +
                        "ts, " +
                        "et " +
                        "from topic_db\n" +
                        "where `table` = 'payment_info'\n" +
                        // "and `type` = 'update'\n" +
                        "and data['payment_status']='1602'"
        );
        tableEnv.createTemporaryView("payment_suc", paymentSuc);

        // TODO 关联(interval join) 无需设置 ttl(事件时间进展到的时候会清掉状态) // 如果用普通内外连接需要ttl 15min+15s
        Table resultTable = tableEnv.sqlQuery("" +
                "select \n" +
                "id,\n" +
                "od.order_id,\n" +
                "user_id,\n" +
                "course_id,\n" +
                "province_id,\n" +
                "date_id,\n" +
                "alipay_trade_no,\n" +
                "payment_suc.trade_body,\n" +
                "payment_type,\n" +
                "payment_status,\n" +
                "callback_time,\n" +
                "callback_content,\n" +
                "origin_amount,\n" +
                "coupon_reduce_amount,\n" +
                "final_amount,\n" +
                "payment_suc.ts ts,\n" +
                "row_op_ts\n" +
                "from payment_suc  " +
                "join dwd_trade_order_detail od " +
                "on payment_suc.order_id=od.order_id " +
                "and od.et >= payment_suc.et - interval '30' minute " +
                "and od.et <= payment_suc.et + interval '5' second "
                );
        tableEnv.createTemporaryView("result_table", resultTable);

        // tableEnv.executeSql("select * from result_table")
        //         .print();
        // TODO 写到kafka主题(kafka connector 创建动态表)
        tableEnv.executeSql("create table dwd_trade_order_payment_success(\n" +
                "id string,\n" +
                "order_id string,\n" +
                "user_id string,\n" +
                "course_id string,\n" +
                "province_id string,\n" +
                "date_id string,\n" +
                "alipay_trade_no string,\n" +
                "trade_body string,\n" +
                "payment_type string,\n" +
                "payment_status string,\n" +
                "callback_time string,\n" +
                "callback_content string,\n" +
                "original_amount string,\n" +
                "coupon_reduce_amount string,\n" +
                "final_amount string,\n" +
                "ts bigint,\n" +
                "row_op_ts TIMESTAMP_LTZ(3) ,\n" +
                "primary key(id) not enforced" +
                ")" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS));
        resultTable.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);
    }
}
