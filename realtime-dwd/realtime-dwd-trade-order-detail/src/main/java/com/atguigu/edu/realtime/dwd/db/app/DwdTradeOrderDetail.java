package com.atguigu.edu.realtime.dwd.db.app;

import com.atguigu.edu.realtime.common.base.BaseSQLApp;
import com.atguigu.edu.realtime.common.constant.Constant;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

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
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {

    }
}
