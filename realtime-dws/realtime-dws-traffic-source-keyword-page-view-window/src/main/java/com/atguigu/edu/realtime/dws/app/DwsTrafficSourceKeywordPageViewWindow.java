package com.atguigu.edu.realtime.dws.app;

import com.atguigu.edu.realtime.common.base.BaseSQLApp;
import com.atguigu.edu.realtime.common.constant.Constant;
import com.atguigu.edu.realtime.common.util.SQLUtil;
import com.atguigu.edu.realtime.dws.func.KeywordUDTF;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ClassName: DwsTrafficSourceKeywordPageViewWindow
 * Package: com.atguigu.edu.realtime.dws.app
 * Description:
 *
 * @Author: tubage
 * @Create: 2024/5/7 14:42
 * @Version: 1.0
 */
public class DwsTrafficSourceKeywordPageViewWindow extends BaseSQLApp {
    public static void main(String[] args) {
        new DwsTrafficSourceKeywordPageViewWindow().start(10021,4,"dws_traffic_source_keyword_page_view_window");
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        // TODO 注册自定义函数
        tableEnv.createTemporaryFunction("ik_analyze", KeywordUDTF.class);

        // TODO 从dwd页面日志读取数据（创建动态表、事件时间、水位线）
        tableEnv.executeSql("CREATE TABLE page_log (\n" +
                "  common map<string,string>,\n" +
                "  page map<string,string>,\n" +
                "  ts bigint,\n" +
                "  et AS TO_TIMESTAMP_LTZ(ts,3),\n" +
                "  WATERMARK FOR et AS et\n" +
                ") " + SQLUtil.getKafkaDDL(Constant.TOPIC_DWD_TRAFFIC_PAGE, "dws_traffic_source_keyword_page_view_window")

        );
        // TODO 过滤出搜索行为（page['last_page_id'] = 'search'
        //  and page['item_type'] = 'keyword' and page['item'] is not null）
        Table searchTable = tableEnv.sqlQuery("select\n" +
                "page['item'] fullword,\n" +
                "et\n" +
                "from page_log where \n" +
                "page['item_type'] = 'keyword' and page['item'] is not null;");
        tableEnv.createTemporaryView("search_table",searchTable);
        // tableEnv.executeSql("select * from search_table").print();

        // TODO 分词并和原表字段(时间)进行关联 Table function
        Table splitTable = tableEnv.sqlQuery("select\n" +
                "keyword,\n" +
                "et\n" +
                "from search_table,\n" +
                "lateral table(ik_analyze(fullword)) t(keyword)");
        tableEnv.createTemporaryView("split_table", splitTable);
        // tableEnv.executeSql("select * from split_table").print();

        // TODO 分组 开窗tvf 聚合
        Table resTable = tableEnv.sqlQuery("SELECT \n" +
                "DATE_FORMAT(window_start,'yyyy-MM-dd HH:mm:ss') stt,    \n" +
                "DATE_FORMAT(window_end,'yyyy-MM-dd HH:mm:ss') edt, \n" +
                "DATE_FORMAT(window_start,'yyyy-MM-dd') cur_date,\n'" +
                Constant.KEYWORD_SEARCH + "' source,\n" +
                "keyword, \n" +
                "count(*) keyword_count\n" +
                "  FROM TABLE(\n" +
                "    TUMBLE(TABLE split_table, DESCRIPTOR(et), INTERVAL '10' SECOND))\n" +
                "  GROUP BY keyword,window_start, window_end");
        tableEnv.createTemporaryView("res_table",resTable);
        // tableEnv.executeSql("select * from res_table").print();
        // TODO 结果写到Doris(doris gmall_realtime建库建表 + doris连接器(SQLUtil))
        tableEnv.executeSql("CREATE TABLE dws_traffic_source_keyword_page_view_window (\n" +
                "    stt string,\n" +
                "    edt string,\n" +
                "    cur_date string,\n" +
                "    source string,\n" +
                "    keyword string,\n" +
                "    keyword_count bigint\n" +
                "    ) " + SQLUtil.getDorisJDBCSinkDDL("dws_traffic_source_keyword_page_view_window"));
        resTable.executeInsert("dws_traffic_source_keyword_page_view_window");

    }
}
