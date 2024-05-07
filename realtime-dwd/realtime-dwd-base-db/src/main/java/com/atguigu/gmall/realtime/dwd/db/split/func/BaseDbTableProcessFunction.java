package com.atguigu.gmall.realtime.dwd.db.split.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.bean.TableProcessDwd;
import com.atguigu.edu.realtime.common.util.JDBCUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.sql.Connection;
import java.util.*;

/**
 * ClassName: BaseDbTableProcessFunction
 * Package: com.atguigu.gmall.realtime.dwd.db.split.func
 * Description:
 *
 * @Author: tubage
 * @Create: 2024/5/7 11:34
 * @Version: 1.0
 */
public class BaseDbTableProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject,TableProcessDwd>> {
    MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor;
    Map<String,TableProcessDwd> configMap =  new HashMap<>();

    @Override
    public void open(Configuration parameters) throws Exception {
        // 解决两条流顺序问题
        Connection mysqlConn = JDBCUtil.getMysqlConnection();
        String sql = "select * from edu_config.table_process_dwd";
        List<TableProcessDwd> tableProcessDwds = JDBCUtil.queryList(mysqlConn, sql, TableProcessDwd.class, true);
        for (TableProcessDwd tableProcessDwd : tableProcessDwds) {
            String sourceTable = tableProcessDwd.getSourceTable();
            String sourceType = tableProcessDwd.getSourceType();
            String key = getKey(sourceTable,sourceType);
            configMap.put(key, tableProcessDwd);
        }
        JDBCUtil.closeMysqlConnection(mysqlConn);
    }

    public BaseDbTableProcessFunction(MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject, TableProcessDwd>> collector) throws Exception {
        ReadOnlyBroadcastState<String, TableProcessDwd> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        // 主流：根据表名和type 作为key看状态里是否有、有就要向下传递、过滤掉不需要的字段、补充ts
        String table = jsonObject.getString("table");
        String type = jsonObject.getString("type");
        String key = getKey(table,type);
        TableProcessDwd tableProcessDwd = null;

        if((tableProcessDwd = broadcastState.get(key)) != null
        || (tableProcessDwd = configMap.get(key)) != null) {

            JSONObject dataJsonObj = jsonObject.getJSONObject("data");
            String sinkColumns = tableProcessDwd.getSinkColumns();
            deleteNotNeedColumns(dataJsonObj,sinkColumns);

            Long ts = jsonObject.getLong("ts");
            dataJsonObj.put("ts", ts);// maxwell是秒级

            collector.collect(Tuple2.of(dataJsonObj,tableProcessDwd));
        }
    }

    @Override
    public void processBroadcastElement(TableProcessDwd tableProcessDwd, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.Context context, Collector<Tuple2<JSONObject, TableProcessDwd>> collector) throws Exception {
        BroadcastState<String, TableProcessDwd> broadcastState = context.getBroadcastState(mapStateDescriptor);
        String op = tableProcessDwd.getOp();

        String sourceTable = tableProcessDwd.getSourceTable();
        String sourceType = tableProcessDwd.getSourceType();
        String key = getKey(sourceTable,sourceType);

        if("d".equals(op)) {
            broadcastState.remove(key);
            configMap.remove(key);
        }else {
            broadcastState.put(key,tableProcessDwd);
            configMap.put(key,tableProcessDwd);
        }

    }

    private String getKey(String sourceTable, String sourceType) {
        return sourceTable + ":" + sourceType;
    }

    private static void deleteNotNeedColumns(JSONObject dataJsonObj, String sinkColumns) {
        List<String> columnList = Arrays.asList(sinkColumns.split(","));

        Set<Map.Entry<String, Object>> entries = dataJsonObj.entrySet();
        entries.removeIf(entry -> !columnList.contains(entry.getKey()));

    }
}
