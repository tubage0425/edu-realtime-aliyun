package com.atguigu.edu.realtime.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.bean.TableProcessDim;
import com.atguigu.edu.realtime.common.util.JDBCUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * ClassName: TableProcessFunction
 * Package: com.atguigu.edu.realtime.function
 * Description:
 *
 * @Author: tubage
 * @Create: 2024/5/6 16:05
 * @Version: 1.0
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>> {
    MapStateDescriptor<String, TableProcessDim> mapStateDescriptor;

    public TableProcessFunction(MapStateDescriptor mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    // 两条流顺序问题
    Map<String, TableProcessDim> configMap = new HashMap<>();
    @Override
    public void open(Configuration parameters) throws Exception {
        // JDBC 将配置表中的配置信息提前加载到程序中configMap 作用域每个并行度 封装JDBCUtil
        java.sql.Connection mysqlConn = JDBCUtil.getMysqlConnection();
        String sql = "select * from edu_config.table_process_dim";

        List<TableProcessDim> tableProcessDims = JDBCUtil.queryList(mysqlConn, sql, TableProcessDim.class, true);
        for (TableProcessDim tableProcessDim : tableProcessDims) {
            configMap.put(tableProcessDim.getSourceTable(),tableProcessDim);
        }

        JDBCUtil.closeMysqlConnection(mysqlConn);
    }

    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
        // 处理业务流：根据处理的表名到广播状态里获取配置信息，能拿到，说明是维度数据，
        // 向下游传递(主流Maxwell结构data数据、过滤不需要字段补充type)
        ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        String table = jsonObject.getString("table");

        TableProcessDim tableProcessDim = null;

        if((tableProcessDim = broadcastState.get(table)) != null
                ||(tableProcessDim = configMap.get(table)) != null) {
            JSONObject dataJsonObj = jsonObject.getJSONObject("data");
            // 过滤不需要的字段
            String sinkColumns = tableProcessDim.getSinkColumns();
            deleteNotNeedColumns(dataJsonObj, sinkColumns);
            // 补充type字段
            String type = jsonObject.getString("type");
            dataJsonObj.put("type", type);

            collector.collect(Tuple2.of(dataJsonObj, tableProcessDim));
        }

    }

    @Override
    public void processBroadcastElement(TableProcessDim tableProcessDim, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context context, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
        // 处理广播流 将流中配置信息放到广播状态中(k source表名 v tableProcessDim对象)
        BroadcastState<String, TableProcessDim> broadcastState = context.getBroadcastState(mapStateDescriptor);
        String op = tableProcessDim.getOp();
        String key = tableProcessDim.getSourceTable();

        if ("d".equals(op)) {
            broadcastState.remove(key);
            configMap.remove(key);
        } else {
            broadcastState.put(key, tableProcessDim);
            configMap.put(key,tableProcessDim);
        }

    }

    private static void deleteNotNeedColumns(JSONObject dataJsonObj, String sinkColumns) {
        List<String> columnList = Arrays.asList(sinkColumns.split(","));

        Set<Map.Entry<String, Object>> entries = dataJsonObj.entrySet();
        entries.removeIf(entry -> !columnList.contains(entry.getKey()));

    }
}
