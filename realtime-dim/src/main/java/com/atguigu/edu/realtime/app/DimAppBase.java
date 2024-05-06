package com.atguigu.edu.realtime.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.bean.TableProcessDim;
import com.atguigu.edu.realtime.common.constant.Constant;
import com.atguigu.edu.realtime.common.util.HBaseUtil;
import com.atguigu.edu.realtime.common.util.JDBCUtil;
import com.atguigu.edu.realtime.function.HBaseSinkFunction;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import jdk.nashorn.internal.scripts.JD;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;

/**
 * ClassName: DimAppBase
 * Package: com.atguigu.edu.realtime.app
 * Description:
 *
 * @Author: tubage
 * @Create: 2024/5/6 13:43
 * @Version: 1.0
 */
public class DimAppBase {
    public static void main(String[] args) throws Exception {
        // TODO 1 准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        // TODO 2 检查点相关设置
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        // CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // checkpointConfig.setCheckpointTimeout(60000L);
        // checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // checkpointConfig.setMinPauseBetweenCheckpoints(2000L);
        // env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        // env.setStateBackend(new HashMapStateBackend());
        // checkpointConfig.setCheckpointStorage(Constant.CHECKPOINT_PATH);
        // System.setProperty("HADOOP_USER_NAME", "china");
        // TODO 3 从kafka topic_db主题读取数据
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setTopics(Constant.TOPIC_DB)
                .setGroupId("dim_app_group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(
                        new DeserializationSchema<String>() {
                            @Override
                            public String deserialize(byte[] bytes) throws IOException {
                                if(bytes != null) {
                                    return new String(bytes);
                                }
                                return null;
                            }

                            @Override
                            public boolean isEndOfStream(String s) {
                                return false;
                            }

                            @Override
                            public TypeInformation<String> getProducedType() {
                                return TypeInformation.of(String.class);
                            }
                        }
                )
                .build();

        DataStreamSource<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource");

        // TODO 4 对kafkaSource中数据做类型转换
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        try {
                            JSONObject jsonObj = JSON.parseObject(s);
                            String db = jsonObj.getString("database");
                            String type = jsonObj.getString("type");
                            String data = jsonObj.getString("data");

                            // ETL
                            if("edu".equals(db)
                            && ("insert".equals(type)
                                    || "update".equals(type)
                                    || "delete".equals(type)
                                    || "bootstrap-insert".equals(type))
                            && data != null
                            && data.length() > 2) {
                                collector.collect(jsonObj);
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
        );
        // jsonObjDS.print();

        // TODO 5 flink cdc读取配置表中的配置信息
        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");
        MySqlSource<String> mysqlSource = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .databaseList("edu_config")
                .tableList("edu_config.table_process_dim")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .jdbcProperties(props)
                .build();
        DataStreamSource<String> mysqlStrDS = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "mysqlSource")
                .setParallelism(1);

        // mysqlStrDS.print();

        // TODO 6 对流中数据做类型转换TableProcessDim对象（根据op类型 crud 看从before还是after获取数据）
        SingleOutputStreamOperator<TableProcessDim> tableProcessDS = mysqlStrDS.map(
                new MapFunction<String, TableProcessDim>() {
                    @Override
                    public TableProcessDim map(String s) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(s);
                        String op = jsonObj.getString("op");
                        TableProcessDim tableProcessDim = null;

                        if ("d".equals(op)) {
                            tableProcessDim = jsonObj.getObject("before", TableProcessDim.class);
                        } else {
                            tableProcessDim = jsonObj.getObject("after", TableProcessDim.class);
                        }
                        tableProcessDim.setOp(op);
                        return tableProcessDim;
                    }
                }
        ).setParallelism(1);
        // tableProcessDS.print();

        // TODO 7 根据操作类型以及配置表中的配置信息，在hbase建表，删表
        tableProcessDS = tableProcessDS.map(
                new RichMapFunction<TableProcessDim, TableProcessDim>() {
                    Connection hbaseConn = null;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = HBaseUtil.getHbaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHbaseConnection(hbaseConn);
                    }

                    @Override
                    public TableProcessDim map(TableProcessDim tableProcessDim) throws Exception {
                        String op = tableProcessDim.getOp();
                        // 根据op类型 确定是建表还是删表（需要表名、列族）cr建表 d删表 u先删表再建表
                        String sinkTable = tableProcessDim.getSinkTable();
                        String[] cfs = tableProcessDim.getSinkFamily().split(",");

                        if("c".equals(op) || "r".equals(op)) {
                            HBaseUtil.createHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable,cfs);
                        }else if("d".equals(op)) {
                            HBaseUtil.dropHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable);
                        }else {
                            HBaseUtil.dropHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable);
                            HBaseUtil.createHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable,cfs);
                        }
                        return tableProcessDim;
                    }
                }
        );
        // TODO 8 广播配置流中数据
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor = new MapStateDescriptor<>("mapStateDescriptor", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastDS = tableProcessDS.broadcast(mapStateDescriptor);
        // TODO 9 主流与广播流关联
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonObjDS.connect(broadcastDS);
        // TODO 10 对关联后的数据处理process
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> processDS = connectDS.process(
                new BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>() {
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
                }
        );
        // processDS.print();
        // TODO 11 将流中维度数据向Hbase中同步
        processDS.addSink(new HBaseSinkFunction());

        env.execute();
    }

    private static void deleteNotNeedColumns(JSONObject dataJsonObj, String sinkColumns) {
        List<String> columnList = Arrays.asList(sinkColumns.split(","));

        Set<Map.Entry<String, Object>> entries = dataJsonObj.entrySet();
        entries.removeIf(entry -> !columnList.contains(entry.getKey()));

    }

}
