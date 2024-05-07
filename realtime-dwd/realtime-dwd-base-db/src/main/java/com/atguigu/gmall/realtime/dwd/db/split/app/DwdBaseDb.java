package com.atguigu.gmall.realtime.dwd.db.split.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.bean.TableProcessDwd;
import com.atguigu.edu.realtime.common.constant.Constant;
import com.atguigu.edu.realtime.common.util.FlinkSinkUtil;
import com.atguigu.edu.realtime.common.util.FlinkSourceUtil;
import com.atguigu.gmall.realtime.dwd.db.split.func.BaseDbTableProcessFunction;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * ClassName: DwdBaseDb
 * Package: com.atguigu.gmall.realtime.dwd.db.split.app
 * Description:
 *
 * @Author: tubage
 * @Create: 2024/5/7 11:14
 * @Version: 1.0
 */
public class DwdBaseDb extends BaseApp {
    public static void main(String[] args) {
        new DwdBaseDb().start(10015,4,"dwd_base_db",
                Constant.TOPIC_DB);
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // TODO 1 topic_db 读取数据类型转换 JSON字符串转为JSON对象，简单etl清洗（脏数据直接不要）
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        try {
                            JSONObject jsonObj = JSON.parseObject(s);
                            String type = jsonObj.getString("type");
                            if (!type.equals("bootstrap-")) {
                                collector.collect(jsonObj);
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
        );
        // TODO 2 flink cdc读取配置表中配置信息（见文档）封装为流
        MySqlSource<String> mysqlSource = FlinkSourceUtil.getMysqlSource("edu_config", "edu_config.table_process_dwd");
        DataStreamSource<String> mysqlStrDS = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "mysqlSource");
        // TODO 2 对流中数据类型转换、封装实体类对象
        /*
        {"before":null,
        "after":{"id":1,"name":"zs","age":18},
        "source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"gmall_config","sequence":null,"table":"t_user","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},
        "op":"r",
        "ts_ms":1713265333564,
        "transaction":null}
         */
        SingleOutputStreamOperator<TableProcessDwd> tableProcessDS = mysqlStrDS.map(
                new MapFunction<String, TableProcessDwd>() {
                    @Override
                    public TableProcessDwd map(String s) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(s);
                        String op = jsonObj.getString("op");
                        TableProcessDwd tableProcessDwd = null;
                        if ("d".equals(op)) {
                            tableProcessDwd = jsonObj.getObject("before", TableProcessDwd.class);
                        } else {
                            tableProcessDwd = jsonObj.getObject("after", TableProcessDwd.class);
                        }
                        tableProcessDwd.setOp(op);
                        return tableProcessDwd;
                    }
                }
        );
        // TODO 3 广播配置表
        MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor = new MapStateDescriptor<String, TableProcessDwd>(
                "mapStateDescriptor", String.class, TableProcessDwd.class
        );
        BroadcastStream<TableProcessDwd> broadcastDS = tableProcessDS.broadcast(mapStateDescriptor);
        // TODO 4 connect
        BroadcastConnectedStream<JSONObject, TableProcessDwd> connectDS = jsonObjDS.connect(broadcastDS);
        // TODO 5 process 使用匿名内部类会报错 序列化问题, 提出来到func
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> resultDS = connectDS.process(
                new BaseDbTableProcessFunction(mapStateDescriptor)
        );
        // TODO 6 流中数据写入kafka不同主题(自定义序列化)
        resultDS.sinkTo(FlinkSinkUtil.getKafkaSink());

    }
}
