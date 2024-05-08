package com.atguigu.edu.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.bean.TableProcessDwd;
import com.atguigu.edu.realtime.common.constant.Constant;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

/**
 * ClassName: FlinkSinkUtil
 * Package: com.atguigu.edu.realtime.common.util
 * Description:
 *
 * @Author: tubage
 * @Create: 2024/5/6 19:01
 * @Version: 1.0
 */
public class FlinkSinkUtil {
    // kafkaSink
    public static KafkaSink<String> getKafkaSink(String topic) {
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())

                        .build()
                )
                // .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE) // 1开启检查点，2 kafkaSink kafkaWriter 底层开启事务
                // .setTransactionalIdPrefix("xxx") //  3 事务ID
                // // 4 检查点成功后正式提交 检查点超时时间 < 事务超时时间 <= 事务最大超时时间(默认15分钟) // 5 消费端 读已提交
                // .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 15*60*1000 + "")
                .build();
        return kafkaSink;
    }

    // TODO dwd 事实表动态分流写入 自定义序列化
    public static KafkaSink<Tuple2<JSONObject, TableProcessDwd>> getKafkaSink( ) {
        KafkaSink<Tuple2<JSONObject, TableProcessDwd>> kafkaSink = KafkaSink.<Tuple2<JSONObject, TableProcessDwd>>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(
                        new KafkaRecordSerializationSchema<Tuple2<JSONObject, TableProcessDwd>>() {
                            @Nullable
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(Tuple2<JSONObject, TableProcessDwd> tuple2, KafkaSinkContext kafkaSinkContext, Long aLong) {
                                JSONObject jsonObj = tuple2.f0;
                                TableProcessDwd tableProcessDwd = tuple2.f1;

                                String topic = tableProcessDwd.getSinkTable();

                                return new ProducerRecord<>(topic,jsonObj.toJSONString().getBytes());
                            }
                        }
                )
                // .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE) // 1开启检查点，2 kafkaSink kafkaWriter 底层开启事务
                // .setTransactionalIdPrefix("xxx") //  3 事务ID
                // // 4 检查点成功后正式提交 检查点超时时间 < 事务超时时间 <= 事务最大超时时间(默认15分钟) // 5 消费端 读已提交
                // .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 15*60*1000 + "")
                .build();
        return kafkaSink;
    }

    // TODO dws 写入doris 连接器方式
    public static DorisSink<String> getDorisSink(String tableName){
        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true"); // 每行一条 json 数据

        DorisSink<String> sink = DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisOptions(DorisOptions.builder() // 设置 doris 的连接参数
                        .setFenodes(Constant.DORIS_FE_NODES)
                        .setTableIdentifier(Constant.DORIS_DATABASE +"." + tableName)
                        .setUsername("root")
                        .setPassword("aaaaaa")
                        .build())
                .setDorisExecutionOptions(DorisExecutionOptions.builder() // 执行参数
                        //.setLabelPrefix("doris-label")  // stream-load 导入的时候的 label 前缀
                        .disable2PC() // 开启两阶段提交后,labelPrefix 需要全局唯一,为了测试方便禁用两阶段提交
                        .setDeletable(false)
                        .setBufferCount(3) // 批次条数: 默认 3
                        .setBufferSize(8*1024) // 批次大小: 默认 1M
                        .setCheckInterval(3000) // 批次输出间隔   三个对批次的限制是或的关系
                        .setMaxRetries(3)
                        .setStreamLoadProp(props) // 设置 stream load 的数据格式 默认是 csv,根据需要改成 json
                        .build())
                .setSerializer(new SimpleStringSerializer())
                .build();
        return sink;
    }
    // TODO dws 获取DorisSink(JDBC方式)
    public static <T> SinkFunction<T> getDorisSinkByJDBC(String sql, Class<T> valueType) {
        SinkFunction<T> JDBCSinkToDoris = JdbcSink.sink(
                sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement ps, T value) throws SQLException {
                        try {
                            Field[] fields = valueType.getDeclaredFields();
                            int paramCount = ps.getParameterMetaData().getParameterCount();
                            for (int i = 0; i < paramCount; i++) {
                                fields[i].setAccessible(true);
                                Object fieldValue = fields[i].get(value);
                                ps.setObject(i + 1, fieldValue);
                            }
                        } catch (IllegalAccessException | SQLException e) {
                            e.printStackTrace();
                        }
                    }
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(Constant.DORIS_JDBC_URL)
                        .withDriverName(Constant.MYSQL_DRIVER)
                        .withUsername(Constant.DORIS_USER)
                        .withPassword(Constant.DORIS_PASSWORD)
                        .build()
        );
        return JDBCSinkToDoris;
    }

}
