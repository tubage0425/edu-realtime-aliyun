package com.atguigu.edu.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.bean.TableProcessDwd;
import com.atguigu.edu.realtime.common.constant.Constant;
import com.sun.tools.internal.jxc.ap.Const;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

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

}
