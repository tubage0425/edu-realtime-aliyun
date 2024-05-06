package com.atguigu.edu.realtime.common.util;

import com.atguigu.edu.realtime.common.constant.Constant;
import com.sun.tools.internal.jxc.ap.Const;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;

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
}
