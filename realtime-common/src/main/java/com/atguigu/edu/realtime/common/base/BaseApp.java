package com.atguigu.edu.realtime.common.base;

import com.atguigu.edu.realtime.common.constant.Constant;
import com.atguigu.edu.realtime.common.util.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: BaseApp
 * Package: com.atguigu.edu.realtime.common.base
 * Description:
 *
 * @Author: tubage
 * @Create: 2024/5/6 16:04
 * @Version: 1.0
 */
public abstract class BaseApp {
    public void start(int port, int parallelism, String ckAndGroupId, String topic) {
        // TODO 1 准备环境
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT,port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        // TODO 2 检查点相关设置
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        // CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // checkpointConfig.setCheckpointTimeout(60000L);
        // checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // checkpointConfig.setMinPauseBetweenCheckpoints(2000L);
        // env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        // env.setStateBackend(new HashMapStateBackend());
        // checkpointConfig.setCheckpointStorage(Constant.CHECKPOINT_PATH + ckAndGroupId);
        // System.setProperty("HADOOP_USER_NAME", "china");
        // TODO 3 从kafka topic_db主题读取数据
        String groupID = ckAndGroupId;
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(topic, groupID);
        DataStreamSource<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource");

        handle(env, kafkaStrDS);

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS);
}
