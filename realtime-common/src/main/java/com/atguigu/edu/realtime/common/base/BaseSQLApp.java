package com.atguigu.edu.realtime.common.base;

import com.atguigu.edu.realtime.common.constant.Constant;
import com.atguigu.edu.realtime.common.util.SQLUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ClassName: BaseSQLApp
 * Package: com.atguigu.edu.realtime.common.base
 * Description:
 *
 * @Author: tubage
 * @Create: 2024/5/6 21:21
 * @Version: 1.0
 */
public abstract class BaseSQLApp {
    // TODO flink sql基类
    public void start(int port, int parallelism, String ckAndGroupId) {
        // TODO 1 基本环境准备
        // 流处理环境
        // 设置并行度
        // 指定表执行环境
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT,port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(parallelism);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // TODO 2 检查点设置（开启、超时时间、是否保留、两个最小时间间隔、重启策略、状态后端、存储路径、用户）
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        // CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // checkpointConfig.setCheckpointTimeout(60000L);
        // checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // checkpointConfig.setMinPauseBetweenCheckpoints(2000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        // env.setStateBackend(new HashMapStateBackend());
        // checkpointConfig.setCheckpointStorage(Constant.CHECKPOINT_PATH + ckAndGroupId);
        // System.setProperty("HADOOP_USER_NAME", Constant.HADOOP_USER_NAME);

        handle(env, tableEnv);
    }

    protected abstract void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv);

    public static void readOdsDb(StreamTableEnvironment tableEnv, String groupId) {
        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "`database`  string,\n" +
                "`table`     string,\n" +
                "`type`      string,\n" +
                "`data`      MAP<string,string>,\n" +
                "`old`       MAP<string,string>,\n" +
                " ts         bigint,\n" +
                " pt as proctime(),\n" +
                " et as TO_TIMESTAMP_LTZ(ts,0),\n" +
                " WATERMARK FOR et AS et\n" +
                ") " + SQLUtil.getKafkaDDL(Constant.TOPIC_DB, groupId)
        ); // map类型 增加事件时间 处理时间字段 watermark kafka一些配置
        // tableEnv.executeSql("select * from topic_db")
        //         .print();
    }
}
