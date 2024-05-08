package com.atguigu.edu.realtime.dwd.login.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.bean.DwdUserUserLoginBean;
import com.atguigu.edu.realtime.common.constant.Constant;
import com.atguigu.edu.realtime.common.util.DateFormatUtil;
import com.atguigu.edu.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * ClassName: DwdUserUserLogin
 * Package: com.atguigu.edu.realtime.dwd.login.app
 * Description:
 *
 * @Author: tubage
 * @Create: 2024/5/6 20:18
 * @Version: 1.0
 */
public class DwdUserUserLogin extends BaseApp {
    public static void main(String[] args) {
        new DwdUserUserLogin().start(10012,4, Constant.TOPIC_DWD_USER_LOGIN,Constant.TOPIC_DWD_TRAFFIC_PAGE);
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // TODO 类型转换以及过滤uid不为空
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(
                new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String s) throws Exception {
                        try {
                            JSONObject jsonObj = JSON.parseObject(s);
                            if (jsonObj.getJSONObject("common").getString("uid") != null) {
                                return jsonObj;
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                        return null;
                    }
                }
        );
        // jsonObjDS.print();




        // TODO 水位线
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = jsonObjDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObject, long l) {
                                        return jsonObject.getLong("ts");
                                    }
                                }
                        )
        );
        // TODO 会话ID分组
        KeyedStream<JSONObject, String> keyedDS = withWatermarkDS.keyBy(
                new KeySelector<JSONObject, String>() {
                    @Override
                    public String getKey(JSONObject jsonObject) throws Exception {
                        return jsonObject.getJSONObject("common").getString("mid");
                    }
                }
        );

        // TODO 状态编程
        SingleOutputStreamOperator<JSONObject> processDS = keyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<JSONObject> firstLoginDtState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<JSONObject> valueStateDescriptor = new ValueStateDescriptor<>(
                                "firstLoginDtState", JSONObject.class
                        );
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1L))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .build());
                        firstLoginDtState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        JSONObject firstLoginDt = firstLoginDtState.value();
                        Long ts = jsonObject.getLong("ts");

                        if (firstLoginDt == null) {
                            firstLoginDtState.update(jsonObject);
                            // 第一条数据到的时候开启定时器
                            context.timerService().registerEventTimeTimer(ts + 3 * 1000L);
                        } else {
                            Long lastTs = firstLoginDt.getLong("ts");
                            if (ts < lastTs) {
                                firstLoginDtState.update(jsonObject);
                            }
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                        out.collect(firstLoginDtState.value());
                    }
                }
        );
        // TODO 转换结构
        SingleOutputStreamOperator<String> beanDS = processDS.map(
                new MapFunction<JSONObject, String>() {
                    @Override
                    public String map(JSONObject jsonObject) throws Exception {

                        JSONObject common = jsonObject.getJSONObject("common");
                        Long ts = jsonObject.getLong("ts");
                        String loginTime = DateFormatUtil.tsToDateTime(ts);
                        String dateId = loginTime.substring(0, 10);
                        DwdUserUserLoginBean dwdUserUserLoginBean = DwdUserUserLoginBean.builder()
                                .userId(common.getString("uid"))
                                .dateId(dateId)
                                .loginTime(loginTime)
                                .channel(common.getString("ch"))
                                .provinceId(common.getString("ar"))
                                .versionCode(common.getString("vc"))
                                .midId(common.getString("mid"))
                                .brand(common.getString("ba"))
                                .model(common.getString("md"))
                                .sourceId(common.getString("sc"))
                                .operatingSystem(common.getString("os"))
                                .ts(ts)
                                .build();

                        return JSON.toJSONString(dwdUserUserLoginBean);
                    }
                }
        );

        beanDS.print();

        // TODO 写出kafka
        KafkaSink<String> kafkaSink = FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_USER_LOGIN);
        beanDS.sinkTo(kafkaSink);
    }
}
