package com.atguigu.edu.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.bean.TradeCourseOrderBean;
import com.atguigu.edu.realtime.common.constant.Constant;
import com.atguigu.edu.realtime.common.func.BeanToJsonStrMapFunction;
import com.atguigu.edu.realtime.common.func.DimAsyncFunction;
import com.atguigu.edu.realtime.common.func.DimMapFunction;
import com.atguigu.edu.realtime.common.util.DateFormatUtil;
import com.atguigu.edu.realtime.common.util.FlinkSinkUtil;
import com.atguigu.edu.realtime.common.util.HBaseUtil;
import com.atguigu.edu.realtime.common.util.RedisUtil;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * ClassName: DwsTradeCourseOrderWindow
 * Package: com.atguigu.edu.realtime.dws.app
 * Description:
 *
 * @Author: tubage
 * @Create: 2024/5/7 18:09
 * @Version: 1.0
 */
public class DwsTradeCourseOrderWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeCourseOrderWindow().start(10023,4,"dws_trade_course_order_window", Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // TODO 从dwd下单事实读取数据
        // TODO 1 处理空消息 JSONStr-JSONObject类型转换
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        if (StringUtils.isNotEmpty(s)) {
                            JSONObject jsonObj = JSON.parseObject(s);
                            collector.collect(jsonObj);
                        }
                    }
                }
        );

        // TODO 2 按照订单明细ID分组 去重（dwd 左外连接的page_log）
        KeyedStream<JSONObject, String> orderDetailKeyedDS = jsonObjDS.keyBy(jsonObject -> jsonObject.getString("id"));

        SingleOutputStreamOperator<JSONObject> distinctDS = orderDetailKeyedDS.process(
                new ProcessFunction<JSONObject, JSONObject>() {
                    ValueState<JSONObject> lastJsonObjState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<JSONObject> valueStateDescriptor = new ValueStateDescriptor<JSONObject>(
                                "lastJsonObjState", JSONObject.class
                        );
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10L)).build());
                        lastJsonObjState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        // 状态+抵消
                        JSONObject lastJsonObj = lastJsonObjState.value();
                        // 第2条数据来 度量值取反
                        if(lastJsonObj != null) {
                            String finalAmount = jsonObject.getString("final_amount");
                            lastJsonObj.put("final_amount", "-" + finalAmount);

                            collector.collect(lastJsonObj);
                        }

                        // 第1条数据来的时候 直接从这开始走，第2条数据过来也会走这
                        lastJsonObjState.update(jsonObject);
                        collector.collect(jsonObject);


                    }
                }
        );

        // distinctDS.print();


        // TODO 3 指定waterMark 事件时间
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = orderDetailKeyedDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObject, long l) {
                                        return jsonObject.getLong("ts") * 1000;
                                    }
                                }
                        )
        );
        // TODO 4 流中数据JSONObj封装为统计的实体Bean
        SingleOutputStreamOperator<TradeCourseOrderBean> beanDS = withWatermarkDS.map(
                new MapFunction<JSONObject, TradeCourseOrderBean>() {
                    @Override
                    public TradeCourseOrderBean map(JSONObject jsonObject) throws Exception {
                        String courseId = jsonObject.getString("course_id");
                        BigDecimal finalAmount = jsonObject.getBigDecimal("final_amount");
                        Long ts = jsonObject.getLong("ts") * 1000;

                        TradeCourseOrderBean tradeCourseOrderBean = TradeCourseOrderBean.builder()
                                .courseId(courseId)
                                .orderTotalAmount(finalAmount)
                                .ts(ts)
                                .build();
                        return tradeCourseOrderBean;
                    }
                }
        );
        // TODO 5 按照统计维度courseID分组
        KeyedStream<TradeCourseOrderBean, String> keyedDS = beanDS.keyBy(TradeCourseOrderBean::getCourseId);

        // TODO 6 开窗
        WindowedStream<TradeCourseOrderBean, String, TimeWindow> windowDS = keyedDS.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)));
        // TODO 7 聚合
        SingleOutputStreamOperator<TradeCourseOrderBean> reduceDS = windowDS.reduce(
                new ReduceFunction<TradeCourseOrderBean>() {
                    @Override
                    public TradeCourseOrderBean reduce(TradeCourseOrderBean t1, TradeCourseOrderBean t2) throws Exception {
                        t1.setOrderTotalAmount(t1.getOrderTotalAmount().add(t2.getOrderTotalAmount()));
                        return t1;
                    }
                },
                new WindowFunction<TradeCourseOrderBean, TradeCourseOrderBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow timeWindow, Iterable<TradeCourseOrderBean> iterable, Collector<TradeCourseOrderBean> collector) throws Exception {
                        TradeCourseOrderBean tradeCourseOrderBean = iterable.iterator().next();

                        String stt = DateFormatUtil.tsToDateTime(timeWindow.getStart());
                        String edt = DateFormatUtil.tsToDateTime(timeWindow.getEnd());
                        String curDate = DateFormatUtil.tsToDate(timeWindow.getStart());
                        tradeCourseOrderBean.setStt(stt);
                        tradeCourseOrderBean.setEdt(edt);
                        tradeCourseOrderBean.setCurDate(curDate);

                        collector.collect(tradeCourseOrderBean);
                    }
                }
        );

        // reduceDS.print();

        // TODO 8 关联course维度(基础方式)
        // TODO 关联course维度（旁路缓存）
        /*
        SingleOutputStreamOperator<TradeCourseOrderBean> withCourseDS = reduceDS.map(
                new RichMapFunction<TradeCourseOrderBean, TradeCourseOrderBean>() {
                    Connection hbaseConn;
                    Jedis jedis;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        jedis = RedisUtil.getJedis();
                        hbaseConn = HBaseUtil.getHbaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        RedisUtil.closeJedis(jedis);
                        HBaseUtil.closeHbaseConnection(hbaseConn);
                    }

                    @Override
                    public TradeCourseOrderBean map(TradeCourseOrderBean tradeCourseOrderBean) throws Exception {
                        // 获取维度主键 先从Redis查 查不到再从HBASE查，查完写入HBASE（写入维度的时候，清空Redis 一致性）
                        String courseId = tradeCourseOrderBean.getCourseId();
                        JSONObject dimJsonObj = RedisUtil.readDim(jedis, "dim_course_info", courseId);

                        if(dimJsonObj != null) {
                            // 缓存命中
                            System.out.println("从Redis缓存中获取到sku数据");
                        }else {
                            dimJsonObj = HBaseUtil.getRow(hbaseConn,Constant.HBASE_NAMESPACE,"dim_course_info",courseId, JSONObject.class);

                            if(dimJsonObj != null) {
                                System.out.println("从HBase中获取到sku数据");
                                RedisUtil.writeDim(jedis,"dim_course_info",courseId,dimJsonObj);
                            }else {
                                System.out.println("没有找到维度数据");
                            }
                        }

                        // 从维度对象中补充数据
                        tradeCourseOrderBean.setCourseName(dimJsonObj.getString("course_name"));
                        tradeCourseOrderBean.setSubjectId(dimJsonObj.getString("subject_id"));

                        return tradeCourseOrderBean;
                    }
                }
        );

         */
        // withCourseDS.print();

        // TODO 关联course维度（旁路缓存+抽取模版）
        /*
        SingleOutputStreamOperator<TradeCourseOrderBean> withCourseDS = reduceDS.map(
                new DimMapFunction<TradeCourseOrderBean>() {
                    @Override
                    public void addDims(TradeCourseOrderBean tradeCourseOrderBean, JSONObject dimJsonObj) {
                        tradeCourseOrderBean.setCourseName(dimJsonObj.getString("course_name"));
                        tradeCourseOrderBean.setSubjectId(dimJsonObj.getString("subject_id"));
                    }

                    @Override
                    public String getRowKey(TradeCourseOrderBean obj) {
                        return obj.getCourseId();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_course_info";
                    }
                }
        );

         */
        // withCourseDS.print();
        // TODO 关联course维度（异步IO优化）
        /*
        SingleOutputStreamOperator<TradeCourseOrderBean> withCourseDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new RichAsyncFunction<TradeCourseOrderBean, TradeCourseOrderBean>() {
                    private AsyncConnection hbaseConnAsync;
                    private StatefulRedisConnection<String,String> redisAsyncConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConnAsync = HBaseUtil.getHbaseConnectionAsync();
                        redisAsyncConn = RedisUtil.getAsyncRedisConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHbaseConnectionAsync(hbaseConnAsync);
                        RedisUtil.closeAsyncRedisConnection(redisAsyncConn);
                    }
                    @Override
                    public void asyncInvoke(TradeCourseOrderBean tradeCourseOrderBean, ResultFuture<TradeCourseOrderBean> resultFuture) throws Exception {
                        // 获取维度主键 先从Redis查 查不到再从HBASE查，查完写入HBASE（写入维度的时候，清空Redis 一致性）
                        String courseId = tradeCourseOrderBean.getCourseId();
                        JSONObject dimJsonObj = RedisUtil.readDimAsync(redisAsyncConn, "dim_course_info", courseId);

                        if(dimJsonObj != null) {
                            // 缓存命中
                            System.out.println("从Redis缓存中获取到sku数据");
                        }else {
                            dimJsonObj = HBaseUtil.getRowAsync(hbaseConnAsync,Constant.HBASE_NAMESPACE,"dim_course_info",courseId);

                            if(dimJsonObj != null) {
                                System.out.println("从HBase中获取到sku数据");
                                RedisUtil.writeDimAsync(redisAsyncConn,"dim_course_info",courseId,dimJsonObj);
                            }else {
                                System.out.println("没有找到维度数据");
                            }
                        }

                        // 从维度对象中补充数据
                        tradeCourseOrderBean.setCourseName(dimJsonObj.getString("course_name"));
                        tradeCourseOrderBean.setSubjectId(dimJsonObj.getString("subject_id"));

                        resultFuture.complete(Collections.singleton(tradeCourseOrderBean));
                    }
                },
                60,
                TimeUnit.SECONDS
        );

         */
        // TODO 关联course维度（异步IO flink API+抽取模板优化）
        SingleOutputStreamOperator<TradeCourseOrderBean> withCourseDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new DimAsyncFunction<TradeCourseOrderBean>() {
                    @Override
                    public void addDims(TradeCourseOrderBean tradeCourseOrderBean, JSONObject dimJsonObj) {
                        tradeCourseOrderBean.setCourseName(dimJsonObj.getString("course_name"));
                        tradeCourseOrderBean.setSubjectId(dimJsonObj.getString("subject_id"));
                    }

                    @Override
                    public String getRowKey(TradeCourseOrderBean obj) {
                        return obj.getCourseId();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_course_info";
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        // 关联subject
        SingleOutputStreamOperator<TradeCourseOrderBean> withSubjectDS = AsyncDataStream.unorderedWait(
                withCourseDS,
                new DimAsyncFunction<TradeCourseOrderBean>() {
                    @Override
                    public void addDims(TradeCourseOrderBean tradeCourseOrderBean, JSONObject dimJsonObj) {
                        tradeCourseOrderBean.setSubjectName(dimJsonObj.getString("subject_name"));
                        tradeCourseOrderBean.setCategoryId(dimJsonObj.getString("category_id"));
                    }

                    @Override
                    public String getRowKey(TradeCourseOrderBean obj) {
                        return obj.getSubjectId();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_subject_info";
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        // 关联category
        SingleOutputStreamOperator<TradeCourseOrderBean> withCatDS = AsyncDataStream.unorderedWait(
                withSubjectDS,
                new DimAsyncFunction<TradeCourseOrderBean>() {
                    @Override
                    public void addDims(TradeCourseOrderBean tradeCourseOrderBean, JSONObject dimJsonObj) {
                        tradeCourseOrderBean.setCategoryName(dimJsonObj.getString("category_name"));
                    }

                    @Override
                    public String getRowKey(TradeCourseOrderBean obj) {
                        return obj.getCategoryId();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category_info";
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        // withCatDS.print();

        // TODO 14 写入Doris(建好表)
        withCatDS
                .map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_course_order_window"));

    }
}
