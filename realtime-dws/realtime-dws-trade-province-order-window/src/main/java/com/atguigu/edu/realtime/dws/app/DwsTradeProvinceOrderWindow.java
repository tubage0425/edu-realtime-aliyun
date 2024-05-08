package com.atguigu.edu.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.bean.DwsTradeProvinceOrderBean;
import com.atguigu.edu.realtime.common.constant.Constant;
import com.atguigu.edu.realtime.common.func.BeanToJsonStrMapFunction;
import com.atguigu.edu.realtime.common.func.DimAsyncFunction;
import com.atguigu.edu.realtime.common.util.DateFormatUtil;
import com.atguigu.edu.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;

/**
 * ClassName: DwsTradeProvinceOrderWindow
 * Package: com.atguigu.edu.realtime.dws.app
 * Description:
 *
 * @Author: tubage
 * @Create: 2024/5/7 20:54
 * @Version: 1.0
 */
public class DwsTradeProvinceOrderWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeProvinceOrderWindow().start(10024,4,"dws_trade_province_order_window", Constant.TOPIC_DB);
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // TODO topic db读数据
        // TODO 筛选订单表 转换结构Bean
        SingleOutputStreamOperator<DwsTradeProvinceOrderBean> beanDS = kafkaStrDS.process(
                new ProcessFunction<String, DwsTradeProvinceOrderBean>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, DwsTradeProvinceOrderBean>.Context context, Collector<DwsTradeProvinceOrderBean> collector) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(s);
                        String table = jsonObj.getString("table");
                        String type = jsonObj.getString("type");
                        Long ts = jsonObj.getLong("ts");

                        if ("order_info".equals(table) &&
                                "insert".equals(type)) {
                            JSONObject dataJsonObj = jsonObj.getJSONObject("data");
                            String userId = dataJsonObj.getString("user_id");
                            String provinceId = dataJsonObj.getString("province_id");
                            BigDecimal finalAmount = dataJsonObj.getBigDecimal("final_amount");

                            DwsTradeProvinceOrderBean dwsTradeProvinceOrderBean = DwsTradeProvinceOrderBean.builder()
                                    .provinceId(provinceId)
                                    .userId(userId)
                                    .orderTotalAmount(finalAmount)
                                    .orderCount(1L)
                                    .ts(ts * 1000)
                                    .build();

                            collector.collect(dwsTradeProvinceOrderBean);
                        }
                    }
                }
        );
        // TODO 按照 provinceId 和 userId 分组(每个省份每个用户维护一个状态)
        KeyedStream<DwsTradeProvinceOrderBean, Tuple2<String, String>> proUidKeyedDS = beanDS.keyBy(
                new KeySelector<DwsTradeProvinceOrderBean, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(DwsTradeProvinceOrderBean dwsTradeProvinceOrderBean) throws Exception {
                        return Tuple2.of(dwsTradeProvinceOrderBean.getProvinceId(), dwsTradeProvinceOrderBean.getUserId());
                    }
                }
        );
        // TODO 状态编程 独立用户数
        SingleOutputStreamOperator<DwsTradeProvinceOrderBean> processDS = proUidKeyedDS.process(
                new KeyedProcessFunction<Tuple2<String, String>, DwsTradeProvinceOrderBean, DwsTradeProvinceOrderBean>() {
                    ValueState<String> lastOrderDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<String>(
                                "lastOrderDateState", String.class
                        );
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1L)).build());
                        lastOrderDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(DwsTradeProvinceOrderBean dwsTradeProvinceOrderBean, KeyedProcessFunction<Tuple2<String, String>, DwsTradeProvinceOrderBean, DwsTradeProvinceOrderBean>.Context context, Collector<DwsTradeProvinceOrderBean> collector) throws Exception {
                        Long ts = dwsTradeProvinceOrderBean.getTs();
                        String curDate = DateFormatUtil.tsToDate(ts);

                        String lastOrderDate = lastOrderDateState.value();

                        Long orderUuCount = 0L;

                        if (lastOrderDate == null || lastOrderDate.compareTo(curDate) < 0) {
                            orderUuCount = 1L;
                            lastOrderDateState.update(curDate);
                        }

                        dwsTradeProvinceOrderBean.setOrderUuCount(orderUuCount);
                        collector.collect(dwsTradeProvinceOrderBean);
                    }
                }
        );
        // TODO 水位线
        SingleOutputStreamOperator<DwsTradeProvinceOrderBean> withWatermarkDS = processDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<DwsTradeProvinceOrderBean>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<DwsTradeProvinceOrderBean>() {
                                    @Override
                                    public long extractTimestamp(DwsTradeProvinceOrderBean dwsTradeProvinceOrderBean, long l) {
                                        return dwsTradeProvinceOrderBean.getTs();
                                    }
                                }
                        )
        );
        // TODO 按照 provinceId 分组
        KeyedStream<DwsTradeProvinceOrderBean, String> proKeyedDS = withWatermarkDS.keyBy(DwsTradeProvinceOrderBean::getProvinceId);
        // TODO 开窗
        WindowedStream<DwsTradeProvinceOrderBean, String, TimeWindow> windowDS = proKeyedDS.window(
                TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L))
        );
        // TODO 聚合
        SingleOutputStreamOperator<DwsTradeProvinceOrderBean> reduceDS = windowDS.reduce(
                new ReduceFunction<DwsTradeProvinceOrderBean>() {
                    @Override
                    public DwsTradeProvinceOrderBean reduce(DwsTradeProvinceOrderBean t1, DwsTradeProvinceOrderBean t2) throws Exception {
                        t1.setOrderUuCount(t1.getOrderUuCount() + t2.getOrderUuCount());
                        t1.setOrderCount(t1.getOrderCount() + t2.getOrderCount());
                        t1.setOrderTotalAmount(t1.getOrderTotalAmount().add(t2.getOrderTotalAmount()));
                        return t1;
                    }
                },
                new WindowFunction<DwsTradeProvinceOrderBean, DwsTradeProvinceOrderBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow timeWindow, Iterable<DwsTradeProvinceOrderBean> iterable, Collector<DwsTradeProvinceOrderBean> collector) throws Exception {
                        DwsTradeProvinceOrderBean bean = iterable.iterator().next();
                        String stt = DateFormatUtil.tsToDateTime(timeWindow.getStart());
                        String edt = DateFormatUtil.tsToDateTime(timeWindow.getEnd());
                        String curDate = DateFormatUtil.tsToDate(timeWindow.getStart());

                        bean.setStt(stt);
                        bean.setEdt(edt);
                        bean.setCurDate(curDate);

                        collector.collect(bean);
                    }
                }
        );
        // TODO 补充省份维度
        SingleOutputStreamOperator<DwsTradeProvinceOrderBean> withProDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new DimAsyncFunction<DwsTradeProvinceOrderBean>() {
                    @Override
                    public void addDims(DwsTradeProvinceOrderBean obj, JSONObject dimJsonObj) {
                        String name = dimJsonObj.getString("name");
                        obj.setProvinceName(name);
                    }

                    @Override
                    public String getRowKey(DwsTradeProvinceOrderBean obj) {
                        return obj.getProvinceId();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_province";
                    }
                },
                60L,
                TimeUnit.SECONDS
        );

        withProDS.print();

        // TODO 写入doris
        withProDS.map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_province_order_window"));
    }
}
