package com.atguigu.edu.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.bean.TrafficPageViewBean;
import com.atguigu.edu.realtime.common.constant.Constant;
import com.atguigu.edu.realtime.common.func.BeanToJsonStrMapFunction;
import com.atguigu.edu.realtime.common.util.DateFormatUtil;
import com.atguigu.edu.realtime.common.util.FlinkSinkUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.awt.image.Kernel;

/**
 * ClassName: DwsTrafficVcChArIsNewPageViewWindow
 * Package: com.atguigu.edu.realtime.dws.app
 * Description:
 *
 * @Author: tubage
 * @Create: 2024/5/7 15:43
 * @Version: 1.0
 */
public class DwsTrafficVcChArIsNewPageViewWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTrafficVcChArIsNewPageViewWindow().start(10022,4,"dws_traffic_vc_ch_ar_is_new_page_view_window", Constant.TOPIC_DWD_TRAFFIC_PAGE);
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // TODO 从dwd页面日志读取数据
        // TODO 转换流中数据类型JSONObject
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(
                new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String s) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(s);
                        return jsonObj;
                    }
                }
        );
        // jsonObjDS.print();

        // TODO 按照mid 分组（UV）
        KeyedStream<JSONObject, String> midKeyedDS = jsonObjDS.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));
        // TODO 状态编程、封装为统计的实体对象
        SingleOutputStreamOperator<TrafficPageViewBean> beanDS = midKeyedDS.process(
                new KeyedProcessFunction<String, JSONObject, TrafficPageViewBean>() {
                    ValueState<String> lastVisitDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<String>(
                                "lastVisitDateState", String.class
                        );
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1L)).build());
                        lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, TrafficPageViewBean>.Context context, Collector<TrafficPageViewBean> collector) throws Exception {
                        JSONObject commonJsonObj = jsonObject.getJSONObject("common");
                        String vc = commonJsonObj.getString("vc");
                        String ch = commonJsonObj.getString("ch");
                        String ar = commonJsonObj.getString("ar");
                        String is_new = commonJsonObj.getString("is_new");

                        JSONObject pageJsonObj = jsonObject.getJSONObject("page");

                        // 获取当前访问日期
                        Long ts = jsonObject.getLong("ts");
                        String curVisitDate = DateFormatUtil.tsToDate(ts);

                        // 从状态中获取上次访问日期
                        String lastVisitDate = lastVisitDateState.value();
                        Long uvCt = 0L;
                        if (StringUtils.isEmpty(lastVisitDate) || !lastVisitDate.equals(curVisitDate)) {
                            uvCt = 1L;
                            lastVisitDateState.update(curVisitDate);
                        }

                        String lastPageId = jsonObject.getString("last_page_id");
                        Long svCt = StringUtils.isEmpty(lastPageId) ? 1L : 0L;

                        TrafficPageViewBean viewBean = new TrafficPageViewBean(
                                "",
                                "",
                                "",
                                vc,
                                ch,
                                ar,
                                is_new,
                                uvCt,
                                svCt,
                                1L,
                                pageJsonObj.getLong("during_time"),
                                ts
                        );
                        collector.collect(viewBean);
                    }
                }
        );
        // beanDS.print();

        // TODO 指定waterMark 提取事件时间字段
        SingleOutputStreamOperator<TrafficPageViewBean> withWatermarkDS = beanDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<TrafficPageViewBean>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<TrafficPageViewBean>() {
                                    @Override
                                    public long extractTimestamp(TrafficPageViewBean trafficPageViewBean, long l) {
                                        return trafficPageViewBean.getTs();
                                    }
                                }
                        )
        );
        // TODO 按照统计维度分组
        KeyedStream<TrafficPageViewBean, Tuple4<String, String, String, String>> keyedDS = withWatermarkDS.keyBy(
                new KeySelector<TrafficPageViewBean, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4<String, String, String, String> getKey(TrafficPageViewBean trafficPageViewBean) throws Exception {
                        return Tuple4.of(
                                trafficPageViewBean.getVc(),
                                trafficPageViewBean.getCh(),
                                trafficPageViewBean.getAr(),
                                trafficPageViewBean.getIsNew());
                    }
                }
        );
        // TODO 开窗
        WindowedStream<TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow> windowDS = keyedDS.window(
                TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L))
        );
        // TODO 聚合
        SingleOutputStreamOperator<TrafficPageViewBean> reduceDS = windowDS.reduce(
                new ReduceFunction<TrafficPageViewBean>() {
                    @Override
                    public TrafficPageViewBean reduce(TrafficPageViewBean t1, TrafficPageViewBean t2) throws Exception {
                        t1.setUvCt(t1.getUvCt() + t2.getUvCt());
                        t1.setPvCt(t1.getPvCt() + t2.getPvCt());
                        t1.setSvCt(t1.getSvCt() + t2.getSvCt());
                        t1.setDurSum(t1.getDurSum() + t2.getDurSum());
                        return t1;
                    }
                },
                new WindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow timeWindow, Iterable<TrafficPageViewBean> iterable, Collector<TrafficPageViewBean> collector) throws Exception {
                        TrafficPageViewBean trafficPageViewBean = iterable.iterator().next();

                        String stt = DateFormatUtil.tsToDateTime(timeWindow.getStart());
                        String edt = DateFormatUtil.tsToDateTime(timeWindow.getEnd());
                        String curDate = DateFormatUtil.tsToDate(timeWindow.getStart());
                        trafficPageViewBean.setStt(stt);
                        trafficPageViewBean.setEdt(edt);
                        trafficPageViewBean.setCur_date(curDate);

                        collector.collect(trafficPageViewBean);
                    }
                }
        );

        // reduceDS.print();
        // TODO 关联维度HBASE（旁路缓存+异步IO）补充省份名称和来源 待补充？？？？


        // TODO 写入Doris  dws_traffic_vc_ch_ar_is_new_page_view_window  （封装工具函数）
        reduceDS
                .map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_traffic_vc_ch_ar_is_new_page_view_window"));
    }
}
