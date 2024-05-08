package com.atguigu.edu.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.bean.DwsInteractionCourseReviewWindowBean;
import com.atguigu.edu.realtime.common.func.BeanToJsonStrMapFunction;
import com.atguigu.edu.realtime.common.func.DimAsyncFunction;
import com.atguigu.edu.realtime.common.util.DateFormatUtil;
import com.atguigu.edu.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * ClassName: DwsInteractionCourseReviewWindow
 * Package: com.atguigu.edu.realtime.dws.app
 * Description:
 *
 * @Author: tubage
 * @Create: 2024/5/8 10:40
 * @Version: 1.0
 */
public class DwsInteractionCourseReviewWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsInteractionCourseReviewWindow().start(10025,4,"dws_interaction_course_review_window","dwd_interaction_review");
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // dwd 课程评价读数据  各课程评价总分、评价人数和好评人数
        // {"course_id":439,"create_time":"2024-05-07 14:03:56","user_id":13,"id":8076,"review_stars":5,"ts":1715061835}
        // 转换结构
        SingleOutputStreamOperator<DwsInteractionCourseReviewWindowBean> beanDS = kafkaStrDS.process(
                new ProcessFunction<String, DwsInteractionCourseReviewWindowBean>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, DwsInteractionCourseReviewWindowBean>.Context context, Collector<DwsInteractionCourseReviewWindowBean> collector) throws Exception {
                        try {
                            JSONObject jsonObj = JSON.parseObject(s);
                            String courseId = jsonObj.getString("course_id");
                            Long ts = jsonObj.getLong("ts");
                            Long reviewStar = jsonObj.getLong("review_stars");

                            DwsInteractionCourseReviewWindowBean reviewBean = DwsInteractionCourseReviewWindowBean.builder()
                                    .courseId(courseId)
                                    .reviewUserCount(1L)
                                    .reviewTotalStars(reviewStar)
                                    .goodReviewUserCount(reviewStar == 5L ? 1L : 0L)
                                    .ts(ts * 1000)
                                    .build();

                            collector.collect(reviewBean);

                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
        );
        // 水位线
        SingleOutputStreamOperator<DwsInteractionCourseReviewWindowBean> withWatermarkDS = beanDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<DwsInteractionCourseReviewWindowBean>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<DwsInteractionCourseReviewWindowBean>() {
                                    @Override
                                    public long extractTimestamp(DwsInteractionCourseReviewWindowBean dwsInteractionCourseReviewWindowBean, long l) {
                                        return dwsInteractionCourseReviewWindowBean.getTs();
                                    }
                                }
                        )
        );
        // 按课程ID分组
        KeyedStream<DwsInteractionCourseReviewWindowBean, String> courseIdKeyedDS = withWatermarkDS.keyBy(DwsInteractionCourseReviewWindowBean::getCourseId);

        // 开窗
        WindowedStream<DwsInteractionCourseReviewWindowBean, String, TimeWindow> windowDS = courseIdKeyedDS.window(
                TumblingEventTimeWindows.of(Time.seconds(10L))
        );
        // 聚合
        SingleOutputStreamOperator<DwsInteractionCourseReviewWindowBean> reduceDS = windowDS.reduce(
                new ReduceFunction<DwsInteractionCourseReviewWindowBean>() {
                    @Override
                    public DwsInteractionCourseReviewWindowBean reduce(DwsInteractionCourseReviewWindowBean t1, DwsInteractionCourseReviewWindowBean t2) throws Exception {
                        t1.setGoodReviewUserCount(t1.getGoodReviewUserCount() + t2.getGoodReviewUserCount());
                        t1.setReviewTotalStars(t1.getReviewTotalStars() + t2.getReviewTotalStars());
                        t1.setReviewUserCount(t1.getReviewUserCount() + t1.getReviewUserCount());
                        return t1;
                    }
                },
                new WindowFunction<DwsInteractionCourseReviewWindowBean, DwsInteractionCourseReviewWindowBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow timeWindow, Iterable<DwsInteractionCourseReviewWindowBean> iterable, Collector<DwsInteractionCourseReviewWindowBean> collector) throws Exception {
                        DwsInteractionCourseReviewWindowBean reviewBean = iterable.iterator().next();
                        String stt = DateFormatUtil.tsToDateTime(timeWindow.getStart());
                        String edt = DateFormatUtil.tsToDateTime(timeWindow.getEnd());
                        String curDate = DateFormatUtil.tsToDate(timeWindow.getStart());

                        reviewBean.setStt(stt);
                        reviewBean.setEdt(edt);
                        reviewBean.setCurDate(curDate);

                        collector.collect(reviewBean);
                    }
                }
        );
        // 补充课程维度
        SingleOutputStreamOperator<DwsInteractionCourseReviewWindowBean> withCourseDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new DimAsyncFunction<DwsInteractionCourseReviewWindowBean>() {
                    @Override
                    public void addDims(DwsInteractionCourseReviewWindowBean obj, JSONObject dimJsonObj) {
                        obj.setCourseName(dimJsonObj.getString("course_name"));
                    }

                    @Override
                    public String getRowKey(DwsInteractionCourseReviewWindowBean obj) {
                        return obj.getCourseId();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_course_info";
                    }
                },
                60L,
                TimeUnit.SECONDS
        );

        withCourseDS.print();
        // 写入doris
        withCourseDS.map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_interaction_course_review_window"));

    }
}
