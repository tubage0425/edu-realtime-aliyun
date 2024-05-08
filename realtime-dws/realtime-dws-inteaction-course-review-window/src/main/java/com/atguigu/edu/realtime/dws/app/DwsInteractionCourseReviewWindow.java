package com.atguigu.edu.realtime.dws.app;

import com.atguigu.edu.realtime.common.base.BaseApp;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
        // 水位线
        // 按课程ID分组
        // 开窗
        // 聚合
        // 补充课程维度
        // 写入doris
    }
}
