package com.atguigu.edu.realtime.dwd.db.split.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.constant.Constant;
import com.atguigu.edu.realtime.common.util.DateFormatUtil;
import com.atguigu.edu.realtime.common.util.FlinkSinkUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;

/**
 * ClassName: DwdBaseLogCopy
 * Package: com.atguigu.edu.realtime.dwd.db.split.app
 * Description:
 *
 * @Author: tubage
 * @Create: 2024/5/6 18:51
 * @Version: 1.0
 */
public class DwdBaseLogCopy extends BaseApp {
    private final String START = "start";
    private final String ERR = "err";
    private final String DISPLAY = "display";
    private final String ACTION = "action";
    private final String PAGE = "page";
    private final String APPVIDEO = "appVideo";

    public static void main(String[] args) {
        new DwdBaseLogCopy().start(10011, 4,"dwd_base_log", Constant.TOPIC_LOG);
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // TODO 1 从kafka主题读取数据
        // TODO 2 ETL清洗-（脏数据过滤掉不要/找个地方存起来dirty_data主题）
        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag") {};
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        try {
                            JSONObject jsonObj = JSON.parseObject(s);
                            collector.collect(jsonObj);
                        } catch (Exception e) {
                            context.output(dirtyTag, s);
                        }
                    }
                }
        );
        SideOutputDataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);
        // jsonObjDS.print("主流");
        // dirtyDS.print("脏数据");

        // 脏数据直接写到kafka脏数据主题
        KafkaSink<String> kafkaSink = FlinkSinkUtil.getKafkaSink("dirty_data");
        dirtyDS.sinkTo(kafkaSink);

        // TODO 3 新老访客标记修复（状态编程 按照设备ID分组 mid）
        KeyedStream<JSONObject, String> keyedDS = jsonObjDS.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));

        SingleOutputStreamOperator<JSONObject> fixedDS = keyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<String> lastVisitDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<String>(
                                "lastVisitDateState", String.class
                        );
                        lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        String lastVisitDate = lastVisitDateState.value();

                        String isNew = jsonObject.getJSONObject("common").getString("is_new");
                        Long ts = jsonObject.getLong("ts");
                        String curVisitDate = DateFormatUtil.tsToDate(ts);

                        /*
                        新访客1，状态为空，把访问日期放状态存起来就行
                        新访客1，状态不为空，是否在同一天，同一天不用修复，如果状态日期和访客日期不在同一天，修复is_new为0
                        老访客0，状态不为空 不需要处理；
                        老访客0，状态为空，数仓刚上线，不会有之前访问日期，状态里放一个当前日期之前的值 代表访问过就行
                         */
                        // 新访客标记
                        if("1".equals(isNew)) {
                            if(StringUtils.isEmpty(lastVisitDate)){
                                lastVisitDateState.update(curVisitDate);
                            }else {
                                if(!lastVisitDate.equals(curVisitDate)) {
                                    isNew = "0";
                                    jsonObject.getJSONObject("common").put("is_new",isNew);
                                }
                            }
                        }else{
                        // 老访客标记
                            if(StringUtils.isEmpty(lastVisitDate)) {
                                Long yesterdayTs = ts - 24 * 60 * 60 * 1000;
                                String yesterday = DateFormatUtil.tsToDate(yesterdayTs);
                                lastVisitDateState.update(yesterday);
                            }
                        }

                        collector.collect(jsonObject);

                    }
                }
        );

        // fixedDS.print();

        // TODO 4 分流（侧输出流标签）err——start、page(action、display)
        OutputTag<String> errTag = new OutputTag<String>("errTag") {};
        OutputTag<String> startTag = new OutputTag<String>("startTag") {};
        OutputTag<String> displayTag = new OutputTag<String>("displayTag") {};
        OutputTag<String> actionTag = new OutputTag<String>("actionTag") {};
        OutputTag<String> appVideoTag = new OutputTag<String>("appVideoTag") {};

        SingleOutputStreamOperator<String> pageDS = fixedDS.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObj, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {
                        // 1-处理错误
                        JSONObject error = jsonObj.getJSONObject("err");
                        if (error != null) {
                            context.output(errTag, jsonObj.toJSONString());
                        }
                        jsonObj.remove("err");

                        // 2 收集启动数据
                        JSONObject start = jsonObj.getJSONObject("start");
                        if (start != null) {
                            context.output(startTag, jsonObj.toJSONString());
                        } else {
                            // 获取 "common" 字段
                            JSONObject common = jsonObj.getJSONObject("common");
                            // 获取 "ts"
                            Long ts = jsonObj.getLong("ts");
                            JSONObject appVideo = jsonObj.getJSONObject("appVideo");

                            // 2-1收集播放数据
                            if (appVideo != null) {
                                context.output(appVideoTag, jsonObj.toJSONString());
                            } else {
                                // 获取 "page" 字段
                                JSONObject page = jsonObj.getJSONObject("page");

                                //  曝光数据
                                JSONArray displays = jsonObj.getJSONArray("displays");
                                if (displays != null) {
                                    for (int i = 0; i < displays.size(); i++) {
                                        JSONObject display = displays.getJSONObject(i);
                                        JSONObject displayObj = new JSONObject();
                                        displayObj.put("display", display);
                                        displayObj.put("common", common);
                                        displayObj.put("page", page);
                                        displayObj.put("ts", ts);
                                        context.output(displayTag, displayObj.toJSONString());
                                    }
                                }

                                // 动作数据
                                JSONArray actions = jsonObj.getJSONArray("actions");
                                if (actions != null) {
                                    for (int i = 0; i < actions.size(); i++) {
                                        JSONObject action = actions.getJSONObject(i);
                                        JSONObject actionObj = new JSONObject();
                                        actionObj.put("action", action);
                                        actionObj.put("common", common);
                                        actionObj.put("page", page);
                                        context.output(actionTag, actionObj.toJSONString());
                                    }
                                }

                                // 页面数据
                                jsonObj.remove("displays");
                                jsonObj.remove("actions");
                                collector.collect(jsonObj.toJSONString());
                            }
                        }

                    }
                }
        );
        SideOutputDataStream<String> errDS = pageDS.getSideOutput(errTag);
        SideOutputDataStream<String> startDS = pageDS.getSideOutput(startTag);
        SideOutputDataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        SideOutputDataStream<String> actionDS = pageDS.getSideOutput(actionTag);
        SideOutputDataStream<String> appVideoDS = pageDS.getSideOutput(appVideoTag);

        pageDS.print("页面：");
        errDS.print("err:");
        startDS.print("start:");
        displayDS.print("display:");
        actionDS.print("action:");
        appVideoDS.print("appVideo:");

        /*
        页面：:4> {"common":{"sc":"1","ar":"28","uid":"41","os":"Android 11.0","ch":"wandoujia","is_new":"0","md":"Xiaomi 10 Pro ","mid":"mid_34","vc":"v2.1.134","ba":"Xiaomi","sid":"9b7b0028-f8c0-4fb1-8ecd-f3c46e7f6fda"},"page":{"page_id":"order","item":"19915","during_time":13682,"item_type":"order_id","last_page_id":"course_detail"},"ts":1714996398469}
        display::4> {"common":{"sc":"1","ar":"28","uid":"41","os":"Android 11.0","ch":"wandoujia","is_new":"0","md":"Xiaomi 10 Pro ","mid":"mid_34","vc":"v2.1.134","ba":"Xiaomi","sid":"9b7b0028-f8c0-4fb1-8ecd-f3c46e7f6fda"},"display":{"display_type":"query","item":"1","item_type":"course_id","pos_id":5,"order":4},"page":{"page_id":"order","item":"19915","during_time":13682,"item_type":"order_id","last_page_id":"course_detail"},"ts":1714996398469}
        appVideo::4> {"common":{"sc":"1","ar":"10","uid":"28","os":"Android 10.0","ch":"xiaomi","is_new":"0","md":"Huawei P30","mid":"mid_159","vc":"v2.1.132","ba":"Huawei","sid":"9f7f3abd-f03f-406c-a05b-a44dbdb857f9"},"appVideo":{"play_sec":30,"position_sec":210,"video_id":"2564"},"ts":1714996398386}
        action::4> {"common":{"sc":"1","ar":"10","uid":"28","os":"Android 10.0","ch":"xiaomi","is_new":"0","md":"Huawei P30","mid":"mid_159","vc":"v2.1.132","ba":"Huawei","sid":"9f7f3abd-f03f-406c-a05b-a44dbdb857f9"},"action":{"item":"273","action_id":"favor_add","item_type":"course_id","ts":1714996398385},"page":{"page_id":"course_detail","item":"273","during_time":7852,"item_type":"course_id","last_page_id":"mine"}}
        start::2> {"common":{"sc":"1","ar":"33","uid":"9","os":"iOS 13.2.3","ch":"Appstore","is_new":"0","md":"iPhone 8","mid":"mid_267","vc":"v2.1.132","ba":"iPhone","sid":"74ce7830-2736-4a36-a8ee-4b4c8bb6233b"},"start":{"entry":"notice","first_open":1,"open_ad_skip_ms":58860,"open_ad_ms":4831,"loading_time":14099,"open_ad_id":3},"ts":1714996398324}
        err::1> {"common":{"sc":"1","ar":"12","uid":"11","os":"Android 11.0","ch":"wandoujia","is_new":"1","md":"Xiaomi Mix2 ","mid":"mid_113","vc":"v2.1.132","ba":"Xiaomi","sid":"6b4932e7-24c7-4c19-945c-382dcd37fddb"},"err":{"msg":" Exception in thread \\  java.net.SocketTimeoutException\\n \\tat com.atguigu.edu2021.mock.log.AppError.main(AppError.java:xxxxxx)","error_code":3688},"appVideo":{"play_sec":30,"position_sec":420,"video_id":"2444"},"ts":1714996398348}
         */

        // TODO 5 不同流数据写到不同主题（封装FlinkSinkUtil 传主题参数）
        Map<String, DataStream> dsMap = new HashMap<>();
        dsMap.put(ERR, errDS);
        dsMap.put(START, startDS);
        dsMap.put(DISPLAY, displayDS);
        dsMap.put(ACTION, actionDS);
        dsMap.put(PAGE, pageDS);
        dsMap.put(APPVIDEO, appVideoDS);

        dsMap.get(ERR).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        dsMap.get(START).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        dsMap.get(DISPLAY).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        dsMap.get(ACTION).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));
        dsMap.get(PAGE).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
        dsMap.get(APPVIDEO).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_APPVIDEO));


    }
}
