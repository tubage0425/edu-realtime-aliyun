package com.atguigu.edu.controller;

import com.atguigu.edu.beans.TrafficChAvgDurTimeBean;
import com.atguigu.edu.beans.TrafficKeywordsBean;
import com.atguigu.edu.service.TrafficStatsService;
import com.atguigu.edu.utils.ReturnJsonObj;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

/**
 * ClassName: TrafficStatsController
 * Package: com.atguigu.edu.controller
 * Description:
 *
 * @Author: tubage
 * @Create: 2024/5/8 18:37
 * @Version: 1.0
 */
@RestController
@MapperScan("com.atguigu.edu.mapper")
public class TrafficStatsController {
    @Autowired
    TrafficStatsService trafficStatsService;
    @RequestMapping("/getKeywords")
    public ResponseEntity<Object> getKeywords(@RequestParam(value="date",defaultValue = "0") Integer date){
        if(date == 0) {
            String yyyyMMdd = DateFormatUtils.format(new Date(), "yyyyMMdd");
            date = Integer.valueOf(yyyyMMdd);
        }

        List<TrafficKeywordsBean> keywordList = trafficStatsService.getKeywords(date);

        if (keywordList == null || keywordList.size() < 1) {
            ReturnJsonObj result = new ReturnJsonObj(404, "No data available for the specified date.", null);
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(result);
        }


        ReturnJsonObj result = new ReturnJsonObj(0, "", keywordList);
        return ResponseEntity.ok(result);
    }

    @RequestMapping("/getAvgDurTime")
    public ResponseEntity<Object> getAvgDurTime(@RequestParam(value="date",defaultValue = "0") Integer date){
        if(date == 0) {
            String yyyyMMdd = DateFormatUtils.format(new Date(), "yyyyMMdd");
            date = Integer.valueOf(yyyyMMdd);
        }

        List<TrafficChAvgDurTimeBean> avgDurTimeList = trafficStatsService.getAvgDurTime(date);

        if (avgDurTimeList == null || avgDurTimeList.size() < 1) {
            ReturnJsonObj result = new ReturnJsonObj(404, "No data available for the specified date.", null);
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(result);
        }

        ArrayList<String> chNameList = new ArrayList<>();
        ArrayList<Double> TimeList = new ArrayList<>();

        for (TrafficChAvgDurTimeBean trafficChAvgDurTimeBean : avgDurTimeList) {
            String ch = trafficChAvgDurTimeBean.getCh();
            Double avgDurTime = trafficChAvgDurTimeBean.getAvgDurTime();
            chNameList.add(ch);
            TimeList.add(avgDurTime);
        }

        HashMap<Object, Object> dataMap = new HashMap<>();
        dataMap.put("categories", chNameList);

        HashMap<Object, Object> seriesMap = new HashMap<>();
        seriesMap.put("name","渠道来源");
        seriesMap.put("data", TimeList);
        ArrayList<Object> seriesList = new ArrayList<>();
        seriesList.add(seriesMap);

        dataMap.put("series", seriesList);


        ReturnJsonObj result = new ReturnJsonObj(0, "", dataMap);
        return ResponseEntity.ok(result);
    }
}
