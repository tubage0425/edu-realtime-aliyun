package com.atguigu.edu.controller;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.beans.TradeProvinceOrderBean;
import com.atguigu.edu.beans.TradeProvinceOrderSugarBean;
import com.atguigu.edu.beans.TradeSubAmtBean;
import com.atguigu.edu.service.TradeOrderService;
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
import java.math.RoundingMode;
import java.util.Date;
import java.util.List;

/**
 * ClassName: TradeOrderController
 * Package: com.atguigu.edu.controller
 * Description:
 *
 * @Author: tubage
 * @Create: 2024/5/8 11:50
 * @Version: 1.0
 */
@RestController
@MapperScan("com.atguigu.edu.mapper")
public class TradeOrderController {
    @Autowired
    TradeOrderService tradeOrderService;
    @RequestMapping( "/gmv")
    public ResponseEntity<Object> getGMV(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if(date == 0) {
            String yyyyMMdd = DateFormatUtils.format(new Date(), "yyyyMMdd");
            date = Integer.valueOf(yyyyMMdd);
        }
        BigDecimal gmv = tradeOrderService.getGMV(date);

        if (gmv == null) {
            ReturnJsonObj result = new ReturnJsonObj(404, "No data available for the specified date.", null);
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(result);
        }

        ReturnJsonObj result = new ReturnJsonObj(0, "success", gmv);
        return ResponseEntity.ok(result);
    }

    @RequestMapping( "/percent")
    public ResponseEntity<Object> getPercent(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if(date == 0) {
            String yyyyMMdd = DateFormatUtils.format(new Date(), "yyyyMMdd");
            date = Integer.valueOf(yyyyMMdd);
        }
        BigDecimal gmv = tradeOrderService.getGMV(date);
        BigDecimal target = BigDecimal.valueOf(200000);

        if (gmv == null) {
            ReturnJsonObj result = new ReturnJsonObj(404, "No data available for the specified date.", null);
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(result);
        }

        BigDecimal percent = gmv.divide(target);

        ReturnJsonObj result = new ReturnJsonObj(0, "success", percent.multiply(BigDecimal.valueOf(100)));
        return ResponseEntity.ok(result);
    }

    @RequestMapping("/orderByProvince")
    public ResponseEntity<Object> getOrderByProvince(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if(date == 0) {
            String yyyyMMdd = DateFormatUtils.format(new Date(), "yyyyMMdd");
            date = Integer.valueOf(yyyyMMdd);
        }
        List<TradeProvinceOrderBean> provinceOrderBeanList = tradeOrderService.getByProvince(date);

        if (provinceOrderBeanList == null || provinceOrderBeanList.size() < 1) {
            ReturnJsonObj result = new ReturnJsonObj(404, "No data available for the specified date.", null);
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(result);
        }

        TradeProvinceOrderSugarBean dataObj = new TradeProvinceOrderSugarBean();
        dataObj.setMapData(provinceOrderBeanList);

        ReturnJsonObj result = new ReturnJsonObj(0, "", dataObj);
        return ResponseEntity.ok(result);
    }

    @RequestMapping( "/subjectAmt")
    public ResponseEntity<Object> getSubjectAmt(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if(date == 0) {
            String yyyyMMdd = DateFormatUtils.format(new Date(), "yyyyMMdd");
            date = Integer.valueOf(yyyyMMdd);
        }
        List<TradeSubAmtBean> subjectAmtList = tradeOrderService.getSubjectAmt(date);

        if (subjectAmtList == null || subjectAmtList.size() < 1) {
            ReturnJsonObj result = new ReturnJsonObj(404, "No data available for the specified date.", null);
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(result);
        }


        ReturnJsonObj result = new ReturnJsonObj(0, "success", subjectAmtList);
        return ResponseEntity.ok(result);
    }

}
