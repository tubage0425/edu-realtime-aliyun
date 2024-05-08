package com.atguigu.edu.realtime.common.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * ClassName: DwsTradeProvinceOrderBean
 * Package: com.atguigu.edu.realtime.common.bean
 * Description:
 *
 * @Author: tubage
 * @Create: 2024/5/7 21:24
 * @Version: 1.0
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class DwsTradeProvinceOrderBean {
    // 窗口起始时间
    String stt;

    // 窗口结束时间
    String edt;
    String curDate;


    // 省份 ID
    String provinceId;

    // 省份名称
    String provinceName;

    // 用户 ID
    @JSONField(serialize = false)
    String userId;

    // 订单总额
    BigDecimal orderTotalAmount;

    // 下单独立用户数
    Long orderUuCount;

    // 订单数
    Long orderCount;

    // 时间戳
    @JSONField(serialize = false)
    Long ts;
}
