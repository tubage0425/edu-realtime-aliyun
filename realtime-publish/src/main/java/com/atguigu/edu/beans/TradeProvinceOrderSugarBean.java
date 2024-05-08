package com.atguigu.edu.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * ClassName: TradeProvinceOrderSugarBean
 * Package: com.atguigu.edu.beans
 * Description:
 *
 * @Author: tubage
 * @Create: 2024/5/8 15:21
 * @Version: 1.0
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TradeProvinceOrderSugarBean {
    private List mapData;
    private String valueName = "订单总额";
    private String sizeValueName =  "独立用户数";

}