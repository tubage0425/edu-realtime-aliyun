package com.atguigu.edu.service;

import com.atguigu.edu.beans.TradeProvinceOrderBean;

import java.math.BigDecimal;
import java.util.List;

/**
 * ClassName: TradeOrderService
 * Package: com.atguigu.edu.service
 * Description:
 *
 * @Author: tubage
 * @Create: 2024/5/8 11:54
 * @Version: 1.0
 */
public interface TradeOrderService {
    BigDecimal getGMV(Integer date);

    List<TradeProvinceOrderBean> getByProvince(Integer date);
}
