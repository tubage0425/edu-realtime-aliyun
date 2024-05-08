package com.atguigu.edu.service.impl;

import com.atguigu.edu.beans.TradeProvinceOrderBean;
import com.atguigu.edu.beans.TradeSubAmtBean;
import com.atguigu.edu.mapper.TradeOrderMapper;
import com.atguigu.edu.service.TradeOrderService;
import lombok.AllArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

/**
 * ClassName: TradeOrderServiceImpl
 * Package: com.atguigu.edu.service.impl
 * Description:
 *
 * @Author: tubage
 * @Create: 2024/5/8 11:54
 * @Version: 1.0
 */
@Service
public class TradeOrderServiceImpl implements TradeOrderService {
    @Autowired
    TradeOrderMapper tradeOrderMapper;
    @Override
    public BigDecimal getGMV(Integer date) {
        return tradeOrderMapper.selectGMV(date);
    }

    @Override
    public List<TradeProvinceOrderBean> getByProvince(Integer date) {
        return tradeOrderMapper.selectByProvince(date);
    }

    @Override
    public List<TradeSubAmtBean> getSubjectAmt(Integer date) {
        return tradeOrderMapper.selectSubjectAmt(date);
    }
}
