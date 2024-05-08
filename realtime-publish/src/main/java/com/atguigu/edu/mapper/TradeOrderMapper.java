package com.atguigu.edu.mapper;

import com.atguigu.edu.beans.TradeProvinceOrderBean;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.List;

/**
 * ClassName: TradeOrderMapper
 * Package: com.atguigu.edu.mapper
 * Description:
 *
 * @Author: tubage
 * @Create: 2024/5/8 11:58
 * @Version: 1.0
 */
@Mapper
public interface TradeOrderMapper {
    // 订单总额
    @Select("select sum(order_total_amount) from dws_trade_course_order_window partition par#{date}")
    BigDecimal selectGMV(Integer date);

    // 各省份独立用户数 交易额
    @Select("select\n" +
            "    province_name name,\n" +
            "    sum(order_total_amount) value,\n" +
            "    sum(order_uu_count) sizeValue\n" +
            "    from dws_trade_province_order_window partition par#{date}\n" +
            "group by province_id,province_name")
    List<TradeProvinceOrderBean> selectByProvince(Integer date);
}
