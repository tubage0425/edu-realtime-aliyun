package com.atguigu.edu.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.List;

/**
 * ClassName: TradeProvinceOrderBean
 * Package: com.atguigu.edu.beans
 * Description:
 *
 * @Author: tubage
 * @Create: 2024/5/8 14:52
 * @Version: 1.0
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TradeProvinceOrderBean {
    private String name;
    private BigDecimal value;
    private Long sizeValue;
}


