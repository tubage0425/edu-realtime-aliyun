package com.atguigu.edu.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * ClassName: TrafficChAvgDurTimeBean
 * Package: com.atguigu.edu.beans
 * Description:
 *
 * @Author: tubage
 * @Create: 2024/5/8 19:12
 * @Version: 1.0
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TrafficChAvgDurTimeBean {
    private String ch;
    private Double avgDurTime;

}
