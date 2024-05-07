package com.atguigu.edu.realtime.common.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * ClassName: TradeCourseOrderBean
 * Package: com.atguigu.edu.realtime.common.bean
 * Description:
 *
 * @Author: tubage
 * @Create: 2024/5/7 18:28
 * @Version: 1.0
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class TradeCourseOrderBean {
    // 窗口起始时间
    String stt;

    // 窗口结束时间
    String edt;
    String curDate;

    // 课程 ID
    String courseId;

    // 课程名称
    String courseName;

    // 科目 ID
    String subjectId;

    // 科目名称
    String subjectName;

    // 类别 ID
    String categoryId;

    // 类别名称
    String categoryName;

    // 下单总金额
    BigDecimal orderTotalAmount;

    // 时间戳
    @JSONField(serialize = false)
    Long ts;
}
