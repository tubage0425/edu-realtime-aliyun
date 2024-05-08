package com.atguigu.edu.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ClassName: TrafficKeywordsBean
 * Package: com.atguigu.edu.beans
 * Description:
 *
 * @Author: tubage
 * @Create: 2024/5/8 18:31
 * @Version: 1.0
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TrafficKeywordsBean {
    // 关键词
    String name;
    // 关键词评分
    Integer value;
}
