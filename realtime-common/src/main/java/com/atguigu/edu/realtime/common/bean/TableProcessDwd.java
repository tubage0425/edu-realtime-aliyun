package com.atguigu.edu.realtime.common.bean;

import lombok.Data;

/**
 * ClassName: TableProcessDwd
 * Package: com.atguigu.edu.realtime.common.bean
 * Description:
 *
 * @Author: tubage
 * @Create: 2024/5/7 11:12
 * @Version: 1.0
 */
@Data
public class TableProcessDwd {
    // 来源表
    String sourceTable;

    // 操作类型
    String sourceType;

    // 输出表
    String sinkTable;

    // 输出字段
    String sinkColumns;
    // 配置表操作类型
    String op;
}
