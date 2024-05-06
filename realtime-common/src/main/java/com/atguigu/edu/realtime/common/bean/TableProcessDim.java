package com.atguigu.edu.realtime.common.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ClassName: TableProcessDim
 * Package: com.atguigu.edu.realtime.common.bean
 * Description:
 *
 * @Author: tubage
 * @Create: 2024/5/6 14:30
 * @Version: 1.0
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TableProcessDim {
    // 来源表名
    String sourceTable;

    // 目标表名
    String sinkTable;

    // 输出字段
    String sinkColumns;

    // 数据到 hbase 的列族
    String sinkFamily;

    // sink到 hbase 的时候的主键字段
    String sinkPk;

    // 配置表操作类型
    String op;
}
