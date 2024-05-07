package com.atguigu.edu.realtime.dws.func;

import com.atguigu.edu.realtime.dws.util.KeyWordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * ClassName: KeywordUDTF
 * Package: com.atguigu.edu.realtime.dws.func
 * Description:
 *
 * @Author: tubage
 * @Create: 2024/5/7 14:52
 * @Version: 1.0
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeywordUDTF extends TableFunction<Row> {
    public void eval(String text) {
        for (String keyword : KeyWordUtil.analyze(text)) {
            collect(Row.of(keyword));
        }
    }
}

