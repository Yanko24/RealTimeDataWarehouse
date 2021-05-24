package com.yankee.gmall.realtime.app.fun;

import com.yankee.gmall.realtime.utils.KeyWordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description keyword分词函数
 * @date 2021/5/21 10:20
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeyWordUDTF extends TableFunction<Row> {
    public void eval(String str) {
        // 调用分词器分词
        List<String> list = KeyWordUtil.analyze(str);

        for (String s : list) {
            collect(Row.of(s));
        }
    }
}
