package org.example.function;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.example.utils.KeywordUtil;

import java.util.List;

@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class SplitFunction extends TableFunction<Row> {
    public void eval(String str) {
        try {
            List<String> keyWord = KeywordUtil.splitKeyWord(str);
            for (String word : keyWord) {
                collect(Row.of(word));
            }
        } catch (Exception e) {
            Row.of(str);
        }
    }
}
