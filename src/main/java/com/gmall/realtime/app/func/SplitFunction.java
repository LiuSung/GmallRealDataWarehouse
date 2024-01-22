package com.gmall.realtime.app.func;

import com.gmall.realtime.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;

@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class SplitFunction extends TableFunction<Row> {

    public void eval(String str) {
        try {
            List<String> words = KeywordUtil.splitKeyword(str);
            for(String word : words){
                collect(Row.of(word));
            }
        } catch (IOException e) {
            throw new RuntimeException("Split Words False");
        }

    }
}
