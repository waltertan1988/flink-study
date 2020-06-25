package com.walter;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.junit.Test;

import java.io.File;

public class BatchWordCountTest {

    @Test
    public void batchWordCountTest() throws Exception {
        String input = "file:///" + new File("src/main/resources/WordCount.txt").getAbsolutePath();
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> text = env.readTextFile(input);

        text.flatMap((value, collector) -> {
            for (String word : value.split("\\s")) {
                if(StringUtils.isNotEmpty(word)){
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        }).returns((TypeInformation) TupleTypeInfo.getBasicTupleTypeInfo(String.class, Integer.class))
        .groupBy(0).sum(1).print();
    }
}
