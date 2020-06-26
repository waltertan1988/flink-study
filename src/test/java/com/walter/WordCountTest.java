package com.walter;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.Test;

import java.io.File;

public class WordCountTest {

    @Test
    public void batchTest() throws Exception {
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

    @Test
    public void streamingTest() throws Exception {
        final String SERVER_IP = "192.168.1.4";
        final int SERVER_PORT = 9999;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> text = env.socketTextStream(SERVER_IP, SERVER_PORT);
        text.flatMap((value, collector) -> {
            for (String word : value.split("\\s")) {
                if(StringUtils.isNotEmpty(word)){
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        }).returns((TypeInformation) TupleTypeInfo.getBasicTupleTypeInfo(String.class, Integer.class))
                .keyBy(0).timeWindow(Time.seconds(5)).sum(1).print();

        env.execute("Flink Streaming Java API Skeleton");
    }
}
