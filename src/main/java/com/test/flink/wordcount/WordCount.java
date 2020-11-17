package com.test.flink.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * Hello world!
 */
public class WordCount {

    public static void main(String[] args) {
        System.out.println("Hello World!");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> text = env.socketTextStream("localhost", 9001, "\n");

        DataStream<WordWithCount> windowCounts = text.flatMap(new FlatMapFunction<String, WordWithCount>() {
            @Override
            public void flatMap(String s, Collector<WordWithCount> collector) throws Exception {
                //
                for (String word : s.split("//s")) {
                    collector.collect(new WordWithCount(word, 1));
                }
            }
        }).keyBy("word")
                .timeWindow(Time.seconds(6), Time.seconds(3))
                .reduce(new ReduceFunction<WordWithCount>() {
                    @Override
                    public WordWithCount reduce(WordWithCount value1, WordWithCount value2) throws Exception {
                        return new WordWithCount(value1.word, value1.count + value2.count);
                    }
                });

        windowCounts.print().setParallelism(1);

        try {
            env.execute("word count test1");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
