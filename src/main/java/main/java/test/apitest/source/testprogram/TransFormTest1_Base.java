package main.java.test.apitest.source.testprogram;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TransFormTest1_Base {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> inputStream = env.readTextFile("/Users/wb-xsz810234/Idea/workspace/flink-study/src/main/resources/test.txt");
        SingleOutputStreamOperator<Object> mapStream = inputStream.map(new MapFunction<String, Object>() {
            @Override
            public String map(String s) throws Exception {
                return s.split(" ")[0];
            }
        });

        mapStream.print("map");

        SingleOutputStreamOperator<Sensor> flatMapStream = inputStream.flatMap(new FlatMapFunction<String, Sensor>() {
            @Override
            public void flatMap(String s, Collector<Sensor> collector) throws Exception {
                String[] spilt = s.split(" ");
                collector.collect(new Sensor(spilt[0], System.currentTimeMillis(), Double.valueOf(spilt[1])));
            }
        });

        flatMapStream.print("flatmap");

        SingleOutputStreamOperator<String> filterStream = inputStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return s.startsWith("sensor_1");
            }
        });

        filterStream.print("filterStream");

        env.execute();
    }

}
