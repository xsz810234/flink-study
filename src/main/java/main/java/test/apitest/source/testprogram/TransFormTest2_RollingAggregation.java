package main.java.test.apitest.source.testprogram;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransFormTest2_RollingAggregation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> inputStream = env.readTextFile("/Users/wb-xsz810234/Idea/workspace/flink-study/src/main/resources/sensor.txt");
        SingleOutputStreamOperator<Sensor> dataStream = inputStream.map(line -> {
            String[] fields = line.split(" ");
            return new Sensor(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });
        /*KeyedStream<Sensor, String> keyStream = dataStream.keyBy(new KeySelector<Sensor, String>() {
            @Override
            public String getKey(Sensor sensor) throws Exception {
                return sensor.getSensorId();
            }
        });*/
        KeyedStream<Sensor, String> keyStream = dataStream.keyBy(Sensor::getSensorId);
        SingleOutputStreamOperator<Sensor> temprature = keyStream.maxBy("temprature");
        temprature.print("temprature");
       // keyStream.print("keyStream");
        env.execute();
    }
}
