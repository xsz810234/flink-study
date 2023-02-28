package main.java.test.apitest.source.testprogram;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformTest3_Reduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> inputStream = env.readTextFile("/Users/wb-xsz810234/Idea/workspace/flink-study/src/main/resources/sensor.txt");
        SingleOutputStreamOperator<Sensor> dataStream = inputStream.map(line -> {
            String[] fields = line.split(" ");
            return new Sensor(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });

        KeyedStream<Sensor, String> keyStream = dataStream.keyBy(Sensor::getSensorId);
//        SingleOutputStreamOperator<Sensor> reduceStream = keyStream.reduce(new ReduceFunction<Sensor>() {
//            @Override
//            public Sensor reduce(Sensor sensor, Sensor t1) throws Exception {
//                return new Sensor(sensor.getSensorId(), Long.max(sensor.getTimestamp(), t1.getTimestamp()), Math.max(sensor.getTemprature(), t1.getTimestamp()));
//            }
//        });
        SingleOutputStreamOperator<Sensor> reduceStream = keyStream.reduce((sensor, t1) -> new Sensor(sensor.getSensorId(), Long.max(sensor.getTimestamp(), t1.getTimestamp()), Math.max(sensor.getTemprature(), t1.getTemprature())));

        reduceStream.print("reduceStream");
        env.execute();
    }
}
