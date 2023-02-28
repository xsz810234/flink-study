package main.java.test.apitest.source.testprogram;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class TransformTest4_MultipleStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> inputStream = env.readTextFile("/Users/wb-xsz810234/Idea/workspace/flink-study/src/main/resources/sensor.txt");
        SingleOutputStreamOperator<Sensor> dataStream = inputStream.map(line -> {
            String[] fields = line.split(" ");
            return new Sensor(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });

        DataStream<String> inputStream2 = env.readTextFile("/Users/wb-xsz810234/Idea/workspace/flink-study/src/main/resources/test.txt");
        SingleOutputStreamOperator<Tuple2<String, Double>> antherStream = inputStream2.map(new MapFunction<String, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(String s) throws Exception {
                String[] strings = s.split(" ");

                return new Tuple2<>(strings[0], Double.valueOf(strings[1]));
            }
        });
        ConnectedStreams<Tuple2<String, Double>, Sensor> connectStream = antherStream.connect(dataStream);

        SingleOutputStreamOperator<Object> connectMapStream = connectStream.map(new CoMapFunction<Tuple2<String, Double>, Sensor, Object>() {
            @Override
            public Object map1(Tuple2<String, Double> stringDoubleTuple2) throws Exception {
                return new Tuple3<>(stringDoubleTuple2.f0, stringDoubleTuple2.f1, "map1.output");
            }

            @Override
            public Object map2(Sensor sensor) throws Exception {
                return new Tuple2<>(sensor.getSensorId(), "normal");
            }
        });

        connectMapStream.print("result");

        env.execute();
    }
}
