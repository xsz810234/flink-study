package main.java.test.apitest.source.testprogram;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Transform_Test5_RichFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(6);
        DataStream<String> inputStream = env.readTextFile("/Users/wb-xsz810234/Idea/workspace/flink-study/src/main/resources/sensor.txt");
        SingleOutputStreamOperator<Sensor> dataStream = inputStream.map(line -> {
            String[] fields = line.split(" ");
            return new Sensor(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });

        //SingleOutputStreamOperator<Tuple2<String, Integer>> richMapStream = dataStream.map(new MyMapper());
        /*SingleOutputStreamOperator<Tuple2<String, Integer>> richMapStream = dataStream.map(new RichMapFunction<Sensor, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Sensor sensor) throws Exception {
                return new Tuple2<>(sensor.getSensorId(), getRuntimeContext().getIndexOfThisSubtask());
            }
        });*/
        SingleOutputStreamOperator<Tuple2<String, Long>> richMapStream = dataStream.map(sensor -> {
            return new Tuple2<>(sensor.getSensorId(), sensor.getTimestamp());
        });
        richMapStream.print("rich");
        env.execute();
    }

    public static class MyMapper extends RichMapFunction<Sensor, Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> map(Sensor sensor) throws Exception {
            return new Tuple2<>(sensor.getSensorId(), getRuntimeContext().getIndexOfThisSubtask());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("start ");
        }

        @Override
        public void close() throws Exception {
            System.out.println("finished");
        }
    }
}
