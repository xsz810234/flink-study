package main.java.test.apitest.source.testprogram;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class WindowTest2_CountWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> socketStrem = env.socketTextStream("localhost", 7777);
        SingleOutputStreamOperator<Sensor> dataStream = socketStrem.map(line -> {
            String[] fields = line.split(" ");
            return new Sensor(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });
        SingleOutputStreamOperator<Double> aggregateStream = dataStream.keyBy(Sensor::getSensorId)
                .countWindow(3, 2).aggregate(new MyAvgTemp());
        aggregateStream.print("result");

        env.execute();
    }

    public static class MyAvgTemp implements AggregateFunction<Sensor, Tuple2<Double, Integer>, Double>{

        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0, 0);
        }

        @Override
        public Tuple2<Double, Integer> add(Sensor sensor, Tuple2<Double, Integer> doubleIntegerTuple2) {
            return new Tuple2<Double, Integer>(sensor.getTemprature()+doubleIntegerTuple2.f0, doubleIntegerTuple2.f1+1);
        }

        @Override
        public Double getResult(Tuple2<Double, Integer> doubleIntegerTuple2) {
            return doubleIntegerTuple2.f0/doubleIntegerTuple2.f1;
        }

        @Override
        public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> doubleIntegerTuple2, Tuple2<Double, Integer> acc1) {
            return new Tuple2<>(doubleIntegerTuple2.f0+acc1.f0, doubleIntegerTuple2.f1+acc1.f1);
        }
    }
}
