package main.java.test.apitest.source.window;

import main.java.test.apitest.source.UDSource;
import main.java.test.model.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class AggregateFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> dataStreamSource = env.addSource(new UDSource()).assignTimestampsAndWatermarks(
            WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.getTimestamp();
                    }
                }));
        dataStreamSource.keyBy(data -> data.getUser())
            .window(TumblingEventTimeWindows.of(Time.seconds(10), Time.seconds(4)))
            .aggregate(new AggregateFunction<Event, Tuple3<String, Integer, Double>, Tuple2<String, Double>>() {
                @Override
                public Tuple3<String, Integer, Double> createAccumulator() {
                    return Tuple3.of("", 0, 0.00);
                }

                @Override
                public Tuple3<String, Integer, Double> add(Event value, Tuple3<String, Integer, Double> accumulator) {
                    Double time = Double.valueOf(value.getUrl());
                    Double total = accumulator.f2 + time;
                    Integer times = accumulator.f1 + 1;
                    if(!value.getUser().equals(accumulator.f0)){
                        System.out.println(String.format("error acc + %s ,%s", value.getUser(), accumulator.f0));
                    }
                    return Tuple3.of(value.getUser(), times, total);
                }

                @Override
                public Tuple2<String, Double> getResult(Tuple3<String, Integer, Double> accumulator) {
                    return Tuple2.of(accumulator.f0, accumulator.f2 / accumulator.f1);
                }

                @Override
                public Tuple3<String, Integer, Double> merge(Tuple3<String, Integer, Double> a, Tuple3<String, Integer, Double> b) {
                    System.out.println("i am merge");
                    return null;
                }
            }).print();
        env.execute();
    }
}
