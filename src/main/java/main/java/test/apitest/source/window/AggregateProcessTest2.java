package main.java.test.apitest.source.window;

import main.java.test.apitest.source.UDSource;
import main.java.test.model.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashSet;

public class AggregateProcessTest2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> dataStreamSource = env.addSource(new UDSource());
        SingleOutputStreamOperator<Event> singleOutputStreamOperator = dataStreamSource.assignTimestampsAndWatermarks(
            WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event event, long l) {
                return event.getTimestamp();
            }
        }));
        singleOutputStreamOperator.keyBy(event -> event.getUser()).window(
            TumblingEventTimeWindows.of(Time.seconds(10), Time.seconds(5))
        ).aggregate(new AggregateClass(), new ProcessClass());

        env.execute();

    }

    public static class AggregateClass implements AggregateFunction<Event, HashSet<String>, Long> {


        @Override
        public HashSet<String> createAccumulator() {
            return new HashSet<>();
        }

        @Override
        public HashSet<String> add(Event value, HashSet<String> accumulator) {
            accumulator.add(value.getUser());
            return accumulator;
        }

        @Override
        public Long getResult(HashSet<String> accumulator) {
            return (long) accumulator.size();
        }

        @Override
        public HashSet<String> merge(HashSet<String> a, HashSet<String> b) {
            return null;
        }
    }

    public static class ProcessClass extends ProcessWindowFunction<Long, String, String, TimeWindow> {

        @Override
        public void process(String s, Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            out.collect(String.format("window %s to %s user %s avg time is %s", start, end, 2));
        }
    }
//        public static class ProcessClass implements WindowFunction<Tuple3<String, Integer, Long>, String, Boolean, TimeWindow> {
//
//            @Override
//            public void apply(Boolean aBoolean, TimeWindow window, Iterable<Tuple3<String, Integer, Long>> input, Collector<String> out) throws Exception {
//                Long start = window.getStart();
//                Long end = window.getEnd();
//                Tuple3<String, Integer, Long> next = input.iterator().next();
//                Long totalTime = (Long) next.getField(2);
//                Integer totalTimes = (Integer) next.getField(1);
//                out.collect(String.format("window %s to %s user %s avg time is %s", start, end, next.getField(0),totalTime/totalTimes));
//        }
}
