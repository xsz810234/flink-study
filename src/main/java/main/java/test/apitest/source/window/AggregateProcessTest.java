package main.java.test.apitest.source.window;

import main.java.test.apitest.source.UDSource;
import main.java.test.model.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

public class AggregateProcessTest {
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
        ).aggregate(new AggregateClass(), new ProcessClass()).print();

        env.execute();

    }

    public static class AggregateClass implements AggregateFunction<Event, Tuple3<String, Integer, Long>, Tuple3<String, Integer, Long>>{

        @Override
        public Tuple3<String, Integer, Long> createAccumulator() {
            return Tuple3.of("", 0, 0l);
        }

        @Override
        public Tuple3<String, Integer, Long> add(Event event, Tuple3<String, Integer, Long> stringLongTuple3) {
            Long total = Long.valueOf(event.getUrl()) + stringLongTuple3.f2;
            return Tuple3.of(event.getUser(), stringLongTuple3.f1+1, total);
        }

        @Override
        public Tuple3<String, Integer, Long> getResult(Tuple3<String, Integer, Long> stringLongTuple3) {
            System.out.println(String.format("now stringLongTuple3 is name %s , times : %s, total time : %s", stringLongTuple3.f0, stringLongTuple3.f1, stringLongTuple3.f2));
            return stringLongTuple3;
        }

        @Override
        public Tuple3<String, Integer, Long> merge(Tuple3<String, Integer, Long> stringLongTuple2, Tuple3<String, Integer, Long> acc1) {
            return null;
        }
    }

    public static class ProcessClass extends ProcessWindowFunction<Tuple3<String, Integer, Long>, String, String, TimeWindow> {

        @Override
        public void process(String s, Context context, Iterable<Tuple3<String, Integer, Long>> elements, Collector<String> out) throws Exception {
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            Tuple3<String, Integer, Long> next = elements.iterator().next();
            Long totalTime = (Long) next.getField(2);
            Integer totalTimes = (Integer) next.getField(1);
            out.collect(String.format("window %s to %s user %s avg time is %s", new Timestamp(start), new Timestamp(end), next.getField(0),totalTime/totalTimes));
        }
    }
}
