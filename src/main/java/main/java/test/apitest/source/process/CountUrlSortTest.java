package main.java.test.apitest.source.process;

import main.java.test.apitest.source.UDSource;
import main.java.test.model.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;

public class CountUrlSortTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> dataStreamSource = env.addSource(new UDSource());

        SingleOutputStreamOperator<Event> singleOutputStreamOperator =
            dataStreamSource.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(0))
            .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                @Override
                public long extractTimestamp(Event element, long recordTimestamp) {
                    return element.getTimestamp();
                }
            })
        );
        singleOutputStreamOperator.map(event -> event.getUrl()).keyBy(url -> url)
            .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
            .aggregate(new CountURLAggregateFunction(), new CountURLProcessWindowFunction()).print();
        env.execute();
    }

    public static class CountURLAggregateFunction implements AggregateFunction<String, HashMap<String, Long>, ArrayList<Tuple2<String, Long>>>{

        @Override
        public HashMap<String, Long> createAccumulator() {
            return new HashMap<>();
        }

        @Override
        public HashMap<String, Long> add(String value, HashMap<String, Long> accumulator) {
            if(accumulator.containsKey(value)){
                Long aLong = accumulator.get(value);
                accumulator.put(value, aLong + 1);
            }else {
                accumulator.putIfAbsent(value,1L);
            }
            return accumulator;
        }

        @Override
        public ArrayList<Tuple2<String, Long>> getResult(HashMap<String, Long> accumulator) {
            ArrayList<Tuple2<String, Long>> list = new ArrayList<>();
            for(String key : accumulator.keySet()){
                list.add(Tuple2.of(key, accumulator.get(key)));
            }
            list.sort(new Comparator<Tuple2<String, Long>>() {
                @Override
                public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
                    return o2.f1 > o1.f1 ? 1 : -1;
                }
            });
            System.out.println("list.size() " + list.size());
            return list;
        }

        @Override
        public HashMap<String, Long> merge(HashMap<String, Long> a, HashMap<String, Long> b) {
            return null;
        }
    }

    public static class CountURLProcessWindowFunction extends ProcessAllWindowFunction<ArrayList<Tuple2<String, Long>>, String, TimeWindow> {

        @Override
        public void process(Context context, Iterable<ArrayList<Tuple2<String, Long>>> elements, Collector<String> out) throws Exception {
            ArrayList<Tuple2<String, Long>> element = elements.iterator().next();
            System.out.println(element.size() + " element.size()");
            StringBuffer stringBuffer = new StringBuffer();
            stringBuffer.append("result --------------------\n");
            int min = Math.min(2, element.size());
            for(int i = 0; i < min; i ++){
                stringBuffer.append(String.format("第 %s 名的url是：%s, 访问量是： %s", i+1, element.get(i).f0, element.get(i).f1));
                stringBuffer.append("-------- --------------------\n");
            }
            stringBuffer.append("end --------------------\n");
            out.collect(stringBuffer.toString());
        }
    }
}
