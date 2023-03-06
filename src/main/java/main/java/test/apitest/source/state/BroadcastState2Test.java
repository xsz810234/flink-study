package main.java.test.apitest.source.state;

import main.java.test.model.Event;
import main.java.test.model.Pattern;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Date;
import java.util.Map;
import java.util.Random;

public class BroadcastState2Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> dataStreamSource = env.fromElements(
                new Event("tony", "abc", new Date().getTime()),
                new Event("tony", "efc", new Date().getTime()),
                new Event("mary", "abc", new Date().getTime()),
                new Event("mary", "cde", new Date().getTime())

        );

        DataStreamSource<Pattern> patternDataStreamSource = env.fromElements(new Pattern("abc", "cde"),
                new Pattern("abc", "efc"));
        MapStateDescriptor<String, Pattern> pattern1 = new MapStateDescriptor<>("pattern-descriptor", Types.STRING, Types.POJO(Pattern.class));
        BroadcastStream<Pattern> patternStream = patternDataStreamSource.broadcast(pattern1);



        dataStreamSource.keyBy(data -> data.getUser())
                .connect(patternStream)
                .process(new MyBroadcastCoProcessFunction()).print();
        env.execute();
    }

    public static class MyBroadcastCoProcessFunction extends KeyedBroadcastProcessFunction<String, Event, Pattern, Tuple2<Event, String>>{
        private ValueState<String> prevs;


        @Override
        public void open(Configuration parameters) throws Exception {
            prevs = getRuntimeContext().getState(new ValueStateDescriptor<String>("pre-vs", String.class));
        }

        @Override
        public void processElement(Event value, ReadOnlyContext ctx, Collector<Tuple2<Event, String>> out) throws Exception {
            Thread.sleep(2_000);
            ReadOnlyBroadcastState<String, Pattern> readOnlyBroadcastState = ctx.getBroadcastState(new MapStateDescriptor<>("pattern-descriptor", Types.STRING, Types.POJO(Pattern.class)));
            String prevalue = prevs.value();
            readOnlyBroadcastState.immutableEntries().forEach(entry ->{
                System.out.println("heihei");
                Pattern pattern = entry.getValue();
                if (prevalue != null && pattern != null) {
                    if (pattern.getAction1().equals(prevalue) && pattern.getAction2().equals(value.getUrl())) {
                        out.collect(Tuple2.of(value, pattern.toString()));
                    }
                }
            });



            prevs.update(value.getUrl());

            System.out.println("after processElement" + value.getUser());
        }

        @Override
        public void processBroadcastElement(Pattern value, Context ctx, Collector<Tuple2<Event, String>> out) throws Exception {
            System.out.println("before processBroadcastElement" + value.getAction2());
            BroadcastState<String, Pattern> broadcastPatternState = ctx.getBroadcastState(new MapStateDescriptor<>("pattern-descriptor", Types.STRING, Types.POJO(Pattern.class)));
            broadcastPatternState.put(value.getAction1()+value.getAction2(), value);
            System.out.println("after processBroadcastElement" + value.getAction2());
        }
    }


}
