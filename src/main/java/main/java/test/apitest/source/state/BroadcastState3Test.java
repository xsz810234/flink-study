package main.java.test.apitest.source.state;

import main.java.test.model.Event;
import main.java.test.model.Pattern;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.api.Expressions.$;

public class BroadcastState3Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings build = EnvironmentSettings.newInstance().build();
        TableEnvironment tableEnvironment = TableEnvironment.create(build);
        TableResult ss = tableEnvironment.executeSql("ss");
        tableEnvironment.from("ss").select($("a"));
        env.setParallelism(1);
        DataStreamSource<Event> dataStreamSource = env.fromElements(
                new Event("tony", "abc", new Date().getTime()),
                new Event("tony", "efc", new Date().getTime()),
                new Event("mary", "abc", new Date().getTime()),
                new Event("mary", "cde", new Date().getTime()),
                new Event("tony", "abc", new Date().getTime()),
                new Event("tony", "efc", new Date().getTime()),
                new Event("cary", "efc", new Date().getTime())

        );

        List<Pattern> list = new ArrayList<>();
        list.add(new Pattern("abc", "cde"));
        list.add(new Pattern("abc", "efc"));
        DataStreamSource<List<Pattern>> patternDataStreamSource = env.fromElements(list);
        MapStateDescriptor<String, Pattern> pattern1 = new MapStateDescriptor<>("pattern-descriptor", Types.STRING, Types.POJO(Pattern.class));
        BroadcastStream<List<Pattern>> patternStream = patternDataStreamSource.broadcast(pattern1);



        dataStreamSource.keyBy(data -> data.getUser())
                .connect(patternStream)
                .process(new MyBroadcastCoProcessFunction()).print();
        env.execute();
    }

    public static class MyBroadcastCoProcessFunction extends KeyedBroadcastProcessFunction<String, Event, List<Pattern>, Tuple2<Event, String>>{
        private ValueState<String> prevs;


        @Override
        public void open(Configuration parameters) throws Exception {
            prevs = getRuntimeContext().getState(new ValueStateDescriptor<String>("pre-vs", String.class));
        }

        @Override
        public void processElement(Event value, ReadOnlyContext ctx, Collector<Tuple2<Event, String>> out) throws Exception {
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
        public void processBroadcastElement(List<Pattern> value, Context ctx, Collector<Tuple2<Event, String>> out) throws Exception {
            BroadcastState<String, Pattern> broadcastPatternState = ctx.getBroadcastState(new MapStateDescriptor<>("pattern-descriptor", Types.STRING, Types.POJO(Pattern.class)));
            for(Pattern pattern : value){
                broadcastPatternState.put(pattern.getAction1()+pattern.getAction2(), pattern);
            }
        }
    }


}
