package main.java.test.apitest.source.window;

import main.java.test.apitest.source.UDSource;
import main.java.test.model.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class WindowTest2 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> dataStreamSource = env.addSource(new UDSource())
//        DataStreamSource<Event> dataStreamSource = env.fromElements(new Event("mary", "./abc", new Date().getTime() -20*2000)
//            ,new Event("mary", "./abc", new Date().getTime() -20*1800)
//            ,new Event("tony", "./xsz", new Date().getTime() -20*1600)
//            ,new Event("hah", "./abc", new Date().getTime() -20*1700)
//            ,new Event("tony", "./abc", new Date().getTime() -20*2000)
//            ,new Event("tony", "./dfg", new Date().getTime() -20*1900)
//            ,new Event("jack", "./abc", new Date().getTime() -20*1600)
//            ,new Event("jack", "./223", new Date().getTime() -20*2000)
//            ,new Event("jack", "./vs", new Date().getTime() -20*1800)
//            ,new Event("ha", "./2we", new Date().getTime() -20*1950)
//            ,new Event("didi", "./abc", new Date().getTime() -20*2000)
//            ,new Event("didi", "./abc", new Date().getTime() -20*1980)
//            ,new Event("fi", "./wq", new Date().getTime() -20*1970)
//            ,new Event("fdsa", "./veeq", new Date().getTime() -20*2000)
//            );
//        SingleOutputStreamOperator<Event> dataStreamSource = env.fromElements(new Event("mary", "./abc", 1000l)
//            ,new Event("mary", "./abc", 2000l))
        .assignTimestampsAndWatermarks( WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(1))
            .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                @Override
                public long extractTimestamp(Event element, long recordTimestamp) {
                    return element.getTimestamp();
                }
            }));
        dataStreamSource.map(new MapFunction<Event, Tuple2<String, Long>>() {

            @Override
            public Tuple2<String, Long> map(Event value) throws Exception {
                return Tuple2.of(value.getUser(), 1l);
            }
        }).keyBy(data -> data.f0)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .reduce((data1, data2) ->
               Tuple2.of(data1.f0, data1.f1 + data2.f1))
         //   .print();
            .addSink(StreamingFileSink.forRowFormat(new Path("./output"), new SimpleStringEncoder<Tuple2<String, Long>>("UTF-8"))
                .withRollingPolicy(
                    DefaultRollingPolicy.builder().withMaxPartSize(5).withRolloverInterval(30).withInactivityInterval(15).build())
                .build()
            );
        env.execute();
    }
}
