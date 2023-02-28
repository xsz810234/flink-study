package main.java.test.apitest.source.window;

import main.java.test.apitest.source.UDSource;
import main.java.test.model.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class WindowTest3 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> dataStreamSource = env.addSource(new UDSource())
        .assignTimestampsAndWatermarks( WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(1))
            .withTimestampAssigner((data, recordTimestamp) -> data.getTimestamp()));
        dataStreamSource.map(data -> Tuple2.of(data.getUser(), 1l)
        ).returns(Types.TUPLE(Types.STRING, Types.LONG)).keyBy(data -> data.f0)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .reduce((data1, data2) ->
               Tuple2.of(data1.f0, data1.f1 + data2.f1))
         //   .print();
            .addSink(StreamingFileSink.forRowFormat(new Path("./output"), new SimpleStringEncoder<Tuple2<String, Long>>("UTF-8"))
                .withRollingPolicy(
                    DefaultRollingPolicy.builder().withMaxPartSize(5 * 1000).withRolloverInterval(30).withInactivityInterval(15).build())
                .build()
            );
        env.execute();
    }
}
