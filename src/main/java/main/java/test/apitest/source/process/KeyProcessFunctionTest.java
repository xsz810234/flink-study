package main.java.test.apitest.source.process;

import main.java.test.apitest.source.UDSource;
import main.java.test.model.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

public class KeyProcessFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> dataStreamSource = env.addSource(new UDSource());
        SingleOutputStreamOperator<Event> eventSingleOutputStreamOperator = dataStreamSource.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(3))
            .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                @Override
                public long extractTimestamp(Event element, long recordTimestamp) {
                    return element.getTimestamp();
                }
            }));
        eventSingleOutputStreamOperator.keyBy(data -> data.getUser()).process(new KeyedProcessFunction<String, Event, String>() {
            @Override
            public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                if(value.getUser().equals("Mary")){
                    out.collect("mary record is " + value.toString());
                    out.collect("mary record time is " + new Timestamp(ctx.timerService().currentProcessingTime()));
                }else if(value.getUser().equals("Tony")){
                    System.out.println("i am registing timer" + new Timestamp(ctx.timerService().currentWatermark()));
                    long currTs = ctx.timerService().currentWatermark();
                    ctx.timerService().registerEventTimeTimer(currTs + 10_000);
                }
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                out.collect("heihei i am onTimer" + new Timestamp(ctx.timerService().currentWatermark()));
            }
        }).print();
        env.execute();
    }
}
