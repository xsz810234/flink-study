package main.java.test.apitest.source.testprogram;

import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.runtime.operators.window.assigners.CountSlidingWindowAssigner;
import org.apache.flink.table.runtime.operators.window.assigners.CountTumblingWindowAssigner;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class WindowTest1_TimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
       // DataStream<String> inputStream = env.readTextFile("/Users/wb-xsz810234/Idea/workspace/flink-study/src/main/resources/sensor.txt");
        DataStreamSource<String> socketStrem = env.socketTextStream("localhost", 7777);
        SingleOutputStreamOperator<Sensor> dataStream = socketStrem.map(line -> {
            String[] fields = line.split(" ");
            return new Sensor(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });

        SingleOutputStreamOperator<Integer> aggregateStream = dataStream.keyBy(Sensor::getSensorId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                .aggregate(new AggregateFunction<Sensor, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(Sensor sensor, Integer integer) {
                        return ++integer;
                    }

                    @Override
                    public Integer getResult(Integer integer) {
                        return integer;
                    }

                    @Override
                    public Integer merge(Integer integer, Integer acc1) {
                        return integer + acc1;
                    }
                });
        //aggregateStream.print("aggregateStream :");
        OutputTag<Sensor> outputTag =  new OutputTag<Sensor>("late");
        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> resultStream = dataStream.keyBy(Sensor::getSensorId).window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                .allowedLateness(Time.seconds(1))
                .sideOutputLateData(outputTag)
                .apply(new WindowFunction<Sensor, Tuple3<String, Long, Integer>, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow timeWindow, Iterable<Sensor> iterable, Collector<Tuple3<String, Long, Integer>> collector) throws Exception {
                        Integer count = IteratorUtils.toList(iterable.iterator()).size();
                        collector.collect(new Tuple3<String, Long, Integer>(s, timeWindow.getEnd(), count));
                    }
                });
        /*.apply((s, timeWindow, iterable, collector) ->{
            Integer count = IteratorUtils.toList(iterable.iterator()).size();

            collector.collect(new Tuple3<>(s, timeWindow.getEnd(), count));
        });*/

        resultStream.print("sensor_id :");
        resultStream.getSideOutput(outputTag).print("late");

        env.execute();
    }
}
