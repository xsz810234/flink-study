package main.java.test.apitest.source.transform;

import main.java.test.apitest.source.UDSource;
import main.java.test.apitest.source.pojo.MyRichMapFuntion;
import main.java.test.model.Event;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class RichMapFunctionTransformTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> eventDataStreamSource = env.addSource(new UDSource());
        eventDataStreamSource.map(new MyRichMapFuntion()).rebalance().print().setParallelism(4);

        eventDataStreamSource.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event value) throws Exception {
                return value.toString();
            }
        }).print();

        eventDataStreamSource.flatMap(new FlatMapFunction<Event, String>() {
            @Override
            public void flatMap(Event value, Collector<String> out) throws Exception {
                out.collect("i am flatmap" + value.toString());
            }
        }).print();
        env.execute();
    }

}
