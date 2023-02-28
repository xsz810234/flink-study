package main.java.test.apitest.source.transform;

import main.java.test.apitest.source.UDSource;
import main.java.test.model.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyTransformTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);
        DataStreamSource<Event> dataSource = env.addSource(new UDSource());

        KeyedStream<Event, String> eventStream = dataSource.keyBy(data -> data.getUser());
       // eventStream.print();
       // eventStream.max("timestamp").print();
        eventStream.maxBy("timestamp").print();
        env.execute();
    }
}
