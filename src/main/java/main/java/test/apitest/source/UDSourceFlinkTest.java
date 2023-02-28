package main.java.test.apitest.source;

import main.java.test.model.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class UDSourceFlinkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        DataStreamSource<Event> eventSource = env.addSource(new UDSource()).setParallelism(3);
        eventSource.print();
        env.execute();
    }
}
