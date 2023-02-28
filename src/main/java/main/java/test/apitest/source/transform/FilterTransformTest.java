package main.java.test.apitest.source.transform;

import main.java.test.apitest.source.UDSource;
import main.java.test.model.Event;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FilterTransformTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> dataSource = env.addSource(new UDSource()).setParallelism(3);
        DataStream<Event> filterResult = dataSource.filter(event -> !event.getUser().isBlank()
        );
        dataSource.flatMap((Event event, Collector<String> collector) -> {
            collector.collect(event.getUser());
            collector.collect(event.getUrl());
        }).returns(TypeInformation.of(String.class)).print();
       // filterResult.print();
        env.execute();
    }
}
