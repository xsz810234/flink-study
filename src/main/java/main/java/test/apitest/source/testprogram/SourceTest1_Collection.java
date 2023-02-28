package main.java.test.apitest.source.testprogram;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class SourceTest1_Collection {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Sensor> dataStreamSource = env.fromCollection(Arrays.asList(
                new Sensor("snsor_1", 1547718199L, 35.8)
                , new Sensor("snsor_3", 1547718201L, 36.8)
                , new Sensor("snsor_5", 1547718195L, 33.8)
                , new Sensor("snsor_4", 1547718196L, 32.8)
                , new Sensor("snsor_12", 1547718207L, 40.8)));
        dataStreamSource.print("data");
        env.execute("test");

    }
}
