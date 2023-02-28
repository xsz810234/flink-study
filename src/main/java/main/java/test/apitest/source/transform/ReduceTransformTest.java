package main.java.test.apitest.source.transform;

import main.java.test.apitest.source.UDSource;
import main.java.test.model.Event;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReduceTransformTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> dataStreamSource = env.addSource(new UDSource());
        SingleOutputStreamOperator<Tuple2<String, Long>> reduceResult = dataStreamSource.map(data -> Tuple2.of(data.getUser(), 1l))
            .returns(Types.TUPLE(Types.STRING, Types.LONG))
            .keyBy(data -> data.f0)
            .reduce((data1, data2) -> Tuple2.of(data1.f0, data1.f1 + data2.f1));
        //reduceResult.keyBy(data -> data.f0).print();
        reduceResult.keyBy(data -> "key").reduce((data1, data2) -> data1.f1 > data2.f1 ? data1 : data2).print();
        env.execute();
    }
}
