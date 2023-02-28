package main.java.test.apitest.source.testprogram;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StateTest2_KeyedState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> inputStream = env.readTextFile("/Users/wb-xsz810234/Idea/workspace/flink-study/src/main/resources/sensor.txt");
        SingleOutputStreamOperator<Sensor> dataStream = inputStream.map(line -> {
            String[] fields = line.split(" ");
            return new Sensor(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });

        SingleOutputStreamOperator<Sensor> mapStream = dataStream.keyBy(Sensor::getSensorId).map(new MyKeyCountMapper());
        mapStream.print("mapStream");

        env.execute();
    }

    public static class MyKeyCountMapper extends RichMapFunction<Sensor, Sensor>{
        private ValueState<Integer> keyCountState;

        private ListState<Sensor> myListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            keyCountState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("key-count", Integer.class));
            myListState = getRuntimeContext().getListState(new ListStateDescriptor<Sensor>("list-state", Sensor.class));

        }


        @Override
        public Sensor map(Sensor sensor) throws Exception {
            Integer count = keyCountState.value() == null ? 0 : keyCountState.value();
            ++count;
            keyCountState.update(count);

            myListState.add(sensor);
            for (Sensor sensor1 : myListState.get()){
                System.out.println(sensor1.getSensorId());
            }


            return sensor;
        }


        @Override
        public void close() throws Exception {
            keyCountState.clear();
            myListState.clear();
        }
    }
}
