package main.java.test.apitest.source.state;

import main.java.test.apitest.source.UDSource;
import main.java.test.model.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class OperatorStateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> dataStreamSource = env.addSource(new UDSource());
        SingleOutputStreamOperator<Event> singleOutputStreamOperator = dataStreamSource.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(0))
            .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                @Override
                public long extractTimestamp(Event element, long recordTimestamp) {
                    return element.getTimestamp();
                }
            }));

        singleOutputStreamOperator.print("single");
        singleOutputStreamOperator.addSink(new MySinkFunction(10));
        env.execute();
    }

    public static class MySinkFunction implements SinkFunction<Event>, CheckpointedFunction{

        private final Integer threshold;


        public MySinkFunction(Integer threshold) {
            this.threshold = threshold;
            bufferData = new ArrayList<>();
        }

        public List<Event> bufferData;

        public ListState<Event> listState;

        @Override
        public void invoke(Event value, Context context) throws Exception {
            bufferData.add(value);
            if(bufferData.size() == threshold){
                for(Event event : bufferData){
                    System.out.println(event.toString());
                }
                bufferData.clear();
            }

        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {

            listState.clear();

            for(Event event : bufferData){
                System.out.println("heihei");
                listState.add(event);
            }

        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            listState = context.getOperatorStateStore()
                .getListState(new ListStateDescriptor<Event>("list-state", Event.class));
            System.out.println("i am initializeState");
            if(context.isRestored()){
                for(Event event : listState.get()){
                    bufferData.add(event);
                }
            }
        }
    }

}
