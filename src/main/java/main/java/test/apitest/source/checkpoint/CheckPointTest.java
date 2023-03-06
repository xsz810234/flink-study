package main.java.test.apitest.source.checkpoint;

import main.java.test.apitest.source.UDSource;
import main.java.test.model.Event;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.Collection;
import java.util.Date;

public class CheckPointTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(100_000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(5_000);
        StateBackend stateBackend = new FsStateBackend("file:///Users/wb-xsz810234/tmp2/tmp");
        env.setStateBackend(stateBackend);

        DataStreamSource<Event> dataStreamSource = env.addSource(new UDSource());

        SingleOutputStreamOperator<Event> dataSource = dataStreamSource.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(1))
            .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                @Override
                public long extractTimestamp(Event element, long recordTimestamp) {
                    System.out.println("recordTimestamp value is " + new Date(recordTimestamp));
                    System.out.println("element datetime is " + new Date(element.getTimestamp()));
                    return element.getTimestamp();
                }
            }));

        dataSource.keyBy(data -> data.getUser())
            .process(new UDKeyedProcessFunction() {
            });
        dataSource.print();
        env.execute();
    }

    public static class UDKeyedProcessFunction extends KeyedProcessFunction<String, Event, Tuple2<String, String>> implements CheckpointedFunction{

        private MapState<String, Long> mapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("value-count", Types.STRING, Types.LONG));
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {

        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {

        }

        @Override
        public void processElement(Event value, Context ctx, Collector<Tuple2<String, String>> out) throws Exception {
            if(mapState.contains(value.getUser())){
                mapState.put(value.getUser(), mapState.get(value.getUser()) + 1);
            }else {
                mapState.put(value.getUser(),1l);
            }
        }
    }
}
