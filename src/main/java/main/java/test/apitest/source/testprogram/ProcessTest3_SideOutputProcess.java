package main.java.test.apitest.source.testprogram;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.DefaultOperatorStateBackend;
import org.apache.flink.runtime.state.StateBackendBuilder;
import org.apache.flink.runtime.state.filesystem.FsStateBackendFactory;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class ProcessTest3_SideOutputProcess {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE);

        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart());
        DataStreamSource<String> socketStrem = env.socketTextStream("localhost", 7777);
        SingleOutputStreamOperator<Sensor> dataStream = socketStrem.map(line -> {
            String[] fields = line.split(" ");
            return new Sensor(fields[0], System.currentTimeMillis(), Double.valueOf(fields[2]));
        });
        OutputTag<Sensor> outputTag = new OutputTag<Sensor>("low-temp"){

        };



        SingleOutputStreamOperator<Sensor> highStream = dataStream.process(new ProcessFunction<Sensor, Sensor>() {
            @Override
            public void processElement(Sensor sensor, Context context, Collector<Sensor> collector) throws Exception {
                if(sensor.getTemprature() > 25.0){
                    collector.collect(sensor);
                }else {
                    context.output(outputTag, sensor);
                }
            }
        });


        highStream.print("high-temp");
        highStream.getSideOutput(outputTag).print("low-temp");
        env.execute();
    }
}
