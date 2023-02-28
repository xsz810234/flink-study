package main.java.test.apitest.source.testprogram;

import org.apache.flink.api.common.eventtime.BoundedOutOfOrdernessWatermarks;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Time;
import java.time.Duration;

public class ProcessTest1_KeyedProcessFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> socketStrem = env.socketTextStream("localhost", 7777);
        SingleOutputStreamOperator<Sensor> dataStream = socketStrem.map(line -> {
            String[] fields = line.split(" ");
            return new Sensor(fields[0], System.currentTimeMillis(), Double.valueOf(fields[2]));
        });
        SingleOutputStreamOperator<String> process = dataStream.keyBy(Sensor::getSensorId)
                .process(new MyProcess());
        process
                .assignTimestampsAndWatermarks(new WatermarkStrategy<String>() {
                    @Override
                    public WatermarkGenerator<String> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        Duration duration = Duration.ofSeconds(3);
                        return new BoundedOutOfOrdernessWatermarks<>(duration);
                    }
                });
        process.print("myprocess :");


        env.execute();
    }

    public static class MyProcess extends KeyedProcessFunction<String, Sensor, String>{

        ValueState<Long> valueStateTs;

        @Override
        public void open(Configuration parameters) throws Exception {
            valueStateTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("myValueState", Long.class));
        }

        @Override
        public void close() throws Exception {
            valueStateTs.clear();
        }

        @Override
        public void processElement(Sensor sensor, Context context, Collector<String> collector) throws Exception {
            System.out.println(sensor.getSensorId() + sensor.getTimestamp() + context.timestamp());
            System.out.println(context.getCurrentKey());
            //context.output();
            System.out.println(context.timerService().currentWatermark());
            collector.collect(sensor.toString());

            context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime()+3000);
            valueStateTs.update(context.timerService().currentProcessingTime()+3000);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<String> collector){
            System.out.println(timestamp + "定时器触发");
        }
    }
}
