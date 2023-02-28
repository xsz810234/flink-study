package main.java.test.apitest.source.testprogram;

import org.apache.flink.api.common.eventtime.BoundedOutOfOrdernessWatermarks;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;

public class ProcessTest2_ApplicationCase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> socketStrem = env.socketTextStream("localhost", 7777);
        SingleOutputStreamOperator<Sensor> dataStream = socketStrem.map(line -> {
            String[] fields = line.split(" ");
            return new Sensor(fields[0], System.currentTimeMillis(), Double.valueOf(fields[2]));
        });
        SingleOutputStreamOperator<Tuple2<String, List<Double>>> process = dataStream.keyBy(Sensor::getSensorId)
                .process(new TempContantsIncrease(5));
        process.print("myprocess :");


        env.execute();
    }

    public static class TempContantsIncrease extends KeyedProcessFunction<String, Sensor, Tuple2<String, List<Double>>>{
        private Integer interval;
        private ValueState<Double> lastTemp;
        private ValueState<Long> tsTime;

        public TempContantsIncrease(Integer interval){
            this.interval = interval;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTemp = getRuntimeContext().getState(new ValueStateDescriptor<Double>("myTempState", Double.class));
            tsTime = getRuntimeContext().getState(new ValueStateDescriptor<Long>("mytsTime", Long.class));
        }

        @Override
        public void close() throws Exception {
            lastTemp.clear();
        }

        @Override
        public void processElement(Sensor sensor, Context context, Collector<Tuple2<String, List<Double>>> collector) throws Exception {
            Double lastTempValue = lastTemp.value() == null ? Double.MIN_VALUE : lastTemp.value();

            if(lastTempValue < sensor.getTemprature() && tsTime.value() == null){
                System.out.println("haha");
                System.out.println("lastTempValue : " + lastTempValue );
                System.out.println("sensor.getTemprature() : " + sensor.getTemprature());
                Long ts = context.timerService().currentProcessingTime() + interval * 1000L;
                context.timerService().registerProcessingTimeTimer(ts);
                tsTime.update(ts);
            }else if(sensor.getTemprature() < lastTempValue && tsTime.value() != null){
                System.out.println("heihei");
                System.out.println("lastTempValue : " + lastTempValue );
                System.out.println("sensor.getTemprature() : " + sensor.getTemprature());
                context.timerService().deleteProcessingTimeTimer(tsTime.value());
                tsTime.clear();
                System.out.println("tsTime.value() : " + tsTime.value());
            }
            lastTemp.update(sensor.getTemprature());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<Tuple2<String, List<Double>>> collector){
            System.out.println(timestamp + "定时器触发");
            tsTime.clear();
        }
    }
}
