package main.java.test.apitest.source.process;

import main.java.test.apitest.source.UDSource;
import main.java.test.model.Event;
import main.java.test.model.UrlCountPOJO;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.Date;

public class ConnectorStreamTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> dataStreamSource = env.fromElements(
                new Event("Mary", "aaa", new Date().getTime()),
                new Event("Tony", "bbb", new Date().getTime()),
                new Event("Park", "ccc", new Date().getTime())
        );
        SingleOutputStreamOperator<Event> singleOutputStreamOperator = dataStreamSource.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.getTimestamp();
                    }
                }));

        DataStreamSource<UrlCountPOJO> urlCountPOJODataStreamSource = env.fromElements(
                new UrlCountPOJO("Tony", 4l, new Date().getTime(),  new Date().getTime()+10l),
                new UrlCountPOJO("Park", 10l, new Date().getTime(),  new Date().getTime()+10l),
                new UrlCountPOJO("Heihei", 10l, new Date().getTime(),  new Date().getTime()+10l));
        SingleOutputStreamOperator<UrlCountPOJO> urlSingleOutputStreamOperator = urlCountPOJODataStreamSource.assignTimestampsAndWatermarks(WatermarkStrategy.<UrlCountPOJO>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner(new SerializableTimestampAssigner<UrlCountPOJO>() {
                    @Override
                    public long extractTimestamp(UrlCountPOJO element, long recordTimestamp) {
                        return element.getWindowStart();
                    }
                }));

        OutputTag<String> unCheck = new OutputTag<>("un-check-data"){};
        SingleOutputStreamOperator<String> process = singleOutputStreamOperator.connect(urlSingleOutputStreamOperator)
                .keyBy(data -> data.getUser(), data -> data.getUrl())
                .process(new CoProcessFuntionUDCLass());
        process.getSideOutput(unCheck).print("un-check-data-2");
        process.print();
        env.execute();

    }

    public static class CoProcessFuntionUDCLass extends CoProcessFunction<Event, UrlCountPOJO, String>{

        OutputTag<String> unCheck = new OutputTag<>("un-check-data"){};

        private ValueState<Event> eventValueState;

        private ValueState<UrlCountPOJO> urlCountValueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            eventValueState = getRuntimeContext().getState(new ValueStateDescriptor<Event>("event-value-state", Event.class));

            urlCountValueState = getRuntimeContext().getState(new ValueStateDescriptor<UrlCountPOJO>("urlcount-value-state", UrlCountPOJO.class));
        }

        @Override
        public void processElement1(Event value, Context ctx, Collector<String> out) throws Exception {
            if(urlCountValueState.value() != null){
                out.collect(String.format("查看记录成功 当前 %s 观看的url为 %s, 观看次数是 %s", value.getUser(), value.getUrl(),urlCountValueState.value().getTimes()));
                urlCountValueState.clear();
            }else {
                eventValueState.update(value);
                ctx.timerService().registerEventTimeTimer(value.getTimestamp() + 5_000);
            }
        }

        @Override
        public void processElement2(UrlCountPOJO value, Context ctx, Collector<String> out) throws Exception {
            if(eventValueState.value() != null){
                out.collect(String.format("查看记录成功 当前观看的url为 %s, 观看次数是 %s", value.getUrl(), value.getTimes()));
                eventValueState.clear();
            }else {

                urlCountValueState.update(value);

                ctx.timerService().registerEventTimeTimer(value.getWindowStart());
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            if(eventValueState.value() != null){
                String format = String.format("查看统计记录失败, 当前eventValueState的值为 %s", eventValueState.value().toString());
                ctx.output(unCheck, format);
            }

            if(urlCountValueState.value() != null){
                String result = String.format("查看记录失败, 当前urlCountValueState的值为 %s" , urlCountValueState.value().toString());
                ctx.output(unCheck, result);
            }

            eventValueState.clear();
            urlCountValueState.clear();
        }
    }
}
