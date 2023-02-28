package main.java.test.apitest.source.process;

import main.java.test.apitest.source.UDSource;
import main.java.test.model.Event;
import main.java.test.model.UrlCountPOJO;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

public class KeyedProcessFunctionTest2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> dataStreamSource = env.addSource(new UDSource());

        SingleOutputStreamOperator<Event> singleOutputStreamOperator = dataStreamSource.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        })
        );

        SingleOutputStreamOperator<UrlCountPOJO> aggregate = singleOutputStreamOperator.keyBy(data -> data.getUrl())
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new AggregateFunction<Event, UrlCountPOJO, UrlCountPOJO>() {
                    @Override
                    public UrlCountPOJO createAccumulator() {
                        UrlCountPOJO urlCountPOJO = new UrlCountPOJO();
                        urlCountPOJO.setTimes(0l);
                        return urlCountPOJO;
                    }

                    @Override
                    public UrlCountPOJO add(Event value, UrlCountPOJO accumulator) {
                        accumulator.setUrl(value.getUrl());
                        accumulator.setTimes(accumulator.getTimes() + 1);
                        return accumulator;
                    }

                    @Override
                    public UrlCountPOJO getResult(UrlCountPOJO accumulator) {
                        return accumulator;
                    }

                    @Override
                    public UrlCountPOJO merge(UrlCountPOJO a, UrlCountPOJO b) {
                        return null;
                    }
                }, new ProcessWindowFunction<UrlCountPOJO, UrlCountPOJO, String, TimeWindow>() {

                    @Override
                    public void process(String s, Context context, Iterable<UrlCountPOJO> elements, Collector<UrlCountPOJO> out) throws Exception {

                        UrlCountPOJO urlCountPOJO = elements.iterator().next();
                        urlCountPOJO.setWindowStart(context.window().getStart());
                        urlCountPOJO.setWindowEnd(context.window().getEnd());
                        urlCountPOJO.setUrl(s);
                        out.collect(urlCountPOJO);
                    }
                });

        aggregate.keyBy(data -> data.getWindowEnd())
                .process(new URLCountKeyedProcessFunction(2)).print();
        env.execute();

    }


    public static class URLCountKeyedProcessFunction extends KeyedProcessFunction<Long, UrlCountPOJO, String>{
        private Integer topN;

        public URLCountKeyedProcessFunction(Integer topN){
            this.topN = topN;
        }

        private ListState<UrlCountPOJO> listState;

        @Override
        public void open(Configuration parameters) throws Exception {
            listState = getRuntimeContext().getListState(
                    new ListStateDescriptor<UrlCountPOJO>("url-count", Types.POJO(UrlCountPOJO.class)));
        }


        @Override
        public void processElement(UrlCountPOJO value, Context ctx, Collector<String> out) throws Exception {
            listState.add(value);
            ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey() +1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList<UrlCountPOJO> element = new ArrayList<>();

            for(UrlCountPOJO urlCountPOJO : listState.get()){
                element.add(urlCountPOJO);
            }

            element.sort(new Comparator<UrlCountPOJO>() {
                @Override
                public int compare(UrlCountPOJO o1, UrlCountPOJO o2) {
                    return o2.getTimes().intValue() - o1.getTimes().intValue();
                }
            });

            System.out.println(element.size() + " element.size()");
            StringBuffer stringBuffer = new StringBuffer();
            stringBuffer.append("result --------------------\n");
            for(int i = 0; i < topN; i ++){
                stringBuffer.append(String.format("第 %s 名的url是：%s, 访问量是： %s", i+1, element.get(i).getUrl(), element.get(i).getTimes()));
                stringBuffer.append("-------- --------------------\n");
            }
            stringBuffer.append("end --------------------\n");
            out.collect(stringBuffer.toString());
        }
    }
}


