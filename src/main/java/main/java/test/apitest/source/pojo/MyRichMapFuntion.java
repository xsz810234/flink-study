package main.java.test.apitest.source.pojo;

import main.java.test.model.Event;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;

public class MyRichMapFuntion extends RichMapFunction<Event, String> {
    @Override
    public String map(Event event) {
        return event.toString();
    }

    @Override
    public void open(Configuration configuration){
        MetricGroup metricGroup = this.getRuntimeContext().getMetricGroup();
        System.out.println("my rich map function start to work");
    }

    @Override
    public void close() {
        System.out.println("my rich map function finish its job");
    }
}
