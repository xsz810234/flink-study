package main.java.test.apitest.source.transform;

import main.java.test.model.Event;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Date;
import java.util.Random;

public class RescaleTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new RichParallelSourceFunction<Event>() {
            private Boolean running = true;
            String[] users = {"Mary", "Alice", "Cary", "Tony"};
            String[] urls = {"./home", "./cart", "./prod?id=100", "./prod?id=20"};
            Random random = new Random();
            @Override
            public void run(SourceContext<Event> ctx) throws Exception {
                for (int i = 1; i <= 20; i++){
                    if( i %2 == getRuntimeContext().getIndexOfThisSubtask()) {
                        String user = users[i % 4];
                        String url = urls[i % 4];
                        Event event = new Event(user, url, new Date().getTime());
                        System.out.println("getRuntimeContext().getIndexOfThisSubtask()" + i +getRuntimeContext().getIndexOfThisSubtask());
                        ctx.collect(event);
                        Thread.sleep(3_000);
                    }else {
                        System.out.println("i is " + i);
                    }
                }
            }

            @Override
            public void cancel() {

            }
        }).setParallelism(2).rescale().print().setParallelism(4);
        env.execute();
    }
}
