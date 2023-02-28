package main.java.test.apitest.source;

import main.java.test.model.Event;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Date;
import java.util.Random;

public class UDSource implements SourceFunction<Event>, ParallelSourceFunction<Event> {
    private Boolean running = true;
    String[] users = {"Mary", "Alice", "Cary", "Tony"};
    String[] urls = {"2", "33", "2222", "231"};
    Random random = new Random();
//    public void run(SourceContext<Event> ctx) throws Exception {
//        while (running){
//            String user = users[random.nextInt(users.length)];
//            String url = urls[random.nextInt(urls.length)];
//            ctx.collect(new Event(user, url, new Date().getTime()));
//            Thread.sleep(2_000);
//        }
//    }


    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        while (running){
            String user = users[random.nextInt(users.length)];
            String url = urls[random.nextInt(urls.length)];
           // int i = random.nextInt(3);
            Event event = new Event(user, url, new Date().getTime() -0 * 1000);
            System.out.println(event.toString());
            ctx.collect(event);
            Thread.sleep(1_000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
