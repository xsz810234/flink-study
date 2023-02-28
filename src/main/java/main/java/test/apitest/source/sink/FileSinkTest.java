package main.java.test.apitest.source.sink;

import main.java.test.apitest.source.UDSource;
import main.java.test.model.Event;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;

public class FileSinkTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<Event> sourceDatastream = env.addSource(new UDSource());
        StreamingFileSink<String> sink = StreamingFileSink.forRowFormat(new Path("./output"),
                new SimpleStringEncoder<String>("UTF-16"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                // 文件到达指定大小以后创建新的文件
                                .withMaxPartSize(1)
                                // 文件写入到达指定时间以后创建新的文件
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(2))
                                //多长时间内没有数据传入，写入新的文件
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(1))
                                .build()
                ).build();
        sourceDatastream.map(data -> data.toString()).addSink(sink);
        sourceDatastream.addSink(JdbcSink.sink(
            "insert into test values (?, ?, ?)",
            (data, event) ->{
                data.setString(1, event.getUser());
                data.setString(2, event.getUrl());
                data.setString(3, event.getTimestamp().toString());
            },
           new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withDriverName("com.mysql.jdbc.Driver").build()
        ));
        env.execute();
    }
}
