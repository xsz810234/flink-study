package main.java.test.apitest.source.testprogram;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceTest2_File {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<String> inputDataStream = env.readTextFile("/Users/wb-xsz810234/Idea/workspace/flink-study/src/main/resources/test.txt");
        inputDataStream.print("input");
        env.execute();
    }

}
