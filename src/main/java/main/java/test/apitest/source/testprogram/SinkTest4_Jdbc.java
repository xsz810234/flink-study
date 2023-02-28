package main.java.test.apitest.source.testprogram;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class SinkTest4_Jdbc {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> inputStream = env.readTextFile("/Users/wb-xsz810234/Idea/workspace/flink-study/src/main/resources/sensor.txt");
        SingleOutputStreamOperator<Sensor> dataStream = inputStream.map(line -> {
            String[] fields = line.split(" ");
            return new Sensor(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });

        dataStream.addSink(new MyJdbcSink());
        env.execute();
    }

    public static class MyJdbcSink extends RichSinkFunction<Sensor> {
        Connection connection = null;
        PreparedStatement insertStmt = null;
        PreparedStatement updateStmt = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://rm-bp1pka5gsnvb71e2oyo.mysql.rds.aliyuncs.com:3306/jdbc", "root", "EMRtest1234!");
            insertStmt  =  connection.prepareStatement("insert into test_sink (id, name) values (?, ?)");
            updateStmt = connection.prepareStatement("update test_sink set name = ? where id = ? ");
        }

        @Override
        public void invoke(Sensor sensor ,SinkFunction.Context context) throws Exception {
            updateStmt.setString(1, sensor.getSensorId());
            updateStmt.setInt(2, 997);
            updateStmt.execute();
            if(updateStmt.getUpdateCount() == 0){
                insertStmt.setString(2, sensor.getSensorId());
                insertStmt.setInt(1, 997);
                insertStmt.execute();
            }
        }


        @Override
        public void close() throws Exception {
            insertStmt.close();
            updateStmt.close();
            connection.close();
        }

    }
}
