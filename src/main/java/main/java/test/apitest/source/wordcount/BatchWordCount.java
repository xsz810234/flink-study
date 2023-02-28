package main.java.test.apitest.source.wordcount;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        String path = BatchWordCount.class.getClassLoader().getResource("input.txt").getPath();
        DataSource<String> lineDataSource = environment.readTextFile(path);
        FlatMapOperator<String, Tuple2<String, Long>> flatResult = lineDataSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] s = line.split(" ");
            for (String single : s) {
                out.collect(Tuple2.of(single, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));
        UnsortedGrouping<Tuple2<String, Long>> tuple2UnsortedGrouping = flatResult.groupBy(0);
        AggregateOperator<Tuple2<String, Long>> sum = tuple2UnsortedGrouping.sum(1);
       // sum.print();
        environment.execute();

    }
}
