package main.java.test.apitest.source.SQL;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

public class SQLAPITest {
    public static void main(String[] args) {
        EnvironmentSettings envSettings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        TableEnvironment tableEnv = TableEnvironment.create(envSettings);

        String sourceSql = "create table sourceTable ( " +
            "name STRING," +
            " url STRING," +
            " `timestamp` BIGINT )"+
            " WITH (" +
            "'connector' = 'filesystem' ," +
            "'path' = 'input/input.txt', " +
            "'format' = 'csv'" +
            ")";
        TableResult tableResult = tableEnv.executeSql(sourceSql);
        Table table = tableEnv.sqlQuery("select name, url from sourceTable");
        String sinkSql = "create table sinkTable ( " +
            " name STRING," +
            " url STRING," +
            "`timestamp` BIGINT )"+
            "WITH (" +
            "'connector' = 'print'" +
            ")";
        TableResult sinkTable = tableEnv.executeSql(sinkSql);

        String insertSql = "select name, count(url) from sourceTable group by name ";
        tableEnv.executeSql(insertSql);
    }
}
