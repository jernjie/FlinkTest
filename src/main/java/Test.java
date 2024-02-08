import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.*;

public class Test {
    public static void main(String[] args) {

        Configuration conf = new Configuration();

        conf.setInteger(RestOptions.PORT, 8089);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        StreamTableEnvironment tbenv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
//        env.disableOperatorChaining();

        TableResult tableResult = tbenv.executeSql(
                "CREATE TABLE datagen (\n" +
                        " f_sequence INT,\n" +
                        " f_random INT,\n" +
                        " f_random_str STRING,\n" +
                        " ts AS localtimestamp,\n" +
                        " WATERMARK FOR ts AS ts\n" +
                        ") WITH (\n" +
                        " 'connector' = 'datagen',\n" +
                        " 'rows-per-second'='50000',\n" +
                        " 'fields.f_sequence.min'='1',\n" +
                        " 'fields.f_sequence.max'='1000',\n" +
                        " 'fields.f_random.min'='1',\n" +
                        " 'fields.f_random.max'='1000',\n" +
                        " 'fields.f_random_str.length'='10'\n" +
                        ")"
        );

        TableResult tableResult1 = tbenv.executeSql(
                "CREATE TABLE print (\n" +
                        " f_sequence INT,\n" +
                        " f_random INT,\n" +
                        " f_random_str STRING\n" +
                        ") WITH (\n" +
                        " 'connector' = 'print'\n" +
                        ")"
        );

        tbenv.executeSql(
                "insert into print select f_sequence,f_random,f_random_str from datagen\n"
        );



    }
}
