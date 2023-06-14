package dev.erdil.drivers;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.List;

public class WordCount {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        DataSet<String> texts = env.readTextFile("resources/wc.txt");

        DataSet<String> filteredTexts = texts.filter(value -> value.startsWith("N"));

        MapOperator<String, Tuple2<String, Integer>> tokenizedTexts = filteredTexts.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return new Tuple2<String, Integer>(value, 1);
            }
        });

        List<Tuple2<String, Integer>> result = tokenizedTexts.groupBy(0).sum(1).collect();

        //env.execute("WordCount Example");
    }
}
