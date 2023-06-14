package dev.erdil.drivers;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.List;

import static dev.erdil.helpers.DataSetHelpers.printDatasetInfo;

public class WordCount {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        DataSet<String> texts = env.readTextFile("resources/wc.txt");

        printDatasetInfo(texts, 5);

        DataSet<String> filteredTexts = texts.filter(value -> value.startsWith("N"));

        printDatasetInfo(filteredTexts, 5);

        MapOperator<String, Tuple2<String, Integer>> tokenizedTexts = filteredTexts.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return new Tuple2<String, Integer>(value, 1);
            }
        });

        tokenizedTexts.print();

        List<Tuple2<String, Integer>> result = tokenizedTexts.groupBy(0).sum(1).collect();

        System.out.println(result);

        //env.execute("WordCount Example");
    }
}
