package dev.erdil.drivers;

import dev.erdil.helpers.DataSetHelpers;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;

import static dev.erdil.helpers.DataSetHelpers.printDatasetInfo;

public class WordCount {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

        final ParameterTool params = ParameterTool.fromArgs(args);

        env.getConfig().setGlobalJobParameters(params);

        DataSet<String> texts = env.readTextFile("resources/wc.txt");

        //env.execute("WordCount Example");

        //printDatasetInfo(texts, 10);

        DataSet<String> filteredTexts = texts.filter(value -> value.startsWith("N"));

        System.out.println(env.getConfig());

        //printDatasetInfo(filteredTexts, 10);
    }
}
