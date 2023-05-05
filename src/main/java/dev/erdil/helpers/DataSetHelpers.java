package dev.erdil.helpers;

import org.apache.flink.api.java.DataSet;

public class DataSetHelpers {

    public static void printDatasetInfo(final DataSet<String> dataset, final int first) {
        try {
            System.out.printf("ItemCount: %d\n", dataset.count());
            System.out.printf("First %d items: %s", first, dataset.iterate(first).collect());
        } catch (Exception e) {
            throw new RuntimeException("Error occurred during handling the dataset.", e);
        }
    }
}
