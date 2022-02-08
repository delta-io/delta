package io.delta.flink.example.sink;

import io.delta.flink.sink.DeltaSink;
import io.delta.flink.sink.DeltaSinkBuilder;
import io.delta.flink.sink.DeltaTablePartitionAssigner;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;

import java.nio.file.Paths;
import java.util.LinkedHashMap;

/**
 * Demonstrates how the Flink Delta Sink can be used to write data to a partitioned Delta table.
 *
 * If you run this example then application will spawn example local Flink job generating data to
 * the underlying Delta table under directory of "src/main/resources/example_table". The job will be
 * run in a daemon thread while in the main app's thread there will Delta Standalone application
 * reading and printing all the data to the std out.
 */
public class DeltaSinkPartitionedTableExample extends DeltaSinkExampleBase {

    public static String TABLE_PATH = Paths.get(".").toAbsolutePath().normalize() +
            "/src/main/resources/example_partitioned_table";

    public static void main(String[] args) throws Exception {
        new DeltaSinkPartitionedTableExample().run(TABLE_PATH);
    }

    @Override
    DeltaSink<RowData> getDeltaSink(String tablePath) {
        DeltaTablePartitionAssigner<RowData> partitionAssigner =
                new DeltaTablePartitionAssigner<>(new MultiplePartitioningColumnComputer());

        DeltaSinkBuilder<RowData> deltaSinkBuilder = DeltaSink.forRowData(
                new Path(TABLE_PATH), new Configuration(), ROW_TYPE);
        deltaSinkBuilder.withBucketAssigner(partitionAssigner);
        return deltaSinkBuilder.build();
    }

    static class MultiplePartitioningColumnComputer implements
            DeltaTablePartitionAssigner.DeltaPartitionComputer<RowData> {

        @Override
        public LinkedHashMap<String, String> generatePartitionValues(
                RowData element, BucketAssigner.Context context) {
            String f1 = element.getString(0).toString();
            int f3 = element.getInt(2);
            LinkedHashMap<String, String> partitionSpec = new LinkedHashMap<>();
            partitionSpec.put("f1", f1);
            partitionSpec.put("f3", Integer.toString(f3));
            return partitionSpec;
        }
    }
}
