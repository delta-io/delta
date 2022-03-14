package io.delta.flink.example.sink;

import io.delta.flink.sink.DeltaSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;

/**
 * Demonstrates how the Flink Delta Sink can be used to write data to Delta table.
 * <p>
 * If you run this example then application will spawn example local Flink job generating data to
 * the underlying Delta table under directory of "src/main/resources/example_table". The job will be
 * run in a daemon thread while in the main app's thread there will Delta Standalone application
 * reading and printing all the data to the std out.
 */
public class DeltaSinkExample extends DeltaSinkExampleBase {

    static String TABLE_PATH = resolveExampleTableAbsolutePath("example_table");

    public static void main(String[] args) throws Exception {
        new DeltaSinkExample().run(TABLE_PATH);
    }

    @Override
    DeltaSink<RowData> getDeltaSink(String tablePath) {
        return DeltaSink
            .forRowData(
                new Path(TABLE_PATH),
                new Configuration(),
                ROW_TYPE)
            .build();
    }
}
