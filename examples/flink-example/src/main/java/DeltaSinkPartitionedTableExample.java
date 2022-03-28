/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package example;

import io.delta.flink.sink.DeltaSink;
import io.delta.flink.sink.RowDataDeltaSinkBuilder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;

/**
 * Demonstrates how the Flink Delta Sink can be used to write data to a partitioned Delta table.
 * <p>
 * If you run this example then application will spawn example local Flink job generating data to
 * the underlying Delta table under directory of "src/main/resources/example_table". The job will be
 * run in a daemon thread while in the main app's thread there will Delta Standalone application
 * reading and printing all the data to the std out.
 */
public class DeltaSinkPartitionedTableExample extends DeltaSinkExampleBase {

    static String TABLE_PATH = resolveExampleTableAbsolutePath("example_partitioned_table");

    public static void main(String[] args) throws Exception {
        new DeltaSinkPartitionedTableExample().run(TABLE_PATH);
    }

    @Override
    DeltaSink<RowData> getDeltaSink(String tablePath) {
        String[] partitionCols = {"f1"};

        RowDataDeltaSinkBuilder deltaSinkBuilder = DeltaSink.forRowData(
            new Path(TABLE_PATH), new Configuration(), ROW_TYPE);
        deltaSinkBuilder.withPartitionColumns(partitionCols);
        return deltaSinkBuilder.build();
    }
}
