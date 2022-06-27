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
package org.example.sink;

import io.delta.flink.sink.DeltaSink;
import io.delta.flink.sink.RowDataDeltaSinkBuilder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.utils.DeltaExampleSourceFunction;
import org.utils.Utils;
import org.utils.job.DeltaSinkLocalJobExampleBase;

/**
 * Demonstrates how the Flink Delta Sink can be used to write data to a partitioned Delta table.
 * <p>
 * If you run this example then application will spawn example local Flink job generating data to
 * the underlying Delta table under directory of "src/main/resources/example_table". The job will be
 * run in a daemon thread while in the main app's thread there will Delta Standalone application
 * reading and printing all the data to the std out.
 */
public class DeltaSinkPartitionedTableExample extends DeltaSinkLocalJobExampleBase {

    static String TABLE_PATH = Utils.resolveExampleTableAbsolutePath("example_partitioned_table");

    public static void main(String[] args) throws Exception {
        new DeltaSinkPartitionedTableExample().run(TABLE_PATH);
    }

    /**
     * An example of using Flink Delta Sink in streaming pipeline.
     */
    @Override
    public StreamExecutionEnvironment createPipeline(
            String tablePath,
            int sourceParallelism,
            int sinkParallelism) {

        DeltaSink<RowData> deltaSink = getDeltaSink(tablePath);
        StreamExecutionEnvironment env = getStreamExecutionEnvironment();

        // Using Flink Delta Sink in processing pipeline
        env
            .addSource(new DeltaExampleSourceFunction())
            .setParallelism(sourceParallelism)
            .sinkTo(deltaSink)
            .name("MyDeltaSink")
            .setParallelism(sinkParallelism);

        return env;
    }

    /**
     * An example of Flink Delta Sink configuration with partition column.
     */
    @Override
    public DeltaSink<RowData> getDeltaSink(String tablePath) {
        String[] partitionCols = {"f1"};

        RowDataDeltaSinkBuilder deltaSinkBuilder = DeltaSink.forRowData(
            new Path(TABLE_PATH), new Configuration(), Utils.FULL_SCHEMA_ROW_TYPE);
        deltaSinkBuilder.withPartitionColumns(partitionCols);
        return deltaSinkBuilder.build();
    }
}
