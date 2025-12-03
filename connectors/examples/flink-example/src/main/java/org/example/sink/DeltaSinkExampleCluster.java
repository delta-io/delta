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

import java.util.UUID;

import io.delta.flink.sink.DeltaSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.utils.DeltaExampleSourceFunction;
import org.utils.Utils;
import org.utils.job.DeltaSinkClusterJobExampleBase;

/**
 * Demonstrates how the Flink Delta Sink can be used to write data to Delta table.
 * <p>
 * This application is supposed to be run on a Flink cluster. When run it will start to generate
 * data to the underlying Delta table under directory of `/tmp/delta-flink-example/<UUID>`.
 */
public class DeltaSinkExampleCluster extends DeltaSinkClusterJobExampleBase {

    static String TABLE_PATH = "/tmp/delta-flink-example/" +
                                   UUID.randomUUID().toString().replace("-", "");

    public static void main(String[] args) throws Exception {
        new DeltaSinkExampleCluster().run(TABLE_PATH);
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
     * An example of Flink Delta Sink configuration.
     */
    @Override
    public DeltaSink<RowData> getDeltaSink(String tablePath) {
        return DeltaSink
            .forRowData(
                new Path(TABLE_PATH),
                new Configuration(),
                Utils.FULL_SCHEMA_ROW_TYPE)
            .build();
    }
}
