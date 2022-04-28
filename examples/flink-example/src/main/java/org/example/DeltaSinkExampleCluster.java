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
package org.example;

import io.delta.flink.sink.DeltaSink;
import org.apache.commons.io.FileUtils;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

/**
 * Demonstrates how the Flink Delta Sink can be used to write data to Delta table.
 * <p>
 * This application is supposed to be run on a Flink cluster. When run it will start to generate
 * data to the underlying Delta table under directory of `/tmp/delta-flink-example/<UUID>`.
 */
public class DeltaSinkExampleCluster extends DeltaSinkExampleBase {

    static String TABLE_PATH = "/tmp/delta-flink-example/" +
                                   UUID.randomUUID().toString().replace("-", "");

    public static void main(String[] args) throws Exception {
        new DeltaSinkExampleCluster().run(TABLE_PATH);
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
    
    @Override
    void run(String tablePath) throws Exception {
        System.out.println("Will use table path: " + tablePath);
        File tableDir = new File(tablePath);
        if (tableDir.exists()) {
            FileUtils.cleanDirectory(tableDir);
        } else {
            tableDir.mkdirs();
        }
        StreamExecutionEnvironment env = getFlinkStreamExecutionEnvironment(tablePath, 1, 1);
        env.execute("TestJob");
    }
}
