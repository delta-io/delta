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
package org.utils.job;

import io.delta.flink.sink.DeltaSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.utils.Utils;

public abstract class DeltaSinkLocalJobExampleBase implements DeltaExampleLocalJobRunner {

    public void run(String tablePath) throws Exception {
        System.out.println("Will use table path: " + tablePath);

        Utils.prepareDirs(tablePath);
        StreamExecutionEnvironment env = createPipeline(tablePath, 2, 3);
        runFlinkJobInBackground(env);
        Utils.printDeltaTableRows(tablePath);
    }

    public abstract DeltaSink<RowData> getDeltaSink(String tablePath);

}
