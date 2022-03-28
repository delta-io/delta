/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package example;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Internal class providing mock implementation for example stream source.
 * <p>
 * This streaming source will be generating events of type {@link DeltaSinkExample.ROW_TYPE} with
 * interval of {@link DeltaSinkExampleSourceFunction.NEXT_ROW_INTERVAL_MILLIS} that will be further
 * fed to the Flink job until the parent process is stopped.
 */
public class DeltaSinkExampleSourceFunction extends RichParallelSourceFunction<RowData> {

    static int NEXT_ROW_INTERVAL_MILLIS = 800;

    public static final DataFormatConverters.DataFormatConverter<RowData, Row> CONVERTER =
            DataFormatConverters.getConverterForDataType(
                    TypeConversions.fromLogicalToDataType(DeltaSinkExample.ROW_TYPE)
            );

    private volatile boolean cancelled = false;

    @Override
    public void run(SourceContext<RowData> ctx) throws InterruptedException {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        while (!cancelled) {

            RowData row = CONVERTER.toInternal(
                    Row.of(
                            String.valueOf(random.nextInt(0, 10)),
                            String.valueOf(random.nextInt(0, 100)),
                            random.nextInt(0, 30))
            );
            ctx.collect(row);
            Thread.sleep(NEXT_ROW_INTERVAL_MILLIS);
        }
    }

    @Override
    public void cancel() {
        cancelled = true;
    }
}
