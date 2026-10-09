/*
 *  Copyright (2026) The Delta Lake Project Authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.delta.flink;

import io.delta.flink.sink.DeltaSinkConf;
import io.delta.flink.sink.DeltaWriterResult;
import io.delta.flink.sink.DeltaWriterTask;
import io.delta.flink.table.*;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.openjdk.jmh.annotations.*;

import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 20, time = 1)
@Fork(1)
public class CommitterBenchmark {

    private static final URI CATALOG_ENDPOINT =
            URI.create("https://e2-dogfood.staging.cloud.databricks.com/");
    private static final String CATALOG_TOKEN = "";
    private static final String tableId = "main.hao.flink_write";

    private DeltaTable table;
    private StructType schema = new StructType()
            .add("id", IntegerType.INTEGER)
            .add("name", StringType.STRING);


    @Setup(Level.Trial)
    public void setup() throws Exception {
        table =
                new CatalogManagedTable(
                        new UnityCatalog("main", CATALOG_ENDPOINT, CATALOG_TOKEN),
                        tableId,
                        Map.of(TableConf.CHECKPOINT_FREQUENCY.key(), "0.3"),
                        CATALOG_ENDPOINT, CATALOG_TOKEN);
        table.open();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        table.close();
    }

    @Benchmark
    public void commitToUCTable() throws Exception {
        DeltaWriterTask writer = new DeltaWriterTask(UUID.randomUUID().toString(), 0, 0,
                table, new DeltaSinkConf(schema, Map.of()), Map.of());

        var context = new EmptyContext();
        for(int i = 0; i < 1000000; i++) {
            String random = UUID.randomUUID().toString();
            GenericRowData data = GenericRowData.of(i, StringData.fromString(random));
            writer.write(data, context);
        }
        var results = writer.complete();

        var actions = results.stream()
                .flatMap(result -> result.getDeltaActions().stream())
                .collect(Collectors.toList());
        var iter = CloseableIterable.inMemoryIterable(Utils.toCloseableIterator(actions.iterator()));
        table.commit(iter, "action", System.currentTimeMillis(), Map.of());
    }
}
