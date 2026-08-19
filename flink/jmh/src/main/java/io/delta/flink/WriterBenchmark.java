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
import io.delta.flink.sink.DeltaWriterTask;
import io.delta.flink.table.HadoopCatalog;
import io.delta.flink.table.HadoopTable;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.openjdk.jmh.annotations.*;

import java.io.File;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 20, time = 1)
@Fork(1)
public class WriterBenchmark {

    @Setup(Level.Trial)
    public void setup() {
    }

    public void testLocalWriterThroughput() throws Exception {
        File tempDir = Files.createTempDirectory(UUID.randomUUID().toString()).toFile();

        StructType schema = new StructType()
                .add("id", IntegerType.INTEGER)
                .add("name", StringType.STRING);

        HadoopTable table = new HadoopTable(tempDir.toURI(), Map.of(), schema, List.of());
        table.open();
        RowType rowType = RowType.of(
                new IntType(),
                new VarCharType(VarCharType.MAX_LENGTH)
        );

        DeltaWriterTask task = new DeltaWriterTask(UUID.randomUUID().toString(), 0,0, table,
                new DeltaSinkConf(schema, Map.of()), Map.of());
        var context = new EmptyContext();
        for(int i = 0; i < 1000000; i++) {
            String random = UUID.randomUUID().toString();
            GenericRowData data = GenericRowData.of(i, StringData.fromString(random));

            RowDataSerializer serializer = new RowDataSerializer(rowType);
            task.write(serializer.toBinaryRow(data), context);
        }
        task.complete();

        table.close();
    }

    public void testS3WriterThroughput() throws Exception {
        String s3path = "s3://hao-extstaging/flink-benchmark/";

        StructType schema = new StructType()
                .add("id", IntegerType.INTEGER)
                .add("name", StringType.STRING);

        HadoopCatalog catalog = new HadoopCatalog(
                Map.of()
        );
        HadoopTable table = new HadoopTable(catalog, s3path, Map.of(), schema, List.of());
        catalog.setEngineLoader(table);
        table.open();
        RowType rowType = RowType.of(
                new IntType(),
                new VarCharType(VarCharType.MAX_LENGTH)
        );

        DeltaWriterTask task = new DeltaWriterTask(UUID.randomUUID().toString(), 0,0, table,
                new DeltaSinkConf(schema, Map.of()), Map.of());
        var context = new EmptyContext();
        for(int i = 0; i < 1000000; i++) {
            String random = UUID.randomUUID().toString();
            GenericRowData data = GenericRowData.of(i, StringData.fromString(random));

            RowDataSerializer serializer = new RowDataSerializer(rowType);
            task.write(serializer.toBinaryRow(data), context);
        }
        task.complete();

        table.close();
    }
}
