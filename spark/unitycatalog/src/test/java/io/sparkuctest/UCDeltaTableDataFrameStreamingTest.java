/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package io.sparkuctest;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.Trigger;

/**
 * DataFrame streaming test suite for Delta Table operations through Unity Catalog.
 *
 * <p>Covers streaming read and write via Structured Streaming. Uses {@link Trigger#AvailableNow()}
 * for deterministic, testable streaming without manual termination. Tests run against both EXTERNAL
 * and MANAGED table types.
 */
public class UCDeltaTableDataFrameStreamingTest extends UCDeltaTableIntegrationBaseTest {

  @TestAllTableTypes
  public void testStreamingRead(TableType tableType) throws Exception {
    withNewTable(
        "streaming_read_test",
        "id INT",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1), (2), (3)", tableName);
          List<Integer> result = new ArrayList<>();
          spark()
              .readStream()
              .format("delta")
              .table(tableName)
              .writeStream()
              .trigger(Trigger.AvailableNow())
              .option("checkpointLocation", checkpoint())
              .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (df, id) -> result.addAll(ids(df)))
              .start()
              .awaitTermination();
          assertThat(result).containsExactlyInAnyOrder(1, 2, 3);
        });
  }

  @TestAllTableTypes
  public void testStreamingWrite(TableType tableType) throws Exception {
    withNewTable(
        "streaming_write_src",
        "id INT",
        tableType,
        srcName ->
            withNewTable(
                "streaming_write_sink",
                "id INT",
                tableType,
                sinkName -> {
                  sql("INSERT INTO %s VALUES (1), (2), (3)", srcName);
                  spark()
                      .readStream()
                      .format("delta")
                      .table(srcName)
                      .writeStream()
                      .format("delta")
                      .outputMode("append")
                      .trigger(Trigger.AvailableNow())
                      .option("checkpointLocation", checkpoint())
                      .toTable(sinkName)
                      .awaitTermination();
                  check(sinkName, List.of(row("1"), row("2"), row("3")));
                }));
  }

  @TestAllTableTypes
  public void testStreamingReadFromVersion(TableType tableType) throws Exception {
    withNewTable(
        "streaming_version_test",
        "id INT",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1), (2), (3)", tableName);
          long v1 = currentVersion(tableName);
          sql("INSERT INTO %s VALUES (4), (5)", tableName);
          List<Integer> result = new ArrayList<>();
          spark()
              .readStream()
              .format("delta")
              .option("startingVersion", v1 + 1)
              .table(tableName)
              .writeStream()
              .trigger(Trigger.AvailableNow())
              .option("checkpointLocation", checkpoint())
              .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (df, id) -> result.addAll(ids(df)))
              .start()
              .awaitTermination();
          assertThat(result).containsExactlyInAnyOrder(4, 5);
        });
  }

  private String checkpoint() throws IOException {
    return Files.createTempDirectory("delta-streaming-ck-").toString();
  }

  private List<Integer> ids(Dataset<Row> df) {
    return df.collectAsList().stream().map(r -> r.getInt(0)).collect(Collectors.toList());
  }

  private long currentVersion(String tableName) {
    return Long.parseLong(sql("DESCRIBE HISTORY %s LIMIT 1", tableName).get(0).get(0));
  }

  private static List<String> row(String... values) {
    return List.of(values);
  }
}
