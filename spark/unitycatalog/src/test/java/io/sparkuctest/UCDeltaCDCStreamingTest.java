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

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.Trigger;

/**
 * End-to-end reproduction for Change Data Feed (CDF) *streaming* reads on Unity-Catalog tables.
 *
 * <p>Runs against the real embedded UC server (via {@link UCDeltaTableIntegrationBaseTest}) under
 * the default V2_ENABLE_MODE=AUTO. A batch CDF read on a managed table already works (see
 * UCDeltaTableDataFrameReadTest#testChangeDataFeedViaDataFrameAPI); this exercises the *streaming*
 * CDF path, which is the one observed to fail with:
 *
 * <pre>
 *   java.lang.IllegalArgumentException: requirement failed:
 *     CDC read is not supported for schema bypass.
 *     at org.apache.spark.sql.delta.sources.DeltaDataSource.sourceSchema(DeltaDataSource.scala:127)
 * </pre>
 *
 * <p>{@code @TestAllTableTypes} runs both EXTERNAL and MANAGED:
 *
 * <ul>
 *   <li>EXTERNAL (path-based, V1 source) is expected to PASS — the normal V1 CDF streaming path.
 *   <li>MANAGED (catalogManaged) is the case under test. If the streaming read falls back to the V1
 *       {@code DeltaDataSource} schema-bypass path, the query throws the error above and this test
 *       FAILS — that failure IS the reproduction. If the read binds to the DSv2 CDC streaming
 *       source, the assertions below pass and CDF streaming on managed tables works end to end.
 * </ul>
 */
public class UCDeltaCDCStreamingTest extends UCDeltaTableIntegrationBaseTest {

  @TestAllTableTypes
  public void testChangeDataFeedStreamingRead(TableType tableType) throws Exception {
    withNewTable(
        "cdf_stream",
        "id INT",
        null,
        tableType,
        "'delta.enableChangeDataFeed'='true'",
        tableName -> {
          // Commit 1: baseline rows we do NOT want the stream to replay.
          sql("INSERT INTO %s VALUES (1), (2), (3)", tableName);
          long baselineVersion = currentVersion(tableName);

          // Commit 2: the change we expect the CDF stream to surface.
          sql("INSERT INTO %s VALUES (4), (5)", tableName);

          List<Integer> seenIds = new ArrayList<>();
          List<String> seenChangeTypes = new ArrayList<>();

          // Streaming CDF read starting after the baseline commit. On a MANAGED table this is the
          // call that has been throwing "CDC read is not supported for schema bypass".
          spark()
              .readStream()
              .format("delta")
              .option("readChangeFeed", "true")
              .option("startingVersion", baselineVersion + 1)
              .table(tableName)
              .writeStream()
              .trigger(Trigger.AvailableNow())
              .option("checkpointLocation", checkpoint())
              .foreachBatch(
                  (VoidFunction2<Dataset<Row>, Long>)
                      (df, batchId) -> {
                        for (Row r : df.select("id", "_change_type").collectAsList()) {
                          seenIds.add(r.getInt(0));
                          seenChangeTypes.add(r.getString(1));
                        }
                      })
              .start()
              .awaitTermination();

          // Expected once CDF streaming works on this table type: only the commit-2 inserts,
          // surfaced as insert change events.
          assertThat(seenIds).containsExactlyInAnyOrder(4, 5);
          assertThat(seenChangeTypes).containsOnly("insert");
        });
  }

  private int checkpointCounter = 0;

  /** Fresh checkpoint dir per streaming query within a test. */
  private String checkpoint() throws Exception {
    java.nio.file.Path dir =
        java.nio.file.Files.createTempDirectory("cdf_ck_" + (checkpointCounter++));
    return dir.toUri().toString();
  }
}
