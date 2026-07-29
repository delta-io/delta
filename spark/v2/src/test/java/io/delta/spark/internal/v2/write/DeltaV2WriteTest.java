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
package io.delta.spark.internal.v2.write;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.Snapshot;
import io.delta.spark.internal.v2.DeltaV2TestBase;
import io.delta.spark.internal.v2.snapshot.PathBasedSnapshotManager;
import java.io.File;
import java.util.Map;
import org.apache.spark.sql.connector.distributions.UnspecifiedDistribution;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Unit tests for {@link DeltaV2Write}. */
public class DeltaV2WriteTest extends DeltaV2TestBase {

  private static final StructType TABLE_SCHEMA =
      DataTypes.createStructType(
          new StructField[] {
            DataTypes.createStructField("id", DataTypes.IntegerType, true),
            DataTypes.createStructField("name", DataTypes.StringType, true)
          });

  @Test
  public void testToBatch_returnsDeltaV2BatchWrite(@TempDir File tempDir) {
    String path = createTable(tempDir, "write_to_batch");
    DeltaV2Write write = newWrite(path, CaseInsensitiveStringMap.empty());

    assertTrue(write.toBatch() instanceof DeltaV2BatchWrite);
  }

  @Test
  public void testToStreaming_returnsDeltaV2StreamingWrite(@TempDir File tempDir) {
    String path = createTable(tempDir, "write_to_streaming");
    DeltaV2Write write = newWrite(path, CaseInsensitiveStringMap.empty());

    assertTrue(write.toStreaming() instanceof DeltaV2StreamingWrite);
  }

  @Test
  public void testToStreaming_rejectsUnsupportedOptions(@TempDir File tempDir) {
    String path = createTable(tempDir, "write_rejects_options");

    // Iterate the production list so a change there (add/remove an option) is covered without
    // updating a separate copy here.
    for (String option : DeltaV2Write.UNSUPPORTED_STREAMING_OPTIONS) {
      CaseInsensitiveStringMap options = new CaseInsensitiveStringMap(Map.of(option, "x"));
      DeltaV2Write write = newWrite(path, options);
      assertThrows(
          UnsupportedOperationException.class,
          write::toStreaming,
          "streaming write should reject option: " + option);
    }
  }

  @Test
  public void testDistributionAndOrdering_unpartitionedRequestsNeither(@TempDir File tempDir) {
    String path = createTable(tempDir, "write_dist_unpartitioned");
    DeltaV2Write write = newWrite(path, CaseInsensitiveStringMap.empty());

    // Unpartitioned: no distribution (never a shuffle) and no ordering.
    assertInstanceOf(UnspecifiedDistribution.class, write.requiredDistribution());
    assertEquals(0, write.requiredOrdering().length);
  }

  @Test
  public void testDistributionAndOrdering_partitionedSortsWithoutDistribution(
      @TempDir File tempDir) {
    // Table (id, name) partitioned by (name). We still request no distribution (no shuffle), only a
    // local sort on the partition column.
    String path = tempDir.getAbsolutePath();
    spark.sql(
        String.format(
            "CREATE TABLE write_dist_part_%d (id INT, name STRING) USING delta "
                + "PARTITIONED BY (name) LOCATION '%s'",
            System.nanoTime(), path));
    StructType dataSchema =
        DataTypes.createStructType(
            new StructField[] {DataTypes.createStructField("id", DataTypes.IntegerType, true)});
    StructType partitionSchema =
        DataTypes.createStructType(
            new StructField[] {DataTypes.createStructField("name", DataTypes.StringType, true)});
    PathBasedSnapshotManager mgr =
        new PathBasedSnapshotManager(path, spark.sessionState().newHadoopConf());
    DeltaV2Write write =
        new DeltaV2Write(
            defaultEngine,
            spark.sessionState().newHadoopConf(),
            path,
            mgr.loadLatestSnapshot(),
            mgr,
            dataSchema,
            partitionSchema,
            WriteTestUtils.logicalWriteInfo(TABLE_SCHEMA, CaseInsensitiveStringMap.empty()));

    assertInstanceOf(UnspecifiedDistribution.class, write.requiredDistribution());
    SortOrder[] ordering = write.requiredOrdering();
    assertEquals(1, ordering.length);
    NamedReference ref = (NamedReference) ordering[0].expression();
    assertArrayEquals(new String[] {"name"}, ref.fieldNames());
  }

  @Test
  public void testDistributionAndOrdering_multipleColumnsSortInPartitionOrder(
      @TempDir File tempDir) {
    // Partition schema (p2, p1): the ordering must follow partition-schema order, not table order.
    String path = tempDir.getAbsolutePath();
    spark.sql(
        String.format(
            "CREATE TABLE write_dist_multi_%d (id INT, p1 STRING, p2 INT) USING delta "
                + "PARTITIONED BY (p2, p1) LOCATION '%s'",
            System.nanoTime(), path));
    StructType dataSchema =
        DataTypes.createStructType(
            new StructField[] {DataTypes.createStructField("id", DataTypes.IntegerType, true)});
    StructType partitionSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("p2", DataTypes.IntegerType, true),
              DataTypes.createStructField("p1", DataTypes.StringType, true)
            });
    StructType fullSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, true),
              DataTypes.createStructField("p1", DataTypes.StringType, true),
              DataTypes.createStructField("p2", DataTypes.IntegerType, true)
            });
    PathBasedSnapshotManager mgr =
        new PathBasedSnapshotManager(path, spark.sessionState().newHadoopConf());
    DeltaV2Write write =
        new DeltaV2Write(
            defaultEngine,
            spark.sessionState().newHadoopConf(),
            path,
            mgr.loadLatestSnapshot(),
            mgr,
            dataSchema,
            partitionSchema,
            WriteTestUtils.logicalWriteInfo(fullSchema, CaseInsensitiveStringMap.empty()));

    assertInstanceOf(UnspecifiedDistribution.class, write.requiredDistribution());
    SortOrder[] ordering = write.requiredOrdering();
    assertEquals(2, ordering.length);
    assertArrayEquals(
        new String[] {"p2"}, ((NamedReference) ordering[0].expression()).fieldNames());
    assertArrayEquals(
        new String[] {"p1"}, ((NamedReference) ordering[1].expression()).fieldNames());
  }

  private String createTable(File tempDir, String tableName) {
    String path = tempDir.getAbsolutePath();
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, name STRING) USING delta LOCATION '%s'", tableName, path));
    return path;
  }

  private DeltaV2Write newWrite(String path, CaseInsensitiveStringMap options) {
    PathBasedSnapshotManager snapshotManager =
        new PathBasedSnapshotManager(path, spark.sessionState().newHadoopConf());
    Snapshot snapshot = snapshotManager.loadLatestSnapshot();
    LogicalWriteInfo info = WriteTestUtils.logicalWriteInfo(TABLE_SCHEMA, options);
    return new DeltaV2Write(
        defaultEngine,
        spark.sessionState().newHadoopConf(),
        path,
        snapshot,
        snapshotManager,
        TABLE_SCHEMA,
        new StructType(),
        info);
  }
}
