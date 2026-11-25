/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package org.apache.spark.sql.delta.v2.utils;

import static io.delta.kernel.internal.actions.AddFile.SCHEMA_WITHOUT_STATS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.util.VectorUtils;
import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.Snapshot;
import org.apache.spark.sql.delta.actions.AddFile;
import org.apache.spark.sql.delta.v2.DeltaV2TestBase;
import org.apache.spark.sql.delta.v2.snapshot.PathBasedSnapshotManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import scala.Option;

public class StreamingHelperTest extends DeltaV2TestBase {

  private PathBasedSnapshotManager snapshotManager;

  @Override
  protected boolean clearTableManagerCacheAfterEach() {
    return true;
  }

  @Test
  public void testGetAddFileWithDataChange() {
    ColumnVector adds =
        VectorUtils.buildColumnVector(
            Arrays.asList(createAddFileRow(true), createAddFileRow(false)), SCHEMA_WITHOUT_STATS);
    ColumnarBatch batch = createAddFileBatch(adds);

    Optional<AddFile> addFile = StreamingHelper.getAddFileWithDataChange(batch, 0);
    assertTrue(addFile.isPresent());
    assertEquals("file.parquet", addFile.get().path());
    assertTrue(addFile.get().dataChange());
    assertFalse(StreamingHelper.getAddFileWithDataChange(batch, 1).isPresent());

    ColumnVector absentAdd =
        VectorUtils.buildColumnVector(Collections.singletonList(null), SCHEMA_WITHOUT_STATS);
    ColumnarBatch absentBatch = createAddFileBatch(absentAdd);
    assertFalse(StreamingHelper.getAddFileWithDataChange(absentBatch, 0).isPresent());
  }

  private static ColumnarBatch createAddFileBatch(ColumnVector adds) {
    return new ColumnarBatch() {
      @Override
      public io.delta.kernel.types.StructType getSchema() {
        return new io.delta.kernel.types.StructType().add("add", SCHEMA_WITHOUT_STATS);
      }

      @Override
      public ColumnVector getColumnVector(int ordinal) {
        return adds;
      }

      @Override
      public int getSize() {
        return adds.getSize();
      }
    };
  }

  private static Row createAddFileRow(boolean dataChange) {
    return new GenericRow(
        SCHEMA_WITHOUT_STATS,
        Map.of(
            SCHEMA_WITHOUT_STATS.indexOf("path"),
            "file.parquet",
            SCHEMA_WITHOUT_STATS.indexOf("partitionValues"),
            VectorUtils.stringStringMapValue(Map.of()),
            SCHEMA_WITHOUT_STATS.indexOf("size"),
            1L,
            SCHEMA_WITHOUT_STATS.indexOf("modificationTime"),
            1L,
            SCHEMA_WITHOUT_STATS.indexOf("dataChange"),
            dataChange));
  }

  @Test
  public void testUnsafeVolatileSnapshot(@TempDir File tempDir) {
    String testTablePath = tempDir.getAbsolutePath();
    String testTableName = "test_volatile_snapshot";
    createEmptyTestTable(testTablePath, testTableName);
    snapshotManager = new PathBasedSnapshotManager(testTablePath, createKernelContext());
    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(testTablePath));
    org.apache.spark.sql.delta.Snapshot deltaSnapshot = deltaLog.unsafeVolatileSnapshot();
    Snapshot kernelSnapshot = snapshotManager.loadLatestSnapshot();

    spark.sql(String.format("INSERT INTO %s VALUES (4, 'David')", testTableName));

    assertEquals(0L, deltaSnapshot.version());
    assertEquals(deltaSnapshot.version(), kernelSnapshot.version());
  }

  @Test
  public void testLoadLatestSnapshot(@TempDir File tempDir) {
    String testTablePath = tempDir.getAbsolutePath();
    String testTableName = "test_update";
    createEmptyTestTable(testTablePath, testTableName);
    snapshotManager = new PathBasedSnapshotManager(testTablePath, createKernelContext());
    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(testTablePath));

    Snapshot initialSnapshot = snapshotManager.loadLatestSnapshot();
    assertEquals(0L, initialSnapshot.version());

    spark.sql(String.format("INSERT INTO %s VALUES (4, 'David')", testTableName));

    org.apache.spark.sql.delta.Snapshot deltaSnapshot =
        deltaLog.update(false, Option.empty(), Option.empty());
    Snapshot updatedSnapshot = snapshotManager.loadLatestSnapshot();
    org.apache.spark.sql.delta.Snapshot cachedSnapshot = deltaLog.unsafeVolatileSnapshot();
    Snapshot kernelcachedSnapshot = snapshotManager.loadLatestSnapshot();

    assertEquals(1L, updatedSnapshot.version());
    assertEquals(deltaSnapshot.version(), updatedSnapshot.version());
    assertEquals(1L, kernelcachedSnapshot.version());
    assertEquals(cachedSnapshot.version(), kernelcachedSnapshot.version());
  }

  @Test
  public void testMultipleLoadLatestSnapshot(@TempDir File tempDir) {
    String testTablePath = tempDir.getAbsolutePath();
    String testTableName = "test_multiple_updates";
    createEmptyTestTable(testTablePath, testTableName);
    snapshotManager = new PathBasedSnapshotManager(testTablePath, createKernelContext());

    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(testTablePath));

    assertEquals(0L, snapshotManager.loadLatestSnapshot().version());

    for (int i = 0; i < 3; i++) {
      spark.sql(
          String.format("INSERT INTO %s VALUES (%d, 'User%d')", testTableName, 20 + i, 20 + i));

      org.apache.spark.sql.delta.Snapshot deltaSnapshot =
          deltaLog.update(false, Option.empty(), Option.empty());
      Snapshot kernelSnapshot = snapshotManager.loadLatestSnapshot();

      long expectedVersion = i + 1;
      assertEquals(expectedVersion, deltaSnapshot.version());
      assertEquals(expectedVersion, kernelSnapshot.version());
    }
  }
}
