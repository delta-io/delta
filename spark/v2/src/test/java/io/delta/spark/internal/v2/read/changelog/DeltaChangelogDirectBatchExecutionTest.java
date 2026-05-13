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
package io.delta.spark.internal.v2.read.changelog;

import io.delta.spark.internal.v2.DeltaV2TestBase;
import io.delta.spark.internal.v2.snapshot.DeltaSnapshotManager;
import io.delta.spark.internal.v2.snapshot.SnapshotManagerFactory;
import java.sql.Timestamp;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;

/**
 * Direct (non-analyzer) batch execution contract test for Delta changelog classes.
 *
 * <p>This is intentionally strict with explicit expected rows to validate end-to-end behavior
 * through ScanBuilder -> Scan -> Batch -> PartitionReader.
 */
public class DeltaChangelogDirectBatchExecutionTest extends DeltaV2TestBase {

  @Test
  public void testDirectBatchExecutionWithExplicitExpectedRows() throws Exception {
    String tableName = "dsv2_changelog_direct_" + System.nanoTime();
    String tablePath = System.getProperty("java.io.tmpdir") + "/" + tableName;

    withTable(tablePath, () -> {
      spark.sql(
          String.format(
              "CREATE TABLE %s (id BIGINT, name STRING) USING delta "
                  + "LOCATION '%s' TBLPROPERTIES "
                  + "('delta.enableDeletionVectors'='false', 'delta.enableRowTracking'='true')",
              tableName, tablePath));
      spark.sql(String.format("INSERT INTO %s VALUES (1, 'Alice'), (2, 'Bob')", tableName));
      spark.sql(String.format("DELETE FROM %s WHERE id = 1", tableName));

      DeltaSnapshotManager snapshotManager =
          SnapshotManagerFactory.create(tablePath, defaultEngine, Optional.empty());
      StructType dataSchema = spark.table(tableName).schema();
      long latestVersion = snapshotManager.loadLatestSnapshot().getVersion();
      Map<Long, Long> commitTimestampsMicros = loadCommitTimestampsMicros(tableName);

      DeltaChangelog changelog =
          new DeltaChangelog(tableName, dataSchema, snapshotManager, 0L, latestVersion);
      ScanBuilder scanBuilder =
          changelog.newScanBuilder(new CaseInsensitiveStringMap(Collections.emptyMap()));
      Scan scan = scanBuilder.build();
      StructType schema = scan.readSchema();
      Batch batch = scan.toBatch();

      List<InternalRow> actualRows = collectRows(batch);
      List<ExpectedRow> expectedRows =
          expectedRows(commitTimestampsMicros.get(1L), commitTimestampsMicros.get(2L));

      // Schema-level contract checks for CDC metadata columns.
      List<String> fieldNames = Arrays.asList(schema.fieldNames());
      assertTrue(fieldNames.contains("_change_type"));
      assertTrue(fieldNames.contains("_commit_version"));
      assertTrue(fieldNames.contains("_commit_timestamp"));
      assertTrue(fieldNames.contains("id"));
      assertTrue(fieldNames.contains("name"));

      int idIndex = fieldNames.indexOf("id");
      int nameIndex = fieldNames.indexOf("name");
      int changeTypeIndex = fieldNames.indexOf("_change_type");
      int commitVersionIndex = fieldNames.indexOf("_commit_version");
      int commitTimestampIndex = fieldNames.indexOf("_commit_timestamp");

      // Explicit row-value checks.
      assertEquals(expectedRows.size(), actualRows.size());
      assertRowsEqual(
          actualRows,
          expectedRows,
          idIndex,
          nameIndex,
          changeTypeIndex,
          commitVersionIndex,
          commitTimestampIndex);
    });
  }

  /**
   * Asserts that {@link DeltaChangelog#rowId()} and {@link DeltaChangelog#rowVersion()} expose the
   * per-row tracking metadata fields (not the per-commit columns). Spark's batch CDC
   * post-processor reads these references to perform carry-over removal and update detection. If
   * {@code rowVersion()} ever pointed at {@code _commit_version} instead of
   * {@code _metadata.row_commit_version}, real updates whose DELETE/INSERT halves share the same
   * commit version would be silently dropped as carry-overs.
   */
  @Test
  public void testRowTrackingMetadataReferences() throws Exception {
    String tableName = "dsv2_changelog_direct_meta_" + System.nanoTime();
    String tablePath = System.getProperty("java.io.tmpdir") + "/" + tableName;

    withTable(tablePath, () -> {
      spark.sql(
          String.format(
              "CREATE TABLE %s (id BIGINT) USING delta LOCATION '%s' TBLPROPERTIES "
                  + "('delta.enableDeletionVectors'='false', 'delta.enableRowTracking'='true')",
              tableName, tablePath));
      spark.sql(String.format("INSERT INTO %s VALUES (1)", tableName));

      DeltaSnapshotManager snapshotManager =
          SnapshotManagerFactory.create(tablePath, defaultEngine, Optional.empty());
      StructType dataSchema = spark.table(tableName).schema();
      long latestVersion = snapshotManager.loadLatestSnapshot().getVersion();

      DeltaChangelog changelog =
          new DeltaChangelog(tableName, dataSchema, snapshotManager, 0L, latestVersion);

      NamedReference[] rowIdRefs = changelog.rowId();
      assertEquals(1, rowIdRefs.length, "Expected a single rowId field reference");
      assertArrayEquals(
          new String[] {"_metadata", "row_id"},
          rowIdRefs[0].fieldNames(),
          "rowId() must point at _metadata.row_id");
      assertArrayEquals(
          new String[] {"_metadata", "row_commit_version"},
          changelog.rowVersion().fieldNames(),
          "rowVersion() must point at _metadata.row_commit_version (per-row), "
              + "not _commit_version (per-commit)");
    });
  }

  /**
   * UPDATE on a row-tracking table is CoW: the file containing the updated row is rewritten,
   * which produces both a RemoveFile (old contents) and an AddFile (new contents) at the same
   * commit. The connector must surface both halves of that pair as raw DELETE + INSERT rows so
   * Spark's post-processor can derive the update.
   */
  @Test
  public void testUpdateProducesPairedDeleteAndInsert() throws Exception {
    String tableName = "dsv2_changelog_direct_update_" + System.nanoTime();
    String tablePath = System.getProperty("java.io.tmpdir") + "/" + tableName;

    withTable(tablePath, () -> {
      spark.sql(
          String.format(
              "CREATE TABLE %s (id BIGINT, name STRING) USING delta LOCATION '%s' "
                  + "TBLPROPERTIES "
                  + "('delta.enableDeletionVectors'='false', 'delta.enableRowTracking'='true')",
              tableName, tablePath));
      spark.sql(String.format("INSERT INTO %s VALUES (1, 'Alice')", tableName));
      spark.sql(String.format("UPDATE %s SET name = 'AliceX' WHERE id = 1", tableName));

      DeltaSnapshotManager snapshotManager =
          SnapshotManagerFactory.create(tablePath, defaultEngine, Optional.empty());
      StructType dataSchema = spark.table(tableName).schema();
      long latestVersion = snapshotManager.loadLatestSnapshot().getVersion();
      Map<Long, Long> commitTimestampsMicros = loadCommitTimestampsMicros(tableName);

      DeltaChangelog changelog =
          new DeltaChangelog(tableName, dataSchema, snapshotManager, 0L, latestVersion);
      Scan scan =
          changelog
              .newScanBuilder(new CaseInsensitiveStringMap(Collections.emptyMap()))
              .build();
      Batch batch = scan.toBatch();
      StructType schema = scan.readSchema();

      List<InternalRow> actualRows = collectRows(batch);

      List<String> fieldNames = Arrays.asList(schema.fieldNames());
      int idIndex = fieldNames.indexOf("id");
      int nameIndex = fieldNames.indexOf("name");
      int changeTypeIndex = fieldNames.indexOf("_change_type");
      int commitVersionIndex = fieldNames.indexOf("_commit_version");
      int commitTimestampIndex = fieldNames.indexOf("_commit_timestamp");

      List<ExpectedRow> expectedRows = new ArrayList<>();
      // v1: initial INSERT
      expectedRows.add(row(1L, "Alice", "insert", 1L, commitTimestampsMicros.get(1L)));
      // v2: UPDATE -> DELETE old + INSERT new at the same commit
      expectedRows.add(row(1L, "Alice", "delete", 2L, commitTimestampsMicros.get(2L)));
      expectedRows.add(row(1L, "AliceX", "insert", 2L, commitTimestampsMicros.get(2L)));

      assertEquals(expectedRows.size(), actualRows.size(),
          "Update should produce a paired DELETE + INSERT at v2 alongside the v1 insert");
      assertRowsEqual(
          actualRows,
          expectedRows,
          idIndex,
          nameIndex,
          changeTypeIndex,
          commitVersionIndex,
          commitTimestampIndex);
    });
  }

  /**
   * Construction with a non-zero start version must restrict the scan to commits within
   * [start, end]. Specifically: actions from earlier commits must not appear in the output. This
   * exercises {@code planInputPartitions} commit-file slicing rather than the full-history
   * happy path.
   */
  @Test
  public void testRangeSlicingNonZeroStart() throws Exception {
    String tableName = "dsv2_changelog_direct_slice_" + System.nanoTime();
    String tablePath = System.getProperty("java.io.tmpdir") + "/" + tableName;

    withTable(tablePath, () -> {
      spark.sql(
          String.format(
              "CREATE TABLE %s (id BIGINT, name STRING) USING delta LOCATION '%s' "
                  + "TBLPROPERTIES "
                  + "('delta.enableDeletionVectors'='false', 'delta.enableRowTracking'='true')",
              tableName, tablePath));
      spark.sql(String.format("INSERT INTO %s VALUES (1, 'Alice')", tableName));
      spark.sql(String.format("INSERT INTO %s VALUES (2, 'Bob')", tableName));
      spark.sql(String.format("INSERT INTO %s VALUES (3, 'Charlie')", tableName));

      DeltaSnapshotManager snapshotManager =
          SnapshotManagerFactory.create(tablePath, defaultEngine, Optional.empty());
      StructType dataSchema = spark.table(tableName).schema();
      Map<Long, Long> commitTimestampsMicros = loadCommitTimestampsMicros(tableName);

      // Range = [v2, v3]. v0 (CREATE) and v1 (Alice) must be excluded from the output.
      DeltaChangelog changelog =
          new DeltaChangelog(tableName, dataSchema, snapshotManager, 2L, 3L);
      Scan scan =
          changelog
              .newScanBuilder(new CaseInsensitiveStringMap(Collections.emptyMap()))
              .build();
      Batch batch = scan.toBatch();
      StructType schema = scan.readSchema();

      List<InternalRow> actualRows = collectRows(batch);

      List<String> fieldNames = Arrays.asList(schema.fieldNames());
      int idIndex = fieldNames.indexOf("id");
      int nameIndex = fieldNames.indexOf("name");
      int changeTypeIndex = fieldNames.indexOf("_change_type");
      int commitVersionIndex = fieldNames.indexOf("_commit_version");
      int commitTimestampIndex = fieldNames.indexOf("_commit_timestamp");

      List<ExpectedRow> expectedRows = new ArrayList<>();
      expectedRows.add(row(2L, "Bob", "insert", 2L, commitTimestampsMicros.get(2L)));
      expectedRows.add(row(3L, "Charlie", "insert", 3L, commitTimestampsMicros.get(3L)));

      assertEquals(expectedRows.size(), actualRows.size(),
          "Sliced range [v2, v3] must exclude v0/v1 actions");
      assertRowsEqual(
          actualRows,
          expectedRows,
          idIndex,
          nameIndex,
          changeTypeIndex,
          commitVersionIndex,
          commitTimestampIndex);
    });
  }

  private static List<InternalRow> collectRows(Batch batch) throws Exception {
    List<InternalRow> out = new ArrayList<>();
    InputPartition[] partitions = batch.planInputPartitions();

    for (InputPartition partition : partitions) {
      // Use a fresh factory per partition to avoid reusing stateful prefetch readers in
      // this direct in-process harness.
      PartitionReaderFactory readerFactory = batch.createReaderFactory();
      PartitionReader<InternalRow> reader = readerFactory.createReader(partition);
      try {
        while (reader.next()) {
          // Reader rows can be reused/mutable.
          out.add(reader.get().copy());
        }
      } finally {
        reader.close();
      }
    }
    return out;
  }

  private static List<ExpectedRow> expectedRows(
      long insertCommitTimestampMicros, long deleteCommitTimestampMicros) {
    List<ExpectedRow> rows = new ArrayList<>();
    // id, name, _change_type, _commit_version, _commit_timestamp
    //
    // Spark's INSERT VALUES splits the two rows into separate Delta data files (one row per
    // file, due to default shuffle partitioning). DELETE WHERE id=1 then affects only Alice's
    // file: it emits a RemoveFile for that file (preimage = Alice) and writes no AddFile
    // because the surviving row count is 0. Bob's file is left untouched, so Bob does not
    // appear in the v2 change set.
    rows.add(row(1L, "Alice", "insert", 1L, insertCommitTimestampMicros));
    rows.add(row(2L, "Bob", "insert", 1L, insertCommitTimestampMicros));
    rows.add(row(1L, "Alice", "delete", 2L, deleteCommitTimestampMicros));
    return rows;
  }

  private Map<Long, Long> loadCommitTimestampsMicros(String tableName) {
    List<Row> history = spark.sql(String.format("DESCRIBE HISTORY %s", tableName)).collectAsList();
    Map<Long, Long> versionToTimestampMicros = new HashMap<>();
    for (Row row : history) {
      long version = ((Number) row.getAs("version")).longValue();
      Timestamp ts = row.getAs("timestamp");
      versionToTimestampMicros.put(version, ts.getTime() * 1000L);
    }
    return versionToTimestampMicros;
  }

  private static ExpectedRow row(
      long id, String name, String changeType, long commitVersion, long commitTimestampMicros) {
    return new ExpectedRow(id, name, changeType, commitVersion, commitTimestampMicros);
  }

  private static void assertRowsEqual(
      List<InternalRow> actual,
      List<ExpectedRow> expected,
      int idIndex,
      int nameIndex,
      int changeTypeIndex,
      int commitVersionIndex,
      int commitTimestampIndex) {
    for (int i = 0; i < expected.size(); i++) {
      InternalRow a = actual.get(i);
      ExpectedRow e = expected.get(i);
      assertEquals(e.id, a.getLong(idIndex), "id mismatch at row " + i);
      assertEquals(
          e.name, a.getUTF8String(nameIndex).toString(), "name mismatch at row " + i);
      assertEquals(
          e.changeType,
          a.getUTF8String(changeTypeIndex).toString(),
          "change_type mismatch at row " + i);
      assertEquals(
          e.commitVersion, a.getLong(commitVersionIndex),
          "commit_version mismatch at row " + i);
      assertEquals(
          e.commitTimestampMicros, a.getLong(commitTimestampIndex),
          "commit_timestamp mismatch at row " + i);
    }
  }

  private static class ExpectedRow {
    private final long id;
    private final String name;
    private final String changeType;
    private final long commitVersion;
    private final long commitTimestampMicros;

    private ExpectedRow(
        long id,
        String name,
        String changeType,
        long commitVersion,
        long commitTimestampMicros) {
      this.id = id;
      this.name = name;
      this.changeType = changeType;
      this.commitVersion = commitVersion;
      this.commitTimestampMicros = commitTimestampMicros;
    }
  }
}
