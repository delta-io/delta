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
package io.delta.spark.internal.v2.write;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.Snapshot;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.spark.internal.v2.DeltaV2TestBase;
import io.delta.spark.internal.v2.InternalRowTestUtils;
import io.delta.spark.internal.v2.snapshot.PathBasedSnapshotManager;
import io.delta.spark.internal.v2.utils.SerializableKernelRowWrapper;
import java.io.File;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Unit tests for {@link DeltaV2BatchWrite}. */
public class DeltaV2BatchWriteTest extends DeltaV2TestBase {

  private static final StructType TABLE_SCHEMA =
      DataTypes.createStructType(
          new StructField[] {
            DataTypes.createStructField("id", DataTypes.IntegerType, true),
            DataTypes.createStructField("name", DataTypes.StringType, true)
          });

  @Test
  public void testCreateBatchWriterFactory_returnsDeltaV2DataWriterFactory(@TempDir File tempDir) {
    DeltaV2BatchWrite write = newWrite(createTable(tempDir, "batch_factory_type"));
    assertTrue(
        write.createBatchWriterFactory(WriteTestUtils.physicalWriteInfo(1))
            instanceof DeltaV2DataWriterFactory);
  }

  @Test
  public void testCommit_appendsData(@TempDir File tempDir) throws Exception {
    String path = createTable(tempDir, "batch_commit");
    DeltaV2BatchWrite write = newWrite(path);
    WriterCommitMessage[] messages = {writeFile(write, 1, "Alice", 2, "Bob")};

    write.commit(messages);

    List<Row> rows = spark.read().format("delta").load(path).orderBy("id").collectAsList();
    assertEquals(2, rows.size());
    assertEquals("Alice", rows.get(0).getString(1));
    assertEquals("Bob", rows.get(1).getString(1));
  }

  @Test
  public void testCommit_multipleTasks_appendsAllData(@TempDir File tempDir) throws Exception {
    String path = createTable(tempDir, "batch_commit_multi");
    DeltaV2BatchWrite write = newWrite(path);
    // Two tasks each write their own file; commit must flatten both tasks' AddFile actions into a
    // single Delta commit rather than dropping all but one.
    DataWriterFactory factory = write.createBatchWriterFactory(WriteTestUtils.physicalWriteInfo(2));
    WriterCommitMessage[] messages = {
      writeFile(factory, 0, 1, "Alice", 2, "Bob"), writeFile(factory, 1, 3, "Carol")
    };

    write.commit(messages);

    List<Row> rows = spark.read().format("delta").load(path).orderBy("id").collectAsList();
    assertEquals(3, rows.size());
    assertEquals("Alice", rows.get(0).getString(1));
    assertEquals("Bob", rows.get(1).getString(1));
    assertEquals("Carol", rows.get(2).getString(1));
  }

  @Test
  public void testAbort_doesNotCommit(@TempDir File tempDir) throws Exception {
    String path = createTable(tempDir, "batch_abort");
    DeltaV2BatchWrite write = newWrite(path);
    // Stage a real file (writeFile runs the executor writer and produces a non-empty commit
    // message) so the test exercises an abort that has data to discard, not a trivial no-op.
    WriterCommitMessage[] messages = {writeFile(write, 1, "Alice", 2, "Bob")};
    long versionsBefore = spark.sql("DESCRIBE HISTORY delta.`" + path + "`").count();

    write.abort(messages);

    assertEquals(0L, spark.read().format("delta").load(path).count(), "abort must not commit data");
    // Distinguishes a correct abort from an erroneous (even empty) commit: a commit would advance
    // the table version, so the history row count must be unchanged.
    assertEquals(
        versionsBefore,
        spark.sql("DESCRIBE HISTORY delta.`" + path + "`").count(),
        "abort must not create a new table version");
  }

  @Test
  public void testAbort_hasNoSideEffectsOnSurroundingWrites(@TempDir File tempDir)
      throws Exception {
    String path = createTable(tempDir, "batch_abort_no_side_effects");

    // Commit before the abort.
    DeltaV2BatchWrite before = newWrite(path);
    before.commit(new WriterCommitMessage[] {writeFile(before, 1, "Alice")});
    // Abort a write that staged real data.
    DeltaV2BatchWrite aborted = newWrite(path);
    aborted.abort(new WriterCommitMessage[] {writeFile(aborted, 2, "Bob")});
    // Commit after the abort.
    DeltaV2BatchWrite after = newWrite(path);
    after.commit(new WriterCommitMessage[] {writeFile(after, 3, "Carol")});

    // Only the surrounding commits survive; the aborted row must not leak.
    List<Row> rows = spark.read().format("delta").load(path).orderBy("id").collectAsList();
    assertEquals(2, rows.size());
    assertEquals("Alice", rows.get(0).getString(1));
    assertEquals("Carol", rows.get(1).getString(1));
  }

  // ---- Partitioned writes ----

  // Table (value INT) partitioned by (part STRING). The write row layout is the full table schema
  // [value, part]; the writer projects [value] into the Parquet body and routes on [part].
  private static final StructType PARTITIONED_FULL_SCHEMA =
      DataTypes.createStructType(
          new StructField[] {
            DataTypes.createStructField("value", DataTypes.IntegerType, true),
            DataTypes.createStructField("part", DataTypes.StringType, true)
          });
  private static final StructType PARTITIONED_DATA_SCHEMA =
      DataTypes.createStructType(
          new StructField[] {DataTypes.createStructField("value", DataTypes.IntegerType, true)});
  private static final StructType PARTITIONED_PART_SCHEMA =
      DataTypes.createStructType(
          new StructField[] {DataTypes.createStructField("part", DataTypes.StringType, true)});

  @Test
  public void testCommit_partitioned_routesRowsAndRoundTrips(@TempDir File tempDir)
      throws Exception {
    String path = createPartitionedTable(tempDir, "batch_part_roundtrip");
    DeltaV2BatchWrite write = newPartitionedWrite(path);
    // Two partitions in one task (rows pre-sorted by part), so the writer rotates at the boundary.
    WriterCommitMessage msg = writePartitionedFile(write, 10, "a", 11, "a", 20, "b");

    // One data file per partition value the task saw: the single-writer rotates once per boundary,
    // so 2 distinct partitions -> 2 AddFile actions (both "a" rows share one file). This is a
    // per-task invariant, not a claim of cluster-wide file-count parity with V1 (which varies with
    // task count / maxRecordsPerFile).
    assertEquals(2, ((DeltaV2WriterCommitMessage) msg).getActionRows().size());

    // Pin the recorded AddFile.partitionValues directly (not only via the read-back below).
    assertEquals(List.of(Map.of("part", "a"), Map.of("part", "b")), partitionValuesOf(msg));

    write.commit(new WriterCommitMessage[] {msg});

    List<Row> rows = spark.read().format("delta").load(path).orderBy("value").collectAsList();
    assertEquals(3, rows.size());
    assertEquals(10, rows.get(0).getInt(rows.get(0).fieldIndex("value")));
    assertEquals("a", rows.get(0).getString(rows.get(0).fieldIndex("part")));
    assertEquals(20, rows.get(2).getInt(rows.get(2).fieldIndex("value")));
    assertEquals("b", rows.get(2).getString(rows.get(2).fieldIndex("part")));

    // Hive-style partition directories exist on disk (Kernel owns the col=val/ encoding).
    assertTrue(new File(path, "part=a").isDirectory(), "expected partition dir part=a");
    assertTrue(new File(path, "part=b").isDirectory(), "expected partition dir part=b");

    // Partition-predicate read returns only that partition, proving AddFile.partitionValues were
    // recorded (pruning couldn't select by partition otherwise).
    List<Row> onlyB = spark.read().format("delta").load(path).filter("part = 'b'").collectAsList();
    assertEquals(1, onlyB.size());
    assertEquals(20, onlyB.get(0).getInt(onlyB.get(0).fieldIndex("value")));
  }

  /**
   * Regression test for the AddFile numRecords stat. Without it the native (Rust) Kernel scan
   * returns zero rows for a partitioned table (silent wrong results); this fails on {@code _ffi} if
   * the stat is dropped.
   */
  @Test
  public void testCommit_partitioned_predicateReadReturnsMatchingRows(@TempDir File tempDir)
      throws Exception {
    String path = createPartitionedTable(tempDir, "batch_part_pred_read");
    DeltaV2BatchWrite write = newPartitionedWrite(path);
    write.commit(new WriterCommitMessage[] {writePartitionedFile(write, 1, "a", 2, "a", 3, "b")});

    // Full scan sees all rows; partition-pruned reads return exactly the matching rows, not zero.
    assertEquals(3, spark.read().format("delta").load(path).count());
    assertEquals(2, spark.read().format("delta").load(path).filter("part = 'a'").count());
    assertEquals(1, spark.read().format("delta").load(path).filter("part = 'b'").count());
  }

  /**
   * DSv2 (Kernel) and DSv1 both record TIMESTAMP partition values as UTC ISO-8601. Timezone is
   * pinned so the strings are deterministic.
   */
  @Test
  public void testCommit_partitioned_timestampPartitionValueMatchesV1(@TempDir File tempDir)
      throws Exception {
    withSQLConf(
        "spark.sql.session.timeZone",
        "UTC",
        () -> {
          String v1Path = new File(tempDir, "v1").getAbsolutePath();
          String v2Path = new File(tempDir, "v2").getAbsolutePath();
          StructType schema =
              new StructType(
                  new StructField[] {
                    DataTypes.createStructField("value", DataTypes.IntegerType, true),
                    DataTypes.createStructField("part", DataTypes.TimestampType, true)
                  });
          Timestamp ts = Timestamp.valueOf("2025-06-15 10:30:00");

          // Same row written by V1 and DSv2.
          spark
              .createDataFrame(List.of(RowFactory.create(1, ts)), schema)
              .write()
              .format("delta")
              .partitionBy("part")
              .save(v1Path);
          spark.sql(
              String.format(
                  "CREATE TABLE t_ts_v2_%d (value INT, part TIMESTAMP) USING delta "
                      + "PARTITIONED BY (part) LOCATION '%s'",
                  System.nanoTime(), v2Path));
          DeltaV2BatchWrite write = newTimestampPartWrite(v2Path);
          write.commit(
              new WriterCommitMessage[] {writeFile(write, 1, DateTimeUtils.fromJavaTimestamp(ts))});

          assertEquals("2025-06-15T10:30:00.000000Z", loggedPartitionValue(v1Path, "part"));
          assertEquals("2025-06-15T10:30:00.000000Z", loggedPartitionValue(v2Path, "part"));
        });
  }

  @Test
  public void testCommit_partitioned_nullPartitionValue(@TempDir File tempDir) throws Exception {
    String path = createPartitionedTable(tempDir, "batch_part_null");
    DeltaV2BatchWrite write = newPartitionedWrite(path);
    // A null partition value lands in Hive's __HIVE_DEFAULT_PARTITION__ directory and reads back
    // as null.
    WriterCommitMessage msg = writePartitionedFile(write, 1, null, 2, "x");

    write.commit(new WriterCommitMessage[] {msg});

    assertTrue(
        new File(path, "part=__HIVE_DEFAULT_PARTITION__").isDirectory(),
        "expected default-partition dir for the null value");
    List<Row> nullRows =
        spark.read().format("delta").load(path).filter("part is null").collectAsList();
    assertEquals(1, nullRows.size());
    assertEquals(1, nullRows.get(0).getInt(nullRows.get(0).fieldIndex("value")));
  }

  @Test
  public void testCommit_partitioned_emptyStringPartitionValueParity(@TempDir File tempDir)
      throws Exception {
    String v1Path = new File(tempDir, "v1").getAbsolutePath();
    String v2Path = new File(tempDir, "v2").getAbsolutePath();
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("value", DataTypes.IntegerType, true),
              DataTypes.createStructField("part", DataTypes.StringType, true)
            });

    // DSv1 write of an empty-string and a non-empty partition value.
    spark
        .createDataFrame(List.of(RowFactory.create(1, ""), RowFactory.create(2, "x")), schema)
        .write()
        .format("delta")
        .partitionBy("part")
        .save(v1Path);

    // DSv2 write of the same rows.
    spark.sql(
        String.format(
            "CREATE TABLE t_empty_v2_%d (value INT, part STRING) USING delta "
                + "PARTITIONED BY (part) LOCATION '%s'",
            System.nanoTime(), v2Path));
    DeltaV2BatchWrite write = newPartitionedWrite(v2Path);
    write.commit(new WriterCommitMessage[] {writePartitionedFile(write, 1, "", 2, "x")});

    // Same partition directories on disk and same rows on read-back under both engines.
    assertEquals(partitionDirs(v1Path), partitionDirs(v2Path));
    List<Row> v1Rows = spark.read().format("delta").load(v1Path).orderBy("value").collectAsList();
    List<Row> v2Rows = spark.read().format("delta").load(v2Path).orderBy("value").collectAsList();
    assertEquals(v1Rows.toString(), v2Rows.toString());
  }

  @Test
  public void testCommit_partitioned_valueNeedingPathEscaping(@TempDir File tempDir)
      throws Exception {
    String path = createPartitionedTable(tempDir, "batch_part_escape");
    DeltaV2BatchWrite write = newPartitionedWrite(path);
    // A space in the partition value must be Hive-escaped in the path but preserved on read back
    // (mirrors the base suite's SPARK-21167 case).
    WriterCommitMessage msg = writePartitionedFile(write, 7, "hello world");

    write.commit(new WriterCommitMessage[] {msg});

    List<Row> rows =
        spark.read().format("delta").load(path).filter("part = 'hello world'").collectAsList();
    assertEquals(1, rows.size());
    assertEquals(7, rows.get(0).getInt(rows.get(0).fieldIndex("value")));
  }

  @Test
  public void testCommit_partitioned_interleavedInputProducesFilePerRun(@TempDir File tempDir)
      throws Exception {
    String path = createPartitionedTable(tempDir, "batch_part_interleaved");
    DeltaV2BatchWrite write = newPartitionedWrite(path);
    // The single-writer rotates on each partition-key CHANGE, not per distinct value: it has no
    // memory of partitions it already closed. Unsorted input a,b,a therefore yields 3 files (a run
    // of "a", a run of "b", another run of "a"), documenting the requiredOrdering dependency -- a
    // sorted a,a,b would produce 2. Data is still correct either way.
    WriterCommitMessage msg = writePartitionedFile(write, 1, "a", 2, "b", 3, "a");
    assertEquals(3, ((DeltaV2WriterCommitMessage) msg).getActionRows().size());

    write.commit(new WriterCommitMessage[] {msg});

    List<Row> rows = spark.read().format("delta").load(path).orderBy("value").collectAsList();
    assertEquals(3, rows.size());
    List<Row> partA =
        spark
            .read()
            .format("delta")
            .load(path)
            .filter("part = 'a'")
            .orderBy("value")
            .collectAsList();
    assertEquals(2, partA.size());
    assertEquals(1, partA.get(0).getInt(partA.get(0).fieldIndex("value")));
    assertEquals(3, partA.get(1).getInt(partA.get(1).fieldIndex("value")));
  }

  @Test
  public void testAbort_partitioned_deletesAllPartitionFiles(@TempDir File tempDir)
      throws Exception {
    String path = createPartitionedTable(tempDir, "batch_part_abort");
    DeltaV2BatchWrite write = newPartitionedWrite(path);
    // Stage a write spanning two partitions (two files), then abort: both files must be removed,
    // not just the last one opened.
    DataWriter<InternalRow> writer =
        write.createBatchWriterFactory(WriteTestUtils.physicalWriteInfo(1)).createWriter(0, 0L);
    writer.write(InternalRowTestUtils.row(10, "a"));
    writer.write(InternalRowTestUtils.row(20, "b"));
    writer.abort();

    for (String dir : new String[] {"part=a", "part=b"}) {
      File[] parquet = new File(path, dir).listFiles((d, n) -> n.endsWith(".parquet"));
      assertTrue(parquet == null || parquet.length == 0, "aborted files must be deleted in " + dir);
    }
    assertEquals(0L, spark.read().format("delta").load(path).count(), "abort must not commit data");
  }

  @Test
  public void testCommit_partitioned_multipleColumns(@TempDir File tempDir) throws Exception {
    String path = tempDir.getAbsolutePath();
    spark.sql(
        String.format(
            "CREATE TABLE t_multicol_%d (value INT, p1 STRING, p2 INT) USING delta "
                + "PARTITIONED BY (p1, p2) LOCATION '%s'",
            System.nanoTime(), path));
    // Full row layout [value, p1, p2]; data schema [value], partition schema [p1, p2].
    StructType full =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("value", DataTypes.IntegerType, true),
              DataTypes.createStructField("p1", DataTypes.StringType, true),
              DataTypes.createStructField("p2", DataTypes.IntegerType, true)
            });
    StructType data =
        DataTypes.createStructType(
            new StructField[] {DataTypes.createStructField("value", DataTypes.IntegerType, true)});
    StructType part =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("p1", DataTypes.StringType, true),
              DataTypes.createStructField("p2", DataTypes.IntegerType, true)
            });
    PathBasedSnapshotManager mgr =
        new PathBasedSnapshotManager(path, spark.sessionState().newHadoopConf());
    DeltaV2BatchWrite write =
        (DeltaV2BatchWrite)
            new DeltaV2Write(
                    defaultEngine,
                    spark.sessionState().newHadoopConf(),
                    path,
                    mgr.loadLatestSnapshot(),
                    mgr,
                    data,
                    part,
                    WriteTestUtils.logicalWriteInfo(full, CaseInsensitiveStringMap.empty()))
                .toBatch();

    DataWriter<InternalRow> writer =
        write.createBatchWriterFactory(WriteTestUtils.physicalWriteInfo(1)).createWriter(0, 0L);
    writer.write(InternalRowTestUtils.row(1, "x", 7));
    WriterCommitMessage msg = writer.commit();
    write.commit(new WriterCommitMessage[] {msg});

    // Nested Hive-style directory p1=x/p2=7/.
    assertTrue(
        new File(path, "p1=x/p2=7").isDirectory(), "expected nested partition dir p1=x/p2=7");
    List<Row> rows = spark.read().format("delta").load(path).collectAsList();
    assertEquals(1, rows.size());
    assertEquals("x", rows.get(0).getString(rows.get(0).fieldIndex("p1")));
    assertEquals(7, rows.get(0).getInt(rows.get(0).fieldIndex("p2")));
  }

  @Test
  public void testCommit_partitioned_multipleColumns_boundaryScenarios(@TempDir File tempDir)
      throws Exception {
    String path = tempDir.getAbsolutePath();
    spark.sql(
        String.format(
            "CREATE TABLE t_multicol_bounds_%d (value INT, p1 STRING, p2 INT) USING delta "
                + "PARTITIONED BY (p1, p2) LOCATION '%s'",
            System.nanoTime(), path));
    DeltaV2BatchWrite write = newMultiColWrite(path);

    // Rows pre-sorted by (p1, p2): inner-only change (x,1)->(x,2), outer change ->(y,5),
    // one null column (y,null), all-null (null,null). Each key change is its own AddFile.
    DataWriter<InternalRow> writer =
        write.createBatchWriterFactory(WriteTestUtils.physicalWriteInfo(1)).createWriter(0, 0L);
    writer.write(InternalRowTestUtils.row(1, "x", 1));
    writer.write(InternalRowTestUtils.row(2, "x", 2));
    writer.write(InternalRowTestUtils.row(3, "y", 5));
    writer.write(InternalRowTestUtils.row(4, "y", null));
    writer.write(InternalRowTestUtils.row(5, null, null));
    WriterCommitMessage msg = writer.commit();

    // A null partition value is recorded as a null map value (not the on-disk sentinel).
    assertEquals(
        List.of(
            partMap("x", "1"),
            partMap("x", "2"),
            partMap("y", "5"),
            partMap("y", null),
            partMap(null, null)),
        partitionValuesOf(msg));

    write.commit(new WriterCommitMessage[] {msg});

    List<Row> rows = spark.read().format("delta").load(path).orderBy("value").collectAsList();
    assertEquals(5, rows.size());
    assertEquals("y", rows.get(3).getString(rows.get(3).fieldIndex("p1")));
    assertTrue(rows.get(3).isNullAt(rows.get(3).fieldIndex("p2")));
    assertTrue(rows.get(4).isNullAt(rows.get(4).fieldIndex("p1")));
    assertTrue(rows.get(4).isNullAt(rows.get(4).fieldIndex("p2")));
    assertEquals(2, spark.read().format("delta").load(path).filter("p1 = 'x'").count());
    assertEquals(
        1, spark.read().format("delta").load(path).filter("p1 = 'y' and p2 is null").count());
    assertEquals(1, spark.read().format("delta").load(path).filter("p1 is null").count());
  }

  /** Builds a batch write for the (value INT, p1 STRING, p2 INT) PARTITIONED BY (p1, p2) table. */
  private DeltaV2BatchWrite newMultiColWrite(String path) {
    StructType full =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("value", DataTypes.IntegerType, true),
              DataTypes.createStructField("p1", DataTypes.StringType, true),
              DataTypes.createStructField("p2", DataTypes.IntegerType, true)
            });
    StructType data =
        DataTypes.createStructType(
            new StructField[] {DataTypes.createStructField("value", DataTypes.IntegerType, true)});
    StructType part =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("p1", DataTypes.StringType, true),
              DataTypes.createStructField("p2", DataTypes.IntegerType, true)
            });
    PathBasedSnapshotManager mgr =
        new PathBasedSnapshotManager(path, spark.sessionState().newHadoopConf());
    return (DeltaV2BatchWrite)
        new DeltaV2Write(
                defaultEngine,
                spark.sessionState().newHadoopConf(),
                path,
                mgr.loadLatestSnapshot(),
                mgr,
                data,
                part,
                WriteTestUtils.logicalWriteInfo(full, CaseInsensitiveStringMap.empty()))
            .toBatch();
  }

  /** Reads the single AddFile's recorded value for partition column {@code col} from the log. */
  private String loggedPartitionValue(String tablePath, String col) {
    List<Row> adds =
        spark
            .read()
            .json(tablePath + "/_delta_log/*.json")
            .where("add is not null")
            .select("add.partitionValues." + col)
            .collectAsList();
    assertEquals(1, adds.size(), "expected exactly one AddFile in the log");
    return adds.get(0).getString(0);
  }

  /** Builds a batch write for the (value INT, part TIMESTAMP) PARTITIONED BY (part) table. */
  private DeltaV2BatchWrite newTimestampPartWrite(String path) {
    StructType full =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("value", DataTypes.IntegerType, true),
              DataTypes.createStructField("part", DataTypes.TimestampType, true)
            });
    StructType data =
        DataTypes.createStructType(
            new StructField[] {DataTypes.createStructField("value", DataTypes.IntegerType, true)});
    StructType part =
        DataTypes.createStructType(
            new StructField[] {DataTypes.createStructField("part", DataTypes.TimestampType, true)});
    PathBasedSnapshotManager mgr =
        new PathBasedSnapshotManager(path, spark.sessionState().newHadoopConf());
    return (DeltaV2BatchWrite)
        new DeltaV2Write(
                defaultEngine,
                spark.sessionState().newHadoopConf(),
                path,
                mgr.loadLatestSnapshot(),
                mgr,
                data,
                part,
                WriteTestUtils.logicalWriteInfo(full, CaseInsensitiveStringMap.empty()))
            .toBatch();
  }

  private String createPartitionedTable(File tempDir, String tableName) {
    String path = tempDir.getAbsolutePath();
    spark.sql(
        String.format(
            "CREATE TABLE %s (value INT, part STRING) USING delta PARTITIONED BY (part) "
                + "LOCATION '%s'",
            tableName, path));
    return path;
  }

  private DeltaV2BatchWrite newPartitionedWrite(String path) {
    PathBasedSnapshotManager snapshotManager =
        new PathBasedSnapshotManager(path, spark.sessionState().newHadoopConf());
    Snapshot snapshot = snapshotManager.loadLatestSnapshot();
    LogicalWriteInfo info =
        WriteTestUtils.logicalWriteInfo(PARTITIONED_FULL_SCHEMA, CaseInsensitiveStringMap.empty());
    DeltaV2Write write =
        new DeltaV2Write(
            defaultEngine,
            spark.sessionState().newHadoopConf(),
            path,
            snapshot,
            snapshotManager,
            PARTITIONED_DATA_SCHEMA,
            PARTITIONED_PART_SCHEMA,
            info);
    return (DeltaV2BatchWrite) write.toBatch();
  }

  /**
   * Writes full rows [value, part] at partition 0 / task 0. {@code valuePartPairs} is a flat list
   * of (int value, String part), supplied already sorted by {@code part} (as Spark would deliver).
   */
  private WriterCommitMessage writePartitionedFile(
      DeltaV2BatchWrite write, Object... valuePartPairs) throws Exception {
    DataWriter<InternalRow> writer =
        write.createBatchWriterFactory(WriteTestUtils.physicalWriteInfo(1)).createWriter(0, 0L);
    for (int i = 0; i < valuePartPairs.length; i += 2) {
      writer.write(InternalRowTestUtils.row(valuePartPairs[i], valuePartPairs[i + 1]));
    }
    return writer.commit();
  }

  /** Builds an expected (p1, p2) partition-values map, allowing null values (unlike Map.of). */
  private static Map<String, String> partMap(String p1, String p2) {
    Map<String, String> map = new LinkedHashMap<>();
    map.put("p1", p1);
    map.put("p2", p2);
    return map;
  }

  /** Sorted list of Hive-style partition directory names (e.g. {@code part=x}) under a table. */
  private static List<String> partitionDirs(String tablePath) {
    File[] dirs = new File(tablePath).listFiles((d, n) -> n.contains("="));
    List<String> names = new ArrayList<>();
    if (dirs != null) {
      for (File dir : dirs) {
        names.add(dir.getName());
      }
    }
    java.util.Collections.sort(names);
    return names;
  }

  /** Extracts each AddFile's partitionValues from a commit message's action rows, in order. */
  private static List<Map<String, String>> partitionValuesOf(WriterCommitMessage msg) {
    List<Map<String, String>> result = new ArrayList<>();
    for (SerializableKernelRowWrapper wrapper :
        ((DeltaV2WriterCommitMessage) msg).getActionRows()) {
      io.delta.kernel.data.Row singleAction = wrapper.getRow();
      int addOrdinal = singleAction.getSchema().indexOf("add");
      AddFile addFile = new AddFile(singleAction.getStruct(addOrdinal));
      MapValue values = addFile.getPartitionValues();
      Map<String, String> map = new LinkedHashMap<>();
      for (int i = 0; i < values.getSize(); i++) {
        // A null partition value is stored as an actual null in the map (the
        // __HIVE_DEFAULT_PARTITION__ sentinel is only the on-disk directory name).
        String value = values.getValues().isNullAt(i) ? null : values.getValues().getString(i);
        map.put(values.getKeys().getString(i), value);
      }
      result.add(map);
    }
    return result;
  }

  /** Runs the executor-side writer at partition 0 / task 0 and returns its commit message. */
  private WriterCommitMessage writeFile(DeltaV2BatchWrite write, Object... idNamePairs)
      throws Exception {
    return writeFile(
        write.createBatchWriterFactory(WriteTestUtils.physicalWriteInfo(1)), 0, idNamePairs);
  }

  /** Runs the executor-side writer for the given partition and returns its commit message. */
  private WriterCommitMessage writeFile(
      DataWriterFactory factory, int partitionId, Object... idNamePairs) throws Exception {
    DataWriter<InternalRow> writer = factory.createWriter(partitionId, 0L);
    for (int i = 0; i < idNamePairs.length; i += 2) {
      writer.write(InternalRowTestUtils.row(idNamePairs[i], idNamePairs[i + 1]));
    }
    return writer.commit();
  }

  private String createTable(File tempDir, String tableName) {
    String path = tempDir.getAbsolutePath();
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, name STRING) USING delta LOCATION '%s'", tableName, path));
    return path;
  }

  private DeltaV2BatchWrite newWrite(String path) {
    Snapshot snapshot =
        new PathBasedSnapshotManager(path, spark.sessionState().newHadoopConf())
            .loadLatestSnapshot();
    LogicalWriteInfo info =
        WriteTestUtils.logicalWriteInfo(TABLE_SCHEMA, CaseInsensitiveStringMap.empty());
    return new DeltaV2BatchWrite(
        defaultEngine,
        spark.sessionState().newHadoopConf(),
        path,
        snapshot,
        TABLE_SCHEMA,
        new StructType(),
        info);
  }
}
