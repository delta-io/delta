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
package io.delta.spark.internal.v2;

import static org.junit.jupiter.api.Assertions.*;

import io.delta.kernel.*;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.ConcurrentWriteException;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.SingleAction;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests that Kernel's conflict resolution correctly handles OPTIMIZE-style transactions (AddFile +
 * RemoveFile with dataChange=false) when a concurrent blind append occurs.
 *
 * <p>This validates the hypothesis that the Kernel's ConflictChecker — which only checks
 * protocol/metadata/txn/domainMetadata — already handles the OPTIMIZE-vs-blind-append case without
 * any code changes, because a blind append does not modify any of those fields.
 */
public class OptimizeConflictResolutionTest extends DeltaV2TestBase {

  /**
   * Core test: OPTIMIZE transaction with RemoveFile + AddFile succeeds when a concurrent blind
   * append has committed between the OPTIMIZE's snapshot read and its commit attempt.
   *
   * <p>Timeline:
   *
   * <ol>
   *   <li>Table created with 3 small files (versions 0-2)
   *   <li>OPTIMIZE reads snapshot at version 2, builds transaction
   *   <li>Blind append commits version 3 (concurrent writer)
   *   <li>OPTIMIZE commits AddFile + RemoveFile — expects conflict resolution to rebase to v4
   * </ol>
   */
  @Test
  public void testOptimizeSucceedsWithConcurrentBlindAppend(@TempDir File tempDir)
      throws Exception {
    String tablePath = new File(tempDir, "optimize_test").getAbsolutePath();

    // --- Setup: create an unpartitioned table with 3 small files (one per append) ---
    spark.range(10).coalesce(1).write().format("delta").save(tablePath);
    spark.range(10, 20).coalesce(1).write().format("delta").mode("append").save(tablePath);
    spark.range(20, 30).coalesce(1).write().format("delta").mode("append").save(tablePath);

    // --- Step 1: Get Kernel snapshot at version 2 ---
    Table table = Table.forPath(defaultEngine, tablePath);
    Snapshot snapshot = table.getLatestSnapshot(defaultEngine);
    assertEquals(2, snapshot.getVersion());

    // --- Step 2: Collect AddFiles that OPTIMIZE would compact ---
    List<AddFile> filesToCompact = collectAddFiles(snapshot, defaultEngine);
    assertEquals(3, filesToCompact.size());

    // --- Step 3: Build OPTIMIZE transaction from the snapshot ---
    Transaction txn =
        snapshot
            .buildUpdateTableTransaction("optimize-prototype", Operation.MANUAL_UPDATE)
            .build(defaultEngine);
    assertEquals(2, txn.getReadTableVersion());

    // --- Step 4: Write a compacted parquet file containing all the data ---
    File compactedFile = writeCompactedFile(tablePath, tempDir);
    assertTrue(compactedFile.exists());
    assertTrue(compactedFile.length() > 0);

    // --- Step 5: INJECT CONCURRENT BLIND APPEND (creates version 3) ---
    spark.range(100, 110).coalesce(1).write().format("delta").mode("append").save(tablePath);
    Snapshot afterAppend = table.getLatestSnapshot(defaultEngine);
    assertEquals(3, afterAppend.getVersion());

    // --- Step 6: Construct OPTIMIZE actions ---
    List<Row> actionsList = new ArrayList<>();

    // RemoveFile for each original file (dataChange = false, as OPTIMIZE does)
    for (AddFile addFile : filesToCompact) {
      Row removeRow = addFile.toRemoveFileRow(false /* dataChange */, Optional.empty());
      actionsList.add(SingleAction.createRemoveFileSingleAction(removeRow));
    }

    // AddFile for the compacted file (dataChange = false)
    Row compactedAddFileRow = createAddFileRow(tablePath, compactedFile);
    actionsList.add(SingleAction.createAddFileSingleAction(compactedAddFileRow));

    CloseableIterable<Row> actions =
        CloseableIterable.inMemoryIterable(Utils.toCloseableIterator(actionsList.iterator()));

    // --- Step 7: Commit — should succeed after conflict resolution ---
    TransactionCommitResult result = txn.commit(defaultEngine, actions);

    // The blind append took version 3, so OPTIMIZE should commit at version 4
    assertEquals(4, result.getVersion());

    // --- Step 8: Verify table state ---
    // Total rows: 30 (original, now compacted) + 10 (blind append) = 40
    long totalCount = spark.read().format("delta").load(tablePath).count();
    assertEquals(40, totalCount);
  }

  /**
   * Verifies that an OPTIMIZE transaction with only RemoveFile actions (no new AddFile) also
   * succeeds with concurrent blind append. This covers the degenerate case.
   */
  @Test
  public void testRemoveOnlyTransactionSucceedsWithConcurrentAppend(@TempDir File tempDir)
      throws Exception {
    String tablePath = new File(tempDir, "remove_only_test").getAbsolutePath();

    spark.range(10).coalesce(1).write().format("delta").save(tablePath);
    spark.range(10, 20).coalesce(1).write().format("delta").mode("append").save(tablePath);

    Table table = Table.forPath(defaultEngine, tablePath);
    Snapshot snapshot = table.getLatestSnapshot(defaultEngine);
    assertEquals(1, snapshot.getVersion());

    List<AddFile> filesToRemove = collectAddFiles(snapshot, defaultEngine);
    assertEquals(2, filesToRemove.size());

    Transaction txn =
        snapshot
            .buildUpdateTableTransaction("optimize-prototype", Operation.MANUAL_UPDATE)
            .build(defaultEngine);

    // Concurrent blind append
    spark.range(100, 110).coalesce(1).write().format("delta").mode("append").save(tablePath);

    // Only RemoveFile actions (dataChange = false)
    List<Row> actionsList = new ArrayList<>();
    for (AddFile addFile : filesToRemove) {
      Row removeRow = addFile.toRemoveFileRow(false /* dataChange */, Optional.empty());
      actionsList.add(SingleAction.createRemoveFileSingleAction(removeRow));
    }

    CloseableIterable<Row> actions =
        CloseableIterable.inMemoryIterable(Utils.toCloseableIterator(actionsList.iterator()));

    TransactionCommitResult result = txn.commit(defaultEngine, actions);
    assertEquals(3, result.getVersion());
  }

  /**
   * Verifies that OPTIMIZE succeeds even with multiple concurrent blind appends (multiple conflict
   * resolution rounds).
   */
  @Test
  public void testOptimizeSucceedsWithMultipleConcurrentAppends(@TempDir File tempDir)
      throws Exception {
    String tablePath = new File(tempDir, "multi_append_test").getAbsolutePath();

    spark.range(10).coalesce(1).write().format("delta").save(tablePath);
    spark.range(10, 20).coalesce(1).write().format("delta").mode("append").save(tablePath);

    Table table = Table.forPath(defaultEngine, tablePath);
    Snapshot snapshot = table.getLatestSnapshot(defaultEngine);
    assertEquals(1, snapshot.getVersion());

    List<AddFile> filesToCompact = collectAddFiles(snapshot, defaultEngine);

    Transaction txn =
        snapshot
            .buildUpdateTableTransaction("optimize-prototype", Operation.MANUAL_UPDATE)
            .build(defaultEngine);

    File compactedFile = writeCompactedFile(tablePath, tempDir);

    // Three concurrent blind appends (versions 2, 3, 4)
    spark.range(100, 110).coalesce(1).write().format("delta").mode("append").save(tablePath);
    spark.range(110, 120).coalesce(1).write().format("delta").mode("append").save(tablePath);
    spark.range(120, 130).coalesce(1).write().format("delta").mode("append").save(tablePath);

    List<Row> actionsList = new ArrayList<>();
    for (AddFile addFile : filesToCompact) {
      Row removeRow = addFile.toRemoveFileRow(false, Optional.empty());
      actionsList.add(SingleAction.createRemoveFileSingleAction(removeRow));
    }
    actionsList.add(
        SingleAction.createAddFileSingleAction(createAddFileRow(tablePath, compactedFile)));

    CloseableIterable<Row> actions =
        CloseableIterable.inMemoryIterable(Utils.toCloseableIterator(actionsList.iterator()));

    TransactionCommitResult result = txn.commit(defaultEngine, actions);

    // Three appends took versions 2, 3, 4 — OPTIMIZE should commit at version 5
    assertEquals(5, result.getVersion());

    // 20 original (compacted) + 30 appended = 50
    long totalCount = spark.read().format("delta").load(tablePath).count();
    assertEquals(50, totalCount);
  }

  // ==================================================================================
  // Conflict detection tests — verify that the Kernel's ConflictChecker correctly
  // rejects OPTIMIZE when a concurrent writer deletes files that OPTIMIZE read or
  // also deleted.
  // ==================================================================================

  /**
   * Delete-read conflict: a concurrent writer deletes a file that OPTIMIZE read and compacted.
   * OPTIMIZE must fail with ConcurrentWriteException because its compacted output includes data
   * from the deleted file, which would resurrect deleted rows.
   */
  @Test
  public void testDeleteReadConflictThrowsException(@TempDir File tempDir) throws Exception {
    String tablePath = new File(tempDir, "delete_read_test").getAbsolutePath();

    // Create table: file A (ids 0-9), file B (ids 10-19), file C (ids 20-29)
    spark.range(0, 10).coalesce(1).write().format("delta").save(tablePath);
    spark.range(10, 20).coalesce(1).write().format("delta").mode("append").save(tablePath);
    spark.range(20, 30).coalesce(1).write().format("delta").mode("append").save(tablePath);

    Table table = Table.forPath(defaultEngine, tablePath);
    Snapshot snapshot = table.getLatestSnapshot(defaultEngine);
    assertEquals(2, snapshot.getVersion());

    List<AddFile> filesToCompact = collectAddFiles(snapshot, defaultEngine);
    assertEquals(3, filesToCompact.size());

    Transaction optimizeTxn =
        snapshot
            .buildUpdateTableTransaction("optimize", Operation.MANUAL_UPDATE)
            .build(defaultEngine);

    File compactedFile = writeCompactedFile(tablePath, tempDir);

    // Concurrent DELETE: another writer removes file B (ids 10-19)
    AddFile fileB = filesToCompact.get(1);
    Transaction deleteTxn =
        snapshot
            .buildUpdateTableTransaction("concurrent-delete", Operation.MANUAL_UPDATE)
            .build(defaultEngine);
    List<Row> deleteActions = new ArrayList<>();
    deleteActions.add(
        SingleAction.createRemoveFileSingleAction(
            fileB.toRemoveFileRow(true /* dataChange */, Optional.empty())));
    deleteTxn.commit(
        defaultEngine,
        CloseableIterable.inMemoryIterable(Utils.toCloseableIterator(deleteActions.iterator())));

    assertEquals(20, spark.read().format("delta").load(tablePath).count());

    // OPTIMIZE tries to commit: RemoveFile(A,B,C) + AddFile(compacted)
    List<Row> optimizeActions = new ArrayList<>();
    for (AddFile addFile : filesToCompact) {
      optimizeActions.add(
          SingleAction.createRemoveFileSingleAction(
              addFile.toRemoveFileRow(false, Optional.empty())));
    }
    optimizeActions.add(
        SingleAction.createAddFileSingleAction(createAddFileRow(tablePath, compactedFile)));

    CloseableIterable<Row> actions =
        CloseableIterable.inMemoryIterable(Utils.toCloseableIterator(optimizeActions.iterator()));

    // Must throw: file B was deleted by the winning commit but OPTIMIZE also removes it
    assertThrows(ConcurrentWriteException.class, () -> optimizeTxn.commit(defaultEngine, actions));

    // Table state unchanged: 20 rows (file B was already deleted by the concurrent writer)
    assertEquals(20, spark.read().format("delta").load(tablePath).count());
  }

  /**
   * Delete-delete conflict: two concurrent OPTIMIZEs both compact the same files. The second must
   * fail with ConcurrentWriteException because the files it tries to remove were already removed by
   * the first OPTIMIZE.
   */
  @Test
  public void testDeleteDeleteConflictThrowsException(@TempDir File tempDir) throws Exception {
    String tablePath = new File(tempDir, "delete_delete_test").getAbsolutePath();

    // Create table: file A (ids 0-9), file B (ids 10-19)
    spark.range(0, 10).coalesce(1).write().format("delta").save(tablePath);
    spark.range(10, 20).coalesce(1).write().format("delta").mode("append").save(tablePath);

    Table table = Table.forPath(defaultEngine, tablePath);
    Snapshot snapshot = table.getLatestSnapshot(defaultEngine);
    assertEquals(1, snapshot.getVersion());

    List<AddFile> files = collectAddFiles(snapshot, defaultEngine);
    assertEquals(2, files.size());

    Transaction optimize1 =
        snapshot
            .buildUpdateTableTransaction("optimize-1", Operation.MANUAL_UPDATE)
            .build(defaultEngine);
    Transaction optimize2 =
        snapshot
            .buildUpdateTableTransaction("optimize-2", Operation.MANUAL_UPDATE)
            .build(defaultEngine);

    File compactedFile1 = writeCompactedFile(tablePath, tempDir);
    File compactedFile2 = writeCompactedFile(tablePath, tempDir);

    // OPTIMIZE-1 commits first: RemoveFile(A,B) + AddFile(compacted-1)
    List<Row> actions1 = new ArrayList<>();
    for (AddFile f : files) {
      actions1.add(
          SingleAction.createRemoveFileSingleAction(f.toRemoveFileRow(false, Optional.empty())));
    }
    actions1.add(
        SingleAction.createAddFileSingleAction(createAddFileRow(tablePath, compactedFile1)));

    TransactionCommitResult result1 =
        optimize1.commit(
            defaultEngine,
            CloseableIterable.inMemoryIterable(Utils.toCloseableIterator(actions1.iterator())));
    assertEquals(2, result1.getVersion());
    assertEquals(20, spark.read().format("delta").load(tablePath).count());

    // OPTIMIZE-2 tries to commit the same removes — must fail
    List<Row> actions2 = new ArrayList<>();
    for (AddFile f : files) {
      actions2.add(
          SingleAction.createRemoveFileSingleAction(f.toRemoveFileRow(false, Optional.empty())));
    }
    actions2.add(
        SingleAction.createAddFileSingleAction(createAddFileRow(tablePath, compactedFile2)));

    CloseableIterable<Row> actions2Iterable =
        CloseableIterable.inMemoryIterable(Utils.toCloseableIterator(actions2.iterator()));

    assertThrows(
        ConcurrentWriteException.class, () -> optimize2.commit(defaultEngine, actions2Iterable));

    // Table state unchanged: still 20 rows from OPTIMIZE-1
    assertEquals(20, spark.read().format("delta").load(tablePath).count());
  }

  // ---- Helper methods ----

  /** Collects all AddFile entries from a snapshot's scan files. */
  private List<AddFile> collectAddFiles(Snapshot snapshot, Engine engine) throws IOException {
    List<AddFile> addFiles = new ArrayList<>();
    Scan scan = snapshot.getScanBuilder().build();
    try (CloseableIterator<FilteredColumnarBatch> scanFiles = scan.getScanFiles(engine)) {
      while (scanFiles.hasNext()) {
        FilteredColumnarBatch batch = scanFiles.next();
        try (CloseableIterator<Row> rows = batch.getRows()) {
          while (rows.hasNext()) {
            Row scanFileRow = rows.next();
            Row addFileRow = scanFileRow.getStruct(InternalScanFileUtils.ADD_FILE_ORDINAL);
            addFiles.add(new AddFile(addFileRow));
          }
        }
      }
    }
    return addFiles;
  }

  /**
   * Writes a single compacted parquet file containing all data from the delta table. Returns the
   * file in the table's data directory.
   */
  private File writeCompactedFile(String tablePath, File tempDir) throws IOException {
    String compactedTempDir = new File(tempDir, "compacted_" + UUID.randomUUID()).getAbsolutePath();
    spark
        .read()
        .format("delta")
        .load(tablePath)
        .coalesce(1)
        .write()
        .format("parquet")
        .save(compactedTempDir);

    File parquetFile = findParquetFile(new File(compactedTempDir));
    File destFile =
        new File(tablePath, "compacted-" + UUID.randomUUID().toString() + ".snappy.parquet");
    Files.copy(parquetFile.toPath(), destFile.toPath());
    return destFile;
  }

  /** Finds the first .parquet file in a directory (Spark writes part-*.parquet files). */
  private File findParquetFile(File directory) throws IOException {
    try (Stream<Path> paths = Files.walk(directory.toPath())) {
      return paths
          .filter(p -> p.toString().endsWith(".parquet"))
          .map(Path::toFile)
          .findFirst()
          .orElseThrow(() -> new RuntimeException("No parquet file found in " + directory));
    }
  }

  /** Creates an AddFile row for a compacted file. Uses dataChange=false as OPTIMIZE does. */
  private Row createAddFileRow(String tablePath, File parquetFile) {
    String relativePath = parquetFile.getName();
    return AddFile.createAddFileRow(
        null, // physicalSchema — only needed for stats serialization, which we skip
        relativePath,
        VectorUtils.stringStringMapValue(Collections.emptyMap()),
        parquetFile.length(),
        parquetFile.lastModified(),
        false, // dataChange = false for OPTIMIZE
        Optional.empty(), // deletionVector
        Optional.empty(), // tags
        Optional.empty(), // baseRowId
        Optional.empty(), // defaultRowCommitVersion
        Optional.empty() // stats
        );
  }
}
