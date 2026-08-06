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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;

/**
 * Java UC integration test for Delta streaming with Deletion Vectors.
 *
 * <p>Ports every scenario from the Scala {@code DeltaV2SourceDeletionVectorsSuite} wrapper's {@code
 * shouldPassTests} list into real Unity Catalog tables under the default {@code
 * V2_ENABLE_MODE=AUTO} (not set here). MANAGED tables route to DSv2/Kernel; EXTERNAL tables use the
 * V1 path. This validates AUTO routing and surfaces DSv2 regressions.
 *
 * <p>{@code @TestAllTableTypes} runs each scenario against both EXTERNAL and MANAGED tables. {@code
 * delta.enableDeletionVectors=true} is set on every table so that partial DELETEs produce Deletion
 * Vectors rather than file rewrites, matching the Scala suite's {@code PersistentDVEnabled} mix-in.
 */
public class UCDeltaSourceDeletionVectorsStreamingTest extends UCDeltaTableIntegrationBaseTest {

  // Table properties applied to every test table.
  private static final String DV_PROPS =
      "'delta.enableChangeDataFeed'='true', 'delta.enableDeletionVectors'='true'";

  private int checkpointCounter = 0;

  /** Returns a fresh local temp-dir path for use as a streaming checkpoint location. */
  private String checkpoint() throws Exception {
    Path dir = Files.createTempDirectory("dv_ck_" + (checkpointCounter++));
    return dir.toUri().toString();
  }

  // ---------------------------------------------------------------------------
  // 1. "allow to delete files before starting a streaming query"
  //    Source: DeltaSourceDeletionVectorTests.test("allow to delete files before starting …")
  //
  // Setup: insert rows 0-4 → DELETE all → insert rows 5-9. Then start the stream: only rows 5-9
  // should be visible (deleted rows are gone; the stream's initial snapshot reflects the latest
  // table state).
  //
  // NOTE: The Scala test forces a DeltaLog checkpoint via deltaLog.checkpoint(). That is not
  // reachable via public SQL for Catalog-Managed tables (OPTIMIZE and other path-based ops are
  // blocked by the catalog), so we do not force a Delta checkpoint here. The behavioral assertion
  // (only rows 5-9 are streamed) is identical with or without a checkpoint, so the scenario's
  // intent is preserved.
  // ---------------------------------------------------------------------------
  @TestAllTableTypes
  public void testDeleteFilesBeforeStreamingQueryWithCheckpoint(TableType tableType)
      throws Exception {
    withNewTable(
        "dv_delete_before_stream_ck",
        "value INT",
        null,
        tableType,
        DV_PROPS,
        tableName -> {
          // Insert rows 0-4
          for (int i = 0; i < 5; i++) {
            sql("INSERT INTO %s VALUES (%d)", tableName, i);
          }
          // DELETE all rows (whole-table delete — no DVs, just RemoveFiles)
          sql("DELETE FROM %s", tableName);
          // Insert rows 5-9 in separate commits
          for (int i = 5; i < 10; i++) {
            sql("INSERT INTO %s VALUES (%d)", tableName, i);
          }

          List<Integer> result = new ArrayList<>();
          spark()
              .readStream()
              .format("delta")
              .table(tableName)
              .writeStream()
              .trigger(Trigger.AvailableNow())
              .option("checkpointLocation", checkpoint())
              .foreachBatch(
                  (VoidFunction2<Dataset<Row>, Long>) (df, id) -> result.addAll(ints(df, "value")))
              .start()
              .awaitTermination();

          assertThat(result).containsExactlyInAnyOrder(5, 6, 7, 8, 9);
        });
  }

  // ---------------------------------------------------------------------------
  // 2. "allow to delete files before staring a streaming query without checkpoint"
  //    Source: DeltaSourceDeletionVectorTests.test("allow to delete files before staring …
  //            without checkpoint")
  //
  // Same as above but NO Delta table checkpoint: insert 0-4 → DELETE all → insert 5-6.
  // The stream should only see rows 5-6.
  // ---------------------------------------------------------------------------
  @TestAllTableTypes
  public void testDeleteFilesBeforeStreamingQueryWithoutCheckpoint(TableType tableType)
      throws Exception {
    withNewTable(
        "dv_delete_before_stream_no_ck",
        "value INT",
        null,
        tableType,
        DV_PROPS,
        tableName -> {
          for (int i = 0; i < 5; i++) {
            sql("INSERT INTO %s VALUES (%d)", tableName, i);
          }
          sql("DELETE FROM %s", tableName);
          // Only two commits after the DELETE — no OPTIMIZE so no Delta checkpoint
          sql("INSERT INTO %s VALUES (5)", tableName);
          sql("INSERT INTO %s VALUES (6)", tableName);

          List<Integer> result = new ArrayList<>();
          spark()
              .readStream()
              .format("delta")
              .table(tableName)
              .writeStream()
              .trigger(Trigger.AvailableNow())
              .option("checkpointLocation", checkpoint())
              .foreachBatch(
                  (VoidFunction2<Dataset<Row>, Long>) (df, id) -> result.addAll(ints(df, "value")))
              .start()
              .awaitTermination();

          assertThat(result).containsExactlyInAnyOrder(5, 6);
        });
  }

  // ---------------------------------------------------------------------------
  // 3. "multiple deletion vectors per file with initial snapshot"
  //    Source: DeltaSourceDeletionVectorTests.test("multiple deletion vectors per file with
  //            initial snapshot")
  //
  // All rows in one file; delete rows 0, 1, 2 in separate commits (DVs accumulate).
  // Stream started AFTER the deletes → only rows 3-9 visible from initial snapshot.
  // ---------------------------------------------------------------------------
  @TestAllTableTypes
  public void testMultipleDeletionVectorsPerFileWithInitialSnapshot(TableType tableType)
      throws Exception {
    withNewTable(
        "dv_multi_dv_initial_snapshot",
        "value INT",
        null,
        tableType,
        DV_PROPS,
        tableName -> {
          // Insert all 10 rows into a single file (COALESCE(1) hint) so the DELETEs below are
          // partial deletes that accumulate Deletion Vectors on one file.
          insertSingleFile0to9(tableName);

          // Partial DELETEs → each creates/updates a Deletion Vector on the same file
          sql("DELETE FROM %s WHERE value = 0", tableName);
          sql("DELETE FROM %s WHERE value = 1", tableName);
          sql("DELETE FROM %s WHERE value = 2", tableName);

          // Start the stream after all deletes — initial snapshot should skip deleted rows
          List<Integer> result = new ArrayList<>();
          spark()
              .readStream()
              .format("delta")
              .table(tableName)
              .writeStream()
              .trigger(Trigger.AvailableNow())
              .option("checkpointLocation", checkpoint())
              .foreachBatch(
                  (VoidFunction2<Dataset<Row>, Long>) (df, id) -> result.addAll(ints(df, "value")))
              .start()
              .awaitTermination();

          assertThat(result).containsExactlyInAnyOrder(3, 4, 5, 6, 7, 8, 9);
        });
  }

  // ---------------------------------------------------------------------------
  // 4. "deleting files fails query if ignoreDeletes = false"
  //    Source: DeltaSourceDeletionVectorTests.testQuietly("deleting files fails …")
  //
  // Stream is running; whole-table DELETE → stream must fail with "ignoreDeletes" hint.
  // ---------------------------------------------------------------------------
  @TestAllTableTypes
  public void testDeletingFilesFailsQueryIfIgnoreDeletesFalse(TableType tableType)
      throws Exception {
    withNewTable(
        "dv_delete_fails_no_ignore",
        "value INT",
        null,
        tableType,
        DV_PROPS,
        tableName -> {
          // Insert 0,2,4,6,8 (even numbers)
          for (int i = 0; i < 10; i += 2) {
            sql("INSERT INTO %s VALUES (%d)", tableName, i);
          }

          StreamingQuery query =
              spark()
                  .readStream()
                  .format("delta")
                  .table(tableName)
                  .writeStream()
                  .option("checkpointLocation", checkpoint())
                  .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (df, id) -> {})
                  .start();
          try {
            query.processAllAvailable();
            // Whole-table DELETE — produces RemoveFiles, not DVs
            sql("DELETE FROM %s", tableName);
            assertStreamingThrowsContaining(query::processAllAvailable, "ignoreDeletes");
          } finally {
            query.stop();
          }
        });
  }

  // ---------------------------------------------------------------------------
  // 5. "deleting files when ignoreChanges = true doesn't fail the query"
  //    Source: DeltaSourceDeletionVectorTests.testQuietly("deleting files when ignoreChanges …")
  //
  // Whole-table DELETE with ignoreChanges=true must NOT fail; subsequent INSERT is visible.
  // ---------------------------------------------------------------------------
  @TestAllTableTypes
  public void testDeletingFilesWithIgnoreChangesDoesNotFailQuery(TableType tableType)
      throws Exception {
    withNewTable(
        "dv_delete_ignore_changes",
        "value INT",
        null,
        tableType,
        DV_PROPS,
        tableName -> {
          for (int i = 0; i < 10; i += 2) {
            sql("INSERT INTO %s VALUES (%d)", tableName, i);
          }

          List<Integer> result = new ArrayList<>();
          StreamingQuery query =
              spark()
                  .readStream()
                  .format("delta")
                  .option("ignoreChanges", "true")
                  .table(tableName)
                  .writeStream()
                  .option("checkpointLocation", checkpoint())
                  .foreachBatch(
                      (VoidFunction2<Dataset<Row>, Long>)
                          (df, id) -> result.addAll(ints(df, "value")))
                  .start();
          try {
            query.processAllAvailable();
            assertThat(result).containsExactlyInAnyOrder(0, 2, 4, 6, 8);

            // Whole-table delete should be silently ignored by ignoreChanges
            sql("DELETE FROM %s", tableName);
            sql("INSERT INTO %s VALUES (10)", tableName);
            query.processAllAvailable();
            // 10 should appear; the DELETE commit itself is skipped
            assertThat(result).contains(10);
          } finally {
            query.stop();
          }
        });
  }

  // ---------------------------------------------------------------------------
  // 6. "allow to delete files after staring a streaming query when ignoreDeletes is true"
  //    Source: DeltaSourceDeletionVectorTests - Seq("ignoreFileDeletion", IGNORE_DELETES_OPTION)
  //            → test for ignoreDeletes
  // ---------------------------------------------------------------------------
  @TestAllTableTypes
  public void testAllowDeleteFilesAfterStreamingStartWithIgnoreDeletes(TableType tableType)
      throws Exception {
    withNewTable(
        "dv_delete_after_stream_ignore_deletes",
        "value INT",
        null,
        tableType,
        DV_PROPS,
        tableName -> {
          for (int i = 0; i < 10; i += 2) {
            sql("INSERT INTO %s VALUES (%d)", tableName, i);
          }

          List<Integer> result = new ArrayList<>();
          StreamingQuery query =
              spark()
                  .readStream()
                  .format("delta")
                  .option("ignoreDeletes", "true")
                  .table(tableName)
                  .writeStream()
                  .option("checkpointLocation", checkpoint())
                  .foreachBatch(
                      (VoidFunction2<Dataset<Row>, Long>)
                          (df, id) -> result.addAll(ints(df, "value")))
                  .start();
          try {
            query.processAllAvailable();
            assertThat(result).containsExactlyInAnyOrder(0, 2, 4, 6, 8);

            // Whole-table delete with ignoreDeletes=true must not fail
            sql("DELETE FROM %s", tableName);
            sql("INSERT INTO %s VALUES (10)", tableName);
            query.processAllAvailable();
            assertThat(result).contains(10);
          } finally {
            query.stop();
          }
        });
  }

  // ---------------------------------------------------------------------------
  // 7. "allow to delete files after staring a streaming query when ignoreFileDeletion is true"
  //    Source: same Seq loop, for ignoreFileDeletion
  // ---------------------------------------------------------------------------
  @TestAllTableTypes
  public void testAllowDeleteFilesAfterStreamingStartWithIgnoreFileDeletion(TableType tableType)
      throws Exception {
    withNewTable(
        "dv_delete_after_stream_ignore_file_del",
        "value INT",
        null,
        tableType,
        DV_PROPS,
        tableName -> {
          for (int i = 0; i < 10; i += 2) {
            sql("INSERT INTO %s VALUES (%d)", tableName, i);
          }

          List<Integer> result = new ArrayList<>();
          StreamingQuery query =
              spark()
                  .readStream()
                  .format("delta")
                  .option("ignoreFileDeletion", "true")
                  .table(tableName)
                  .writeStream()
                  .option("checkpointLocation", checkpoint())
                  .foreachBatch(
                      (VoidFunction2<Dataset<Row>, Long>)
                          (df, id) -> result.addAll(ints(df, "value")))
                  .start();
          try {
            query.processAllAvailable();
            assertThat(result).containsExactlyInAnyOrder(0, 2, 4, 6, 8);

            sql("DELETE FROM %s", tableName);
            sql("INSERT INTO %s VALUES (10)", tableName);
            query.processAllAvailable();
            assertThat(result).contains(10);
          } finally {
            query.stop();
          }
        });
  }

  // ---------------------------------------------------------------------------
  // 8. "updating the source table causes failure when ignoreChanges = false - using DELETE"
  //    Source: DeltaSourceDeletionVectorTests.testQuietly("updating the source table …")
  //
  // Partial DELETE (produces a DV) without any ignore option → stream must fail
  // with "skipChangeCommits" hint.
  // ---------------------------------------------------------------------------
  @TestAllTableTypes
  public void testUpdatingSourceTableCausesFailureWhenIgnoreChangesFalse(TableType tableType)
      throws Exception {
    withNewTable(
        "dv_update_fails_no_ignore",
        "value INT",
        null,
        tableType,
        DV_PROPS,
        tableName -> {
          // Rows in groups of 2 → {0,1},{2,3},{4,5},{6,7},{8,9}. DELETE value=3 is a PARTIAL
          // delete of the {2,3} file → produces a DV (a "data update").
          insertGroupsOfTwo(tableName);

          StreamingQuery query =
              spark()
                  .readStream()
                  .format("delta")
                  .table(tableName)
                  .writeStream()
                  .option("checkpointLocation", checkpoint())
                  .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (df, id) -> {})
                  .start();
          try {
            query.processAllAvailable();

            // Partial DELETE → DV → "data update" error mentioning skipChangeCommits
            sql("DELETE FROM %s WHERE value = 3", tableName);
            assertStreamingThrowsContaining(
                query::processAllAvailable, "data update", "skipChangeCommits");
          } finally {
            query.stop();
          }
        });
  }

  // ---------------------------------------------------------------------------
  // 9. "allow to update the source table when ignoreChanges = true - using DELETE"
  //    Source: DeltaSourceDeletionVectorTests.testQuietly("allow to update … ignoreChanges …")
  //
  // Partial DELETE with ignoreChanges=true must not fail;
  // subsequent INSERT rows are delivered.
  // ---------------------------------------------------------------------------
  @TestAllTableTypes
  public void testAllowUpdateSourceTableWithIgnoreChanges(TableType tableType) throws Exception {
    withNewTable(
        "dv_update_ignore_changes",
        "value INT",
        null,
        tableType,
        DV_PROPS,
        tableName -> {
          // groups of 2: {0,1},{2,3},{4,5},{6,7},{8,9}
          insertGroupsOfTwo(tableName);

          List<Integer> result = new ArrayList<>();
          StreamingQuery query =
              spark()
                  .readStream()
                  .format("delta")
                  .option("ignoreChanges", "true")
                  .table(tableName)
                  .writeStream()
                  .option("checkpointLocation", checkpoint())
                  .foreachBatch(
                      (VoidFunction2<Dataset<Row>, Long>)
                          (df, id) -> result.addAll(ints(df, "value")))
                  .start();
          try {
            query.processAllAvailable();
            assertThat(result).containsExactlyInAnyOrder(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

            // Partial DELETE(3) → DV on the {2,3} file → re-emits surviving row 2.
            sql("DELETE FROM %s WHERE value = 3", tableName);
            // Append a new file with value 10.
            sql("INSERT INTO %s VALUES (10)", tableName);
            query.processAllAvailable();

            // Scala answerWithIgnoreChanges = (0 to 10) :+ 2 → 0-10 plus an extra re-emitted 2.
            List<Integer> expected = new ArrayList<>(intRangeList(0, 11));
            expected.add(2);
            assertThat(result).containsExactlyInAnyOrderElementsOf(expected);
          } finally {
            query.stop();
          }
        });
  }

  // ---------------------------------------------------------------------------
  // 10. "updating source table when ignoreDeletes = true fails the query - using DELETE"
  //     Source: DeltaSourceDeletionVectorTests.testQuietly("updating source table when
  //             ignoreDeletes = true … DELETE")
  //
  // ignoreDeletes=true allows whole-file deletes but NOT partial updates (DVs).
  // A partial DELETE (DV) must still fail with "skipChangeCommits".
  // ---------------------------------------------------------------------------
  @TestAllTableTypes
  public void testUpdatingSourceTableWithIgnoreDeletesStillFails(TableType tableType)
      throws Exception {
    withNewTable(
        "dv_update_ignore_deletes_fails",
        "value INT",
        null,
        tableType,
        DV_PROPS,
        tableName -> {
          // groups of 2 → DELETE value=3 is a PARTIAL delete of the {2,3} file → DV.
          insertGroupsOfTwo(tableName);

          StreamingQuery query =
              spark()
                  .readStream()
                  .format("delta")
                  .option("ignoreDeletes", "true")
                  .table(tableName)
                  .writeStream()
                  .option("checkpointLocation", checkpoint())
                  .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (df, id) -> {})
                  .start();
          try {
            query.processAllAvailable();

            // ignoreDeletes allows whole-file removals but NOT partial updates (DVs). This partial
            // DELETE is a "data update" → fails despite ignoreDeletes=true.
            sql("DELETE FROM %s WHERE value = 3", tableName);
            assertStreamingThrowsContaining(
                query::processAllAvailable, "data update", "skipChangeCommits");
          } finally {
            query.stop();
          }
        });
  }

  // ---------------------------------------------------------------------------
  // 11-14. "subsequent DML commands are processed correctly in a batch - DELETE->DELETE"
  //        with option combos: List(), ignoreDeletes, skipChangeCommits, ignoreChanges
  //
  // Source: DeltaSourceDeletionVectorTests – allSourceOptions loop for DELETE->DELETE
  //         (ignoreOperationsTestWithManualClock)
  //
  // Scala ordering reproduced: insert 0-14 in groups of 3 (one file each, so rows i,i+1,i+2 share
  // a file). Start a CONTINUOUS stream, processAllAvailable() to consume the initial snapshot
  // (batch 0 = CheckAnswer(0..14)). Then commit BOTH DELETEs and processAllAvailable() once more,
  // so the two commits form the next batch together.
  // ---------------------------------------------------------------------------

  /**
   * DELETE->DELETE with no options: the first DELETE is a partial update (DV), which must fail with
   * "skipChangeCommits" once processed after the initial snapshot.
   */
  @TestAllTableTypes
  public void testSubsequentDmlDeleteDeleteNoOptions(TableType tableType) throws Exception {
    withNewTable(
        "dv_dml_dd_no_opts",
        "value INT",
        null,
        tableType,
        DV_PROPS,
        tableName -> {
          insertGroupsOfThree(tableName);

          List<Integer> result = new ArrayList<>();
          StreamingQuery query = startContinuousStream(tableName, new String[0], result);
          try {
            query.processAllAvailable();
            assertThat(result).containsExactlyInAnyOrder(intRange(0, 15));

            sql("DELETE FROM %s WHERE value = 3", tableName);
            sql("DELETE FROM %s WHERE value = 4", tableName);
            assertStreamingThrowsContaining(query::processAllAvailable, "skipChangeCommits");
          } finally {
            query.stop();
          }
        });
  }

  /**
   * DELETE->DELETE with ignoreDeletes=true: partial DELETEs are still data updates, so the query
   * must fail with "skipChangeCommits".
   */
  @TestAllTableTypes
  public void testSubsequentDmlDeleteDeleteIgnoreDeletes(TableType tableType) throws Exception {
    withNewTable(
        "dv_dml_dd_ignore_deletes",
        "value INT",
        null,
        tableType,
        DV_PROPS,
        tableName -> {
          insertGroupsOfThree(tableName);

          List<Integer> result = new ArrayList<>();
          StreamingQuery query =
              startContinuousStream(tableName, new String[] {"ignoreDeletes", "true"}, result);
          try {
            query.processAllAvailable();
            assertThat(result).containsExactlyInAnyOrder(intRange(0, 15));

            sql("DELETE FROM %s WHERE value = 3", tableName);
            sql("DELETE FROM %s WHERE value = 4", tableName);
            assertStreamingThrowsContaining(query::processAllAvailable, "skipChangeCommits");
          } finally {
            query.stop();
          }
        });
  }

  /**
   * DELETE->DELETE with skipChangeCommits=true: both DELETE commits are silently skipped. The
   * stream only ever emits the initial snapshot. Scala CheckAnswer((0 until 15)) = 0-14.
   */
  @TestAllTableTypes
  public void testSubsequentDmlDeleteDeleteSkipChangeCommits(TableType tableType) throws Exception {
    withNewTable(
        "dv_dml_dd_skip_change",
        "value INT",
        null,
        tableType,
        DV_PROPS,
        tableName -> {
          insertGroupsOfThree(tableName);

          List<Integer> result = new ArrayList<>();
          StreamingQuery query =
              startContinuousStream(tableName, new String[] {"skipChangeCommits", "true"}, result);
          try {
            query.processAllAvailable();
            assertThat(result).containsExactlyInAnyOrder(intRange(0, 15));

            sql("DELETE FROM %s WHERE value = 3", tableName);
            sql("DELETE FROM %s WHERE value = 4", tableName);
            query.processAllAvailable();

            // Both DELETE commits are skipped → still just 0-14
            assertThat(result).containsExactlyInAnyOrder(intRange(0, 15));
          } finally {
            query.stop();
          }
        });
  }

  /**
   * DELETE->DELETE with ignoreChanges=true: each partial DELETE re-emits the surviving rows of the
   * touched file. Rows 3,4,5 share one file.
   *
   * <p>Scala CheckAnswer((0 until 15) ++ Seq(4, 5, 5)): after DELETE(3) the file is re-emitted with
   * rows {4,5}; after DELETE(4) the file (DV={3}) is re-emitted with {5}. Total = 0-14 + [4,5,5].
   */
  @TestAllTableTypes
  public void testSubsequentDmlDeleteDeleteIgnoreChanges(TableType tableType) throws Exception {
    withNewTable(
        "dv_dml_dd_ignore_changes",
        "value INT",
        null,
        tableType,
        DV_PROPS,
        tableName -> {
          insertGroupsOfThree(tableName);

          List<Integer> result = new ArrayList<>();
          StreamingQuery query =
              startContinuousStream(tableName, new String[] {"ignoreChanges", "true"}, result);
          try {
            query.processAllAvailable();
            assertThat(result).containsExactlyInAnyOrder(intRange(0, 15));

            sql("DELETE FROM %s WHERE value = 3", tableName);
            sql("DELETE FROM %s WHERE value = 4", tableName);
            query.processAllAvailable();

            // Baseline 0-14 + re-emitted {4,5} from DELETE(3) + re-emitted {5} from DELETE(4)
            List<Integer> expected = new ArrayList<>(intRangeList(0, 15));
            expected.add(4);
            expected.add(5);
            expected.add(5);
            assertThat(result).containsExactlyInAnyOrderElementsOf(expected);
          } finally {
            query.stop();
          }
        });
  }

  // ---------------------------------------------------------------------------
  // 15-18. "subsequent DML commands are processed correctly in a batch - INSERT->DELETE"
  //        with option combos: List(), ignoreDeletes, skipChangeCommits, ignoreChanges
  //
  // Source: DeltaSourceDeletionVectorTests – allSourceOptions loop for INSERT->DELETE
  // ---------------------------------------------------------------------------

  // INSERT->DELETE: initial 0-14 (groups of 3) processed first; then INSERT (15,16) and
  // DELETE(15) form the next batch together.

  /**
   * INSERT->DELETE with no options: the DELETE is a partial update (DV), must fail with
   * "skipChangeCommits" once processed after the initial snapshot.
   */
  @TestAllTableTypes
  public void testSubsequentDmlInsertDeleteNoOptions(TableType tableType) throws Exception {
    withNewTable(
        "dv_dml_id_no_opts",
        "value INT",
        null,
        tableType,
        DV_PROPS,
        tableName -> {
          insertGroupsOfThree(tableName);

          List<Integer> result = new ArrayList<>();
          StreamingQuery query = startContinuousStream(tableName, new String[0], result);
          try {
            query.processAllAvailable();
            assertThat(result).containsExactlyInAnyOrder(intRange(0, 15));

            // command1: INSERT 15,16 into one file; command2: DELETE 15 (DV on that file)
            // Single file (COALESCE(1)) so the later DELETE(15) is a partial delete (DV).
            sql(
                "INSERT INTO %s SELECT /*+ COALESCE(1) */ * FROM VALUES (15),(16) AS t(value)",
                tableName);
            sql("DELETE FROM %s WHERE value = 15", tableName);
            assertStreamingThrowsContaining(query::processAllAvailable, "skipChangeCommits");
          } finally {
            query.stop();
          }
        });
  }

  /** INSERT->DELETE with ignoreDeletes=true: partial DELETE is still a data update → fail. */
  @TestAllTableTypes
  public void testSubsequentDmlInsertDeleteIgnoreDeletes(TableType tableType) throws Exception {
    withNewTable(
        "dv_dml_id_ignore_deletes",
        "value INT",
        null,
        tableType,
        DV_PROPS,
        tableName -> {
          insertGroupsOfThree(tableName);

          List<Integer> result = new ArrayList<>();
          StreamingQuery query =
              startContinuousStream(tableName, new String[] {"ignoreDeletes", "true"}, result);
          try {
            query.processAllAvailable();
            assertThat(result).containsExactlyInAnyOrder(intRange(0, 15));

            // Single file (COALESCE(1)) so the later DELETE(15) is a partial delete (DV).
            sql(
                "INSERT INTO %s SELECT /*+ COALESCE(1) */ * FROM VALUES (15),(16) AS t(value)",
                tableName);
            sql("DELETE FROM %s WHERE value = 15", tableName);
            assertStreamingThrowsContaining(query::processAllAvailable, "skipChangeCommits");
          } finally {
            query.stop();
          }
        });
  }

  /**
   * INSERT->DELETE with skipChangeCommits=true: the DELETE commit is ignored; the INSERT is
   * delivered. Scala CheckAnswer((0 to 16)) = 0-16 (no duplicates).
   */
  @TestAllTableTypes
  public void testSubsequentDmlInsertDeleteSkipChangeCommits(TableType tableType) throws Exception {
    withNewTable(
        "dv_dml_id_skip_change",
        "value INT",
        null,
        tableType,
        DV_PROPS,
        tableName -> {
          insertGroupsOfThree(tableName);

          List<Integer> result = new ArrayList<>();
          StreamingQuery query =
              startContinuousStream(tableName, new String[] {"skipChangeCommits", "true"}, result);
          try {
            query.processAllAvailable();
            assertThat(result).containsExactlyInAnyOrder(intRange(0, 15));

            // Single file (COALESCE(1)) so the later DELETE(15) is a partial delete (DV).
            sql(
                "INSERT INTO %s SELECT /*+ COALESCE(1) */ * FROM VALUES (15),(16) AS t(value)",
                tableName);
            sql("DELETE FROM %s WHERE value = 15", tableName);
            query.processAllAvailable();

            // INSERT delivered (15,16), DELETE commit skipped → 0-16
            assertThat(result).containsExactlyInAnyOrder(intRange(0, 17));
          } finally {
            query.stop();
          }
        });
  }

  /**
   * INSERT->DELETE with ignoreChanges=true: INSERT delivers 15+16 in one file; DELETE(15) creates a
   * DV and re-emits the surviving row 16 as a duplicate. Scala CheckAnswer((0 to 16) ++ Seq(16)) =
   * 0-16 + extra 16.
   */
  @TestAllTableTypes
  public void testSubsequentDmlInsertDeleteIgnoreChanges(TableType tableType) throws Exception {
    withNewTable(
        "dv_dml_id_ignore_changes",
        "value INT",
        null,
        tableType,
        DV_PROPS,
        tableName -> {
          insertGroupsOfThree(tableName);

          List<Integer> result = new ArrayList<>();
          StreamingQuery query =
              startContinuousStream(tableName, new String[] {"ignoreChanges", "true"}, result);
          try {
            query.processAllAvailable();
            assertThat(result).containsExactlyInAnyOrder(intRange(0, 15));

            // INSERT 15 and 16 into ONE file; DELETE 15 → DV on the {15,16} file → re-emits {16}
            // Single file (COALESCE(1)) so the later DELETE(15) is a partial delete (DV).
            sql(
                "INSERT INTO %s SELECT /*+ COALESCE(1) */ * FROM VALUES (15),(16) AS t(value)",
                tableName);
            sql("DELETE FROM %s WHERE value = 15", tableName);
            query.processAllAvailable();

            // baseline 0-16 + extra re-emitted 16
            List<Integer> expected = new ArrayList<>(intRangeList(0, 17));
            expected.add(16);
            assertThat(result).containsExactlyInAnyOrderElementsOf(expected);
          } finally {
            query.stop();
          }
        });
  }

  // ---------------------------------------------------------------------------
  // 19. "multiple deletion vectors per file - List((ignoreChanges,true))"
  //     Source: DeltaSourceDeletionVectorTests – multiDVSourceOptions loop, ignoreChanges
  //
  // Start stream, read initial snapshot (0-9), then apply two partial DELETEs (DVs),
  // then processAllAvailable. Each DV commit re-emits the surviving file rows.
  // ---------------------------------------------------------------------------
  @TestAllTableTypes
  public void testMultipleDeletionVectorsPerFileIgnoreChanges(TableType tableType)
      throws Exception {
    withNewTable(
        "dv_multi_dv_ignore_changes",
        "value INT",
        null,
        tableType,
        DV_PROPS,
        tableName -> {
          // All 10 rows in a single file (COALESCE(1) hint), matching the Scala coalesce(1).
          insertSingleFile0to9(tableName);

          List<Integer> result = new ArrayList<>();
          StreamingQuery query =
              spark()
                  .readStream()
                  .format("delta")
                  .option("ignoreChanges", "true")
                  .table(tableName)
                  .writeStream()
                  .option("checkpointLocation", checkpoint())
                  .foreachBatch(
                      (VoidFunction2<Dataset<Row>, Long>)
                          (df, id) -> result.addAll(ints(df, "value")))
                  .start();
          try {
            query.processAllAvailable();
            assertThat(result).containsExactlyInAnyOrder(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

            // V1: Delete row 0 — first DV on the file
            sql("DELETE FROM %s WHERE value = 0", tableName);
            // V2: Delete row 1 — updates DV (cumulative: {0,1})
            sql("DELETE FROM %s WHERE value = 1", tableName);

            query.processAllAvailable();
            // v0: rows 0-9 (initial snapshot, already in result)
            // v1: file re-added with DV={0}   → rows 1-9
            // v2: file re-added with DV={0,1} → rows 2-9
            List<Integer> expected = new ArrayList<>();
            // initial: 0-9
            for (int i = 0; i <= 9; i++) expected.add(i);
            // after DELETE(0) re-emit: 1-9
            for (int i = 1; i <= 9; i++) expected.add(i);
            // after DELETE(1) re-emit: 2-9
            for (int i = 2; i <= 9; i++) expected.add(i);

            assertThat(result).containsExactlyInAnyOrderElementsOf(expected);
          } finally {
            query.stop();
          }
        });
  }

  // ---------------------------------------------------------------------------
  // 20. "multiple deletion vectors per file - List((ignoreFileDeletion,true))"
  //     Source: DeltaSourceDeletionVectorTests – multiDVSourceOptions loop, ignoreFileDeletion
  //
  // Same structure as above but with ignoreFileDeletion=true.
  // ---------------------------------------------------------------------------
  @TestAllTableTypes
  public void testMultipleDeletionVectorsPerFileIgnoreFileDeletion(TableType tableType)
      throws Exception {
    withNewTable(
        "dv_multi_dv_ignore_file_del",
        "value INT",
        null,
        tableType,
        DV_PROPS,
        tableName -> {
          insertSingleFile0to9(tableName);

          List<Integer> result = new ArrayList<>();
          StreamingQuery query =
              spark()
                  .readStream()
                  .format("delta")
                  .option("ignoreFileDeletion", "true")
                  .table(tableName)
                  .writeStream()
                  .option("checkpointLocation", checkpoint())
                  .foreachBatch(
                      (VoidFunction2<Dataset<Row>, Long>)
                          (df, id) -> result.addAll(ints(df, "value")))
                  .start();
          try {
            query.processAllAvailable();
            assertThat(result).containsExactlyInAnyOrder(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

            sql("DELETE FROM %s WHERE value = 0", tableName);
            sql("DELETE FROM %s WHERE value = 1", tableName);

            query.processAllAvailable();
            List<Integer> expected = new ArrayList<>();
            for (int i = 0; i <= 9; i++) expected.add(i);
            for (int i = 1; i <= 9; i++) expected.add(i);
            for (int i = 2; i <= 9; i++) expected.add(i);

            assertThat(result).containsExactlyInAnyOrderElementsOf(expected);
          } finally {
            query.stop();
          }
        });
  }

  // ---------------------------------------------------------------------------
  // 21. "streaming read with nulls and deletion vectors does not NPE"
  //     Source: DeltaSourceDeletionVectorTests.test("streaming read with nulls …")
  //
  // Regression for #6578: ColumnVectorWithFilter.closeIfFreeable must not release
  // the underlying Parquet vector's `nulls` array — the reader reuses it for the
  // next batch. Verified by the absence of a stream exception.
  // ---------------------------------------------------------------------------
  @TestAllTableTypes
  public void testStreamingReadWithNullsAndDvDoesNotNPE(TableType tableType) throws Exception {
    withNewTable(
        "dv_nulls_no_npe",
        "id INT, value STRING, opt_int INT",
        null,
        tableType,
        DV_PROPS,
        tableName -> {
          sql(
              "INSERT INTO %s VALUES (1, null, null), (2, 'b', null), (null, null, null)",
              tableName);
          sql("INSERT INTO %s VALUES (3, 'c', 3), (null, 'x', 1)", tableName);

          List<Row> rows = new ArrayList<>();
          StreamingQuery query =
              spark()
                  .readStream()
                  .format("delta")
                  .table(tableName)
                  .writeStream()
                  .trigger(Trigger.AvailableNow())
                  .option("checkpointLocation", checkpoint())
                  .foreachBatch(
                      (VoidFunction2<Dataset<Row>, Long>)
                          (df, id) -> rows.addAll(df.collectAsList()))
                  .start();
          query.awaitTermination();

          // The query must complete without an NPE exception
          assertThat(rows).hasSize(5);
        });
  }

  // ---------------------------------------------------------------------------
  // 22. "streaming read with variant column and deletion vectors does not ClassCastException"
  //     Source: DeltaSourceDeletionVectorTests.test("streaming read with variant …")
  //
  // Regression for #6578: ColumnVectorWithFilter.getChild must not assume dataType()
  // is a StructType — Spark's ColumnVector.getVariant calls getChild(0) on VARIANT vectors.
  // ---------------------------------------------------------------------------
  @TestAllTableTypes
  public void testStreamingReadWithVariantColumnAndDvDoesNotClassCastException(TableType tableType)
      throws Exception {
    withNewTable(
        "dv_variant_no_cce",
        "id INT, data VARIANT",
        null,
        tableType,
        DV_PROPS,
        tableName -> {
          sql(
              "INSERT INTO %s SELECT 1, parse_json('{\"key\": \"value\"}')"
                  + " UNION ALL SELECT 2, parse_json('[1,2,3]')",
              tableName);
          sql("INSERT INTO %s SELECT 3, parse_json('{\"nested\": {\"a\": 1}}')", tableName);

          List<Row> rows = new ArrayList<>();
          StreamingQuery query =
              spark()
                  .readStream()
                  .format("delta")
                  .table(tableName)
                  .writeStream()
                  .trigger(Trigger.AvailableNow())
                  .option("checkpointLocation", checkpoint())
                  .foreachBatch(
                      (VoidFunction2<Dataset<Row>, Long>)
                          (df, id) -> rows.addAll(df.collectAsList()))
                  .start();
          query.awaitTermination();

          // Query must complete without a ClassCastException
          assertThat(rows).hasSize(3);
        });
  }

  // ---------------------------------------------------------------------------
  // 23. "variant column and deletion vectors preserve payload identity"
  //     Source: DeltaSourceDeletionVectorTests.test("variant column and deletion vectors …")
  //
  // Partial DELETE on a VARIANT table creates a DV. Streaming should apply the DV
  // and expose the correct variant payload for surviving rows.
  // ---------------------------------------------------------------------------
  @TestAllTableTypes
  public void testVariantColumnAndDvPreservePayloadIdentity(TableType tableType) throws Exception {
    withNewTable(
        "dv_variant_payload",
        "id INT, v VARIANT",
        null,
        tableType,
        DV_PROPS,
        tableName -> {
          // Insert rows 0-9 into a single file (COALESCE(1)) so DELETE creates a DV
          sql(
              "INSERT INTO %s"
                  + " SELECT /*+ COALESCE(1) */ id, parse_json(concat('{\"row\":', id, '}'))"
                  + " FROM (SELECT explode(sequence(0, 9)) AS id)",
              tableName);
          // Delete even rows → DV on the single file
          sql("DELETE FROM %s WHERE id %% 2 = 0", tableName);

          List<Row> rows = new ArrayList<>();
          spark()
              .readStream()
              .format("delta")
              .table(tableName)
              .writeStream()
              .trigger(Trigger.AvailableNow())
              .option("checkpointLocation", checkpoint())
              .foreachBatch(
                  (VoidFunction2<Dataset<Row>, Long>)
                      (df, id) ->
                          rows.addAll(
                              df.selectExpr(
                                      "id", "variant_get(v, '$.row', 'long') as row_in_payload")
                                  .collectAsList()))
              .start()
              .awaitTermination();

          // Only odd ids survive; payload must match id
          assertThat(rows).hasSize(5);
          for (Row r : rows) {
            int id = r.getInt(0);
            long payload = r.getLong(1);
            assertThat(id).isOdd();
            assertThat(payload).isEqualTo(id);
          }
        });
  }

  // ---------------------------------------------------------------------------
  // 24. "array column and deletion vectors preserve element identity"
  //     Source: DeltaSourceDeletionVectorTests.test("array column and deletion vectors …")
  //
  // DELETE even-id rows via DV; surviving rows must expose correct array elements.
  // ---------------------------------------------------------------------------
  @TestAllTableTypes
  public void testArrayColumnAndDvPreserveElementIdentity(TableType tableType) throws Exception {
    withNewTable(
        "dv_array_identity",
        "id INT, arr ARRAY<INT>",
        null,
        tableType,
        DV_PROPS,
        tableName -> {
          // Insert rows 0-9 with arr=[id*10, id*10+1] into a single file (COALESCE(1)) → DV on
          // DELETE
          sql(
              "INSERT INTO %s"
                  + " SELECT /*+ COALESCE(1) */ id, array(id*10, id*10+1)"
                  + " FROM (SELECT explode(sequence(0, 9)) AS id)",
              tableName);
          // Delete even rows → DV
          sql("DELETE FROM %s WHERE id %% 2 = 0", tableName);

          List<Row> rows = new ArrayList<>();
          spark()
              .readStream()
              .format("delta")
              .table(tableName)
              .writeStream()
              .trigger(Trigger.AvailableNow())
              .option("checkpointLocation", checkpoint())
              .foreachBatch(
                  (VoidFunction2<Dataset<Row>, Long>)
                      (df, id) ->
                          rows.addAll(
                              df.selectExpr("id", "arr[0] as a0", "arr[1] as a1").collectAsList()))
              .start()
              .awaitTermination();

          // Only odd ids survive; array elements must be id*10 and id*10+1
          assertThat(rows).hasSize(5);
          for (Row r : rows) {
            int id = r.getInt(0);
            int a0 = r.getInt(1);
            int a1 = r.getInt(2);
            assertThat(id).isOdd();
            assertThat(a0).isEqualTo(id * 10);
            assertThat(a1).isEqualTo(id * 10 + 1);
          }
        });
  }

  // ---------------------------------------------------------------------------
  // Private helpers
  // ---------------------------------------------------------------------------

  /**
   * Starts a CONTINUOUS streaming query (no trigger) that collects the {@code value} column into
   * {@code out}, with the given option key/value pairs (flat array: k0, v0, k1, v1, …).
   *
   * <p>This does not use {@link Trigger#AvailableNow()} so the caller can interleave {@code
   * processAllAvailable()} calls with DML commits, reproducing the Scala {@code StreamManualClock}
   * ordering: the initial snapshot is processed as one batch, and a later set of DML commits forms
   * the next batch. The caller owns stopping the query.
   */
  private StreamingQuery startContinuousStream(
      String tableName, String[] kvOptions, List<Integer> out) throws Exception {
    var reader = spark().readStream().format("delta");
    for (int i = 0; i + 1 < kvOptions.length; i += 2) {
      reader = reader.option(kvOptions[i], kvOptions[i + 1]);
    }
    return reader
        .table(tableName)
        .writeStream()
        .option("checkpointLocation", checkpoint())
        .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (df, id) -> out.addAll(ints(df, "value")))
        .start();
  }

  /**
   * Inserts integers [0, 15) in groups of 3 per commit, each group coalesced into ONE file. This
   * mirrors the Scala {@code ignoreOperationsTestWithManualClock} setup ({@code
   * Seq(i,i+1,i+2).toDF().coalesce(1)}), so that a single-row DELETE (e.g. {@code value = 3}) is a
   * PARTIAL delete that produces a Deletion Vector (a "data update"), not a whole-file removal.
   */
  private void insertGroupsOfThree(String tableName) {
    for (int i = 0; i < 15; i += 3) {
      sql(
          "INSERT INTO %s SELECT /*+ COALESCE(1) */ * FROM VALUES (%d),(%d),(%d) AS t(value)",
          tableName, i, i + 1, i + 2);
    }
  }

  /**
   * Inserts integers [0, 10) in groups of 2 per commit, each group coalesced into ONE file. This
   * mirrors the Scala {@code ignoreOperationsTest} setup ({@code Seq(i, i+1).coalesce(1)}); it
   * guarantees a single-row DELETE (e.g. {@code value = 3}) is a PARTIAL delete that produces a
   * Deletion Vector (a "data update"), not a whole-file removal.
   */
  private void insertGroupsOfTwo(String tableName) {
    for (int i = 0; i < 10; i += 2) {
      sql(
          "INSERT INTO %s SELECT /*+ COALESCE(1) */ * FROM VALUES (%d),(%d) AS t(value)",
          tableName, i, i + 1);
    }
  }

  /**
   * Inserts integers 0-9 as a SINGLE file using the {@code COALESCE(1)} SQL hint (mirrors the Scala
   * {@code coalesce(1)} writes). A single file is required so that a partial DELETE produces a
   * Deletion Vector on that one file rather than removing a whole file.
   */
  private void insertSingleFile0to9(String tableName) {
    sql(
        "INSERT INTO %s SELECT /*+ COALESCE(1) */ * FROM VALUES (0),(1),(2),(3),(4),(5),(6),(7),"
            + "(8),(9) AS t(value)",
        tableName);
  }

  /** Returns an Integer array containing [start, end). For use in assertj vararg methods. */
  private Integer[] intRange(int start, int end) {
    Integer[] arr = new Integer[end - start];
    for (int i = 0; i < arr.length; i++) arr[i] = start + i;
    return arr;
  }

  private List<Integer> intRangeList(int start, int end) {
    List<Integer> list = new ArrayList<>();
    for (int i = start; i < end; i++) list.add(i);
    return list;
  }

  /** Collects the named integer column from a DataFrame. */
  private List<Integer> ints(Dataset<Row> df, String col) {
    return df.select(col).collectAsList().stream()
        .map(r -> r.isNullAt(0) ? null : r.getInt(0))
        .collect(Collectors.toList());
  }

  /**
   * Asserts that {@code action} throws an exception whose cause chain contains all the given {@code
   * fragments} (case-insensitive).
   */
  private static void assertStreamingThrowsContaining(
      ThrowingCallable action, String... fragments) {
    assertThatThrownBy(action)
        .satisfies(
            e -> {
              StringBuilder full = new StringBuilder();
              for (Throwable t = e; t != null; t = t.getCause()) {
                if (t.getMessage() != null) full.append(t.getMessage()).append(' ');
              }
              String msg = full.toString();
              for (String fragment : fragments) {
                assertThat(msg).containsIgnoringCase(fragment);
              }
            });
  }
}
