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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.StreamingQueryProgress;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.io.TempDir;

/**
 * Ports {@link io.delta.spark.internal.v2.V2StreamingRestoreTest} (DSv2 EXTERNAL via {@code
 * dsv2.delta.<path>}) to Unity Catalog so each case runs against both EXTERNAL and MANAGED tables
 * via {@code @TestAllTableTypes}.
 *
 * <p>RESTORE writes a single commit containing {@code RemoveFile(dataChange=true)} for files in the
 * current snapshot that are absent in the target snapshot, and {@code AddFile(dataChange=true)} for
 * files in the target snapshot that are absent in the current snapshot. On UC MANAGED tables
 * RESTORE is allowed as long as clustering does not change (see {@code
 * UCDeltaTableBlockMetadataUpdateTest#testRestoreTableWithUnchangedClusteringSucceeds}); the tests
 * here use plain (non-clustered) tables, so RESTORE composes with MANAGED.
 *
 * <p>Skipped on MANAGED:
 *
 * <ul>
 *   <li>{@code testRestoreOnColMappedTable_streamThrough_skipChangeCommits}: needs ALTER TABLE ADD
 *       COLUMN, blocked by UCSingleCatalog on managed tables.
 * </ul>
 */
public class UCDeltaStreamingRestoreTest extends UCDeltaTableIntegrationBaseTest {

  @TempDir private Path tempDir;
  private int checkpointCount;

  /** Fresh local checkpoint dir; UC server holds cloud creds, not Spark. */
  private String checkpoint() throws IOException {
    Path ckDir = tempDir.resolve("ck-" + checkpointCount++);
    Files.createDirectory(ckDir);
    return ckDir.toString();
  }

  private String queryName(String tag, TableType tableType) {
    return tag + "_" + tableType.name().toLowerCase() + "_" + checkpointCount;
  }

  /** Format a UNIX-millis instant as the session-local-timezone string Spark parses. */
  private String formatTs(long millis) {
    String tz = spark().sessionState().conf().sessionLocalTimeZone();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    sdf.setTimeZone(TimeZone.getTimeZone(tz));
    return sdf.format(new Date(millis));
  }

  private void restoreTo(String tableName, long version) {
    sql("RESTORE TABLE %s TO VERSION AS OF %d", tableName, version);
  }

  /** Drain a memory-sink streaming query and return the materialized rows. */
  private List<Row> drainMemorySink(Dataset<Row> df, String qName) throws Exception {
    StreamingQuery q = null;
    try {
      q =
          df.writeStream()
              .format("memory")
              .queryName(qName)
              .outputMode("append")
              .option("checkpointLocation", checkpoint())
              .start();
      q.processAllAvailable();
      return spark().sql("SELECT * FROM " + qName).collectAsList();
    } finally {
      if (q != null) {
        q.stop();
      }
      spark().sql("DROP VIEW IF EXISTS " + qName);
    }
  }

  // 1. v0=CREATE; v1..v3 appends; v4 RESTORE to v1. Stream with skipChangeCommits=true.
  @TestAllTableTypes
  public void testRestoreBetweenStreamStartAndEnd_withSkipChangeCommits(TableType tableType)
      throws Exception {
    withNewTable(
        "restore_skip",
        "id INT",
        null,
        tableType,
        tableName -> {
          long v0 = currentVersion(tableName);
          sql("INSERT INTO %s SELECT cast(id as int) FROM range(0, 10)", tableName); // v1
          sql("INSERT INTO %s SELECT cast(id as int) FROM range(10, 20)", tableName); // v2
          sql("INSERT INTO %s SELECT cast(id as int) FROM range(20, 30)", tableName); // v3
          restoreTo(tableName, v0 + 1); // v4: RESTORE to v1

          Dataset<Row> df =
              spark()
                  .readStream()
                  .format("delta")
                  .option("skipChangeCommits", "true")
                  .table(tableName);

          List<Row> rows = drainMemorySink(df, queryName("restore_skip", tableType));
          // RESTORE fully skipped under skipChangeCommits=true; stream sees v1+v2+v3 appends = 30.
          assertEquals(
              30,
              rows.size(),
              "skipChangeCommits should drop RESTORE; expected 30 rows: " + rows.size());
        });
  }

  // 2. startingTimestamp captured just before RESTORE resolves to the RESTORE commit (latest
  // mtime), not the restored version's original mtime.
  @TestAllTableTypes
  public void testStartingTimestamp_acrossRestoreVersion(TableType tableType) throws Exception {
    withNewTable(
        "restore_st",
        "id INT",
        null,
        tableType,
        tableName -> {
          long v0 = currentVersion(tableName);
          sql("INSERT INTO %s SELECT cast(id as int) FROM range(0, 10)", tableName); // v1
          sql("INSERT INTO %s SELECT cast(id as int) FROM range(10, 20)", tableName); // v2
          sql("INSERT INTO %s SELECT cast(id as int) FROM range(20, 30)", tableName); // v3

          Thread.sleep(200);
          long cutoff = System.currentTimeMillis();
          Thread.sleep(200);
          restoreTo(tableName, v0 + 1); // v4

          Dataset<Row> df =
              spark()
                  .readStream()
                  .format("delta")
                  .option("ignoreChanges", "true")
                  .option("startingTimestamp", formatTs(cutoff))
                  .table(tableName);

          List<Row> rows = drainMemorySink(df, queryName("restore_st", tableType));
          // startingTimestamp=cutoff resolves to the RESTORE commit; ignoreChanges emits its 10
          // AddFile rows (the v1 contents re-added). A larger row count would mean the timestamp
          // drifted backward to v1's original mtime.
          assertEquals(
              10,
              rows.size(),
              "startingTimestamp just before RESTORE should resolve to the RESTORE commit; got: "
                  + rows.size());
        });
  }

  // 3. startingVersion at the RESTORE commit with ignoreChanges emits only the RESTORE's AddFiles.
  @TestAllTableTypes
  public void testStartingVersion_atRestoreCommit(TableType tableType) throws Exception {
    withNewTable(
        "restore_sv",
        "id INT",
        null,
        tableType,
        tableName -> {
          long v0 = currentVersion(tableName);
          sql("INSERT INTO %s SELECT cast(id as int) FROM range(0, 10)", tableName); // v1
          sql("INSERT INTO %s SELECT cast(id as int) FROM range(10, 20)", tableName); // v2
          sql("INSERT INTO %s SELECT cast(id as int) FROM range(20, 30)", tableName); // v3
          restoreTo(tableName, v0 + 1); // v4
          long restoreVersion = currentVersion(tableName);

          Dataset<Row> df =
              spark()
                  .readStream()
                  .format("delta")
                  .option("ignoreChanges", "true")
                  .option("startingVersion", Long.toString(restoreVersion))
                  .table(tableName);

          List<Row> rows = drainMemorySink(df, queryName("restore_sv", tableType));
          assertEquals(
              10,
              rows.size(),
              "startingVersion="
                  + restoreVersion
                  + " should emit only the RESTORE commit's AddFiles (10 rows); got: "
                  + rows.size());
        });
  }

  // 4. DV-enabled: v1 insert(0..9), v2 DV-DELETE(id<5), v3 RESTORE to v1.
  // skipChangeCommits drops v2+v3; stream emits only v1 = 10 rows.
  @TestAllTableTypes
  public void testRestoreOnDvTable_streamThrough_skipChangeCommits(TableType tableType)
      throws Exception {
    withNewTable(
        "restore_dv_skip",
        "id INT",
        null,
        tableType,
        "'delta.enableDeletionVectors' = 'true'",
        tableName -> {
          long v0 = currentVersion(tableName);
          sql("INSERT INTO %s SELECT cast(id as int) FROM range(0, 10)", tableName); // v1
          sql("DELETE FROM %s WHERE id < 5", tableName); // v2
          restoreTo(tableName, v0 + 1); // v3

          Dataset<Row> df =
              spark()
                  .readStream()
                  .format("delta")
                  .option("skipChangeCommits", "true")
                  .table(tableName);

          List<Row> rows = drainMemorySink(df, queryName("restore_dv_skip", tableType));
          assertEquals(
              10,
              rows.size(),
              "DV + RESTORE under skipChangeCommits should emit only v1; got: " + rows.size());
        });
  }

  // 5. Partitioned: v1 inserts; v2 INSERT OVERWRITE PARTITION(p=0); v3 RESTORE to v1.
  // skipChangeCommits drops v2+v3; stream sees v1's 10 rows.
  @TestAllTableTypes
  public void testRestoreOnPartitionedTable_streamThrough_skipChangeCommits(TableType tableType)
      throws Exception {
    withNewTable(
        "restore_part_skip",
        "id INT, p INT",
        "p",
        tableType,
        tableName -> {
          long v0 = currentVersion(tableName);
          // v1: 5 rows in p=0, 5 in p=1.
          sql(
              "INSERT INTO %s SELECT cast(id as int), cast(id %% 2 as int) FROM range(0, 10)",
              tableName);
          // v2: INSERT OVERWRITE PARTITION rewrites p=0.
          sql(
              "INSERT OVERWRITE TABLE %s PARTITION (p=0) "
                  + "VALUES (100), (102), (104), (106), (108)",
              tableName);
          restoreTo(tableName, v0 + 1); // v3 = RESTORE to v1

          Dataset<Row> df =
              spark()
                  .readStream()
                  .format("delta")
                  .option("skipChangeCommits", "true")
                  .table(tableName);

          List<Row> rows = drainMemorySink(df, queryName("restore_part_skip", tableType));
          assertEquals(
              10,
              rows.size(),
              "Partitioned + OVERWRITE + RESTORE under skipChangeCommits should emit v1's 10 rows; got: "
                  + rows.size());
        });
  }

  // 6. Column-mapped: v1 inserts; v2 ADD COLUMN; v3 inserts with new col; v4 RESTORE to v1.
  // KNOWN-GAP: skipped on MANAGED - ALTER TABLE ADD COLUMN is blocked by UCSingleCatalog
  // (see UCDeltaTableBlockMetadataUpdateTest::testAlterTableOperationsAreBlocked).
  @TestAllTableTypes
  public void testRestoreOnColMappedTable_streamThrough_skipChangeCommits(TableType tableType)
      throws Exception {
    Assumptions.assumeFalse(
        tableType == TableType.MANAGED,
        "KNOWN-GAP: ALTER TABLE ADD COLUMN is blocked on UC MANAGED tables");

    withNewTable(
        "restore_cm_skip",
        "id INT, value STRING",
        null,
        tableType,
        "'delta.columnMapping.mode' = 'name'",
        tableName -> {
          long v0 = currentVersion(tableName);
          sql("INSERT INTO %s VALUES (0, 'a'), (1, 'b'), (2, 'c')", tableName); // v1
          sql("ALTER TABLE %s ADD COLUMN (value2 STRING)", tableName); // v2
          sql("INSERT INTO %s VALUES (3, 'd', 'D'), (4, 'e', 'E')", tableName); // v3
          restoreTo(tableName, v0 + 1); // v4 = RESTORE to v1

          Dataset<Row> df =
              spark()
                  .readStream()
                  .format("delta")
                  .option("skipChangeCommits", "true")
                  .table(tableName);

          List<Row> rows = drainMemorySink(df, queryName("restore_cm_skip", tableType));
          assertEquals(
              5,
              rows.size(),
              "CM + ADD COLUMN + RESTORE under skipChangeCommits should emit 5 rows; got: "
                  + rows.size());
        });
  }

  // 7. Pin the design difference between ignoreChanges (30+10=40) and skipChangeCommits (30) on
  // the same RESTORE setup.
  @TestAllTableTypes
  public void testRestoreWithIgnoreChanges(TableType tableType) throws Exception {
    withNewTable(
        "restore_ic_eq",
        "id INT",
        null,
        tableType,
        tableName -> {
          long v0 = currentVersion(tableName);
          sql("INSERT INTO %s SELECT cast(id as int) FROM range(0, 10)", tableName); // v1
          sql("INSERT INTO %s SELECT cast(id as int) FROM range(10, 20)", tableName); // v2
          sql("INSERT INTO %s SELECT cast(id as int) FROM range(20, 30)", tableName); // v3
          restoreTo(tableName, v0 + 1); // v4

          Dataset<Row> ignoreChangesDF =
              spark().readStream().format("delta").option("ignoreChanges", "true").table(tableName);
          List<Row> ignoreChangesRows =
              drainMemorySink(ignoreChangesDF, queryName("restore_ic", tableType));

          Dataset<Row> skipDF =
              spark()
                  .readStream()
                  .format("delta")
                  .option("skipChangeCommits", "true")
                  .table(tableName);
          List<Row> skipRows = drainMemorySink(skipDF, queryName("restore_skip_eq", tableType));

          assertEquals(
              30, skipRows.size(), "skipChangeCommits drops the RESTORE -> 30 rows from v1+v2+v3");
          assertEquals(
              40,
              ignoreChangesRows.size(),
              "ignoreChanges emits RESTORE's AddFiles on top of v1+v2+v3 -> 40 rows; got: "
                  + ignoreChangesRows.size());
        });
  }

  // 8. Without ignoreChanges/skipChangeCommits, RESTORE throws DELTA_SOURCE_TABLE_IGNORE_CHANGES.
  @TestAllTableTypes
  public void testRestoreWithoutIgnoreChanges_throwsClearly(TableType tableType) throws Exception {
    withNewTable(
        "restore_no_ignore",
        "id INT",
        null,
        tableType,
        tableName -> {
          long v0 = currentVersion(tableName);
          sql("INSERT INTO %s SELECT cast(id as int) FROM range(0, 10)", tableName); // v1
          sql("INSERT INTO %s SELECT cast(id as int) FROM range(10, 20)", tableName); // v2
          restoreTo(tableName, v0 + 1); // v3

          String qName = queryName("restore_no_ignore", tableType);
          Dataset<Row> df = spark().readStream().format("delta").table(tableName);
          StreamingQuery q =
              df.writeStream()
                  .format("memory")
                  .queryName(qName)
                  .outputMode("append")
                  .option("checkpointLocation", checkpoint())
                  .start();

          StreamingQueryException ex;
          try {
            ex = assertThrows(StreamingQueryException.class, q::processAllAvailable);
          } finally {
            q.stop();
            spark().sql("DROP VIEW IF EXISTS " + qName);
          }

          String fullMsg = String.valueOf(ex) + " / " + String.valueOf(ex.getCause());
          assertTrue(
              fullMsg.contains("DELTA_SOURCE_TABLE_IGNORE_CHANGES")
                  || fullMsg.contains("ignoreChanges"),
              "Expected DELTA_SOURCE_TABLE_IGNORE_CHANGES; got: " + fullMsg);
          assertTrue(
              fullMsg.toUpperCase().contains("RESTORE"),
              "Expected error to mention RESTORE; got: " + fullMsg);
        });
  }

  // 9. AvailableNow must terminate when the snapshot tail includes a RESTORE commit under
  // skipChangeCommits.
  @TestAllTableTypes
  public void testRestore_triggerAvailableNow(TableType tableType) throws Exception {
    withNewTable(
        "restore_an",
        "id INT",
        null,
        tableType,
        tableName -> {
          long v0 = currentVersion(tableName);
          sql("INSERT INTO %s SELECT cast(id as int) FROM range(0, 10)", tableName); // v1
          sql("INSERT INTO %s SELECT cast(id as int) FROM range(10, 20)", tableName); // v2
          restoreTo(tableName, v0 + 1); // v3

          String qName = queryName("restore_an", tableType);
          StreamingQuery q =
              spark()
                  .readStream()
                  .format("delta")
                  .option("skipChangeCommits", "true")
                  .table(tableName)
                  .writeStream()
                  .format("memory")
                  .queryName(qName)
                  .outputMode("append")
                  .trigger(Trigger.AvailableNow())
                  .option("checkpointLocation", checkpoint())
                  .start();
          try {
            q.processAllAvailable();
            assertTrue(q.awaitTermination(60_000L), "AvailableNow should terminate within 60s");

            long count =
                spark().sql("SELECT COUNT(*) FROM " + qName).collectAsList().get(0).getLong(0);
            assertEquals(
                20L, count, "AvailableNow + RESTORE + skipChangeCommits should emit v1+v2 = 20");
          } finally {
            q.stop();
            spark().sql("DROP VIEW IF EXISTS " + qName);
          }
        });
  }

  // 10. RESTORE's AddFiles flow through the same rate-limiting path as ordinary AddFiles:
  // maxFilesPerTrigger=1 across a RESTORE that re-adds 3 files -> 3 non-empty batches.
  @TestAllTableTypes
  public void testRestore_maxFilesPerTrigger(TableType tableType) throws Exception {
    withNewTable(
        "restore_rate",
        "id INT",
        null,
        tableType,
        tableName -> {
          long v0 = currentVersion(tableName);
          // 3 single-file appends so the snapshot at v3 has 3 distinct files.
          for (int i = 0; i < 3; i++) {
            int lo = i * 10;
            int hi = lo + 10;
            sql(
                "INSERT INTO %s SELECT /*+ COALESCE(1) */ cast(id as int) FROM range(%d, %d)",
                tableName, lo, hi);
          }
          // v4: INSERT OVERWRITE collapses to one new file and removes the prior 3.
          sql("INSERT OVERWRITE TABLE %s VALUES (999)", tableName);
          // v5: RESTORE to v3 - re-adds the 3 prior files, removes the OVERWRITE file.
          restoreTo(tableName, v0 + 3);
          long restoreVersion = currentVersion(tableName);

          String qName = queryName("restore_rate", tableType);
          Dataset<Row> df =
              spark()
                  .readStream()
                  .format("delta")
                  .option("ignoreChanges", "true")
                  .option("maxFilesPerTrigger", "1")
                  .option("startingVersion", Long.toString(restoreVersion))
                  .table(tableName);
          StreamingQuery q =
              df.writeStream()
                  .format("memory")
                  .queryName(qName)
                  .outputMode("append")
                  .option("checkpointLocation", checkpoint())
                  .start();
          try {
            q.processAllAvailable();
            StreamingQueryProgress[] progress = q.recentProgress();
            long batchesWithRows = 0L;
            for (StreamingQueryProgress p : progress) {
              if (p.numInputRows() > 0L) {
                batchesWithRows++;
                assertTrue(
                    p.numInputRows() <= 10L,
                    "Each batch must read at most 1 file (10 rows); got " + p.numInputRows());
              }
            }
            assertEquals(
                3L,
                batchesWithRows,
                "maxFilesPerTrigger=1 across RESTORE should produce 3 non-empty batches; got "
                    + batchesWithRows);

            long total =
                spark().sql("SELECT COUNT(*) FROM " + qName).collectAsList().get(0).getLong(0);
            assertEquals(30L, total, "All 30 rows from the 3 restored files should reach the sink");
          } finally {
            q.stop();
            spark().sql("DROP VIEW IF EXISTS " + qName);
          }
        });
  }
}
