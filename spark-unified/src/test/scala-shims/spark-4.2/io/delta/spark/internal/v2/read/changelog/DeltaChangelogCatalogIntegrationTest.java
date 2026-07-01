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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.spark.internal.v2.snapshot.DeltaSnapshotManager;
import io.delta.spark.internal.v2.snapshot.SnapshotManagerFactory;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for catalog-routed CDC entrypoint (TableCatalog.loadChangelog).
 *
 * <p>These tests intentionally exercise SQL/DataFrame paths (not direct DeltaChangelog
 * construction) so they validate analyzer -> catalog -> changelog wiring.
 */
public class DeltaChangelogCatalogIntegrationTest extends DeltaChangelogTestBase {

  // ===========================================================================================
  // Fixtures and helpers
  // ===========================================================================================

  /**
   * Creates a row-tracking-enabled Delta table with 5 INSERT commits on top of CREATE, runs the
   * given body with the table name, and drops the table + path on completion.
   *
   * <p>Resulting commit history (used by all timestamp-range tests below):
   *
   * <pre>
   *   v0 = CREATE TABLE
   *   v1 = INSERT (1, 'Alice')
   *   v2 = INSERT (2, 'Bob')
   *   v3 = INSERT (3, 'Charlie')
   *   v4 = INSERT (4, 'Dave')
   *   v5 = INSERT (5, 'Eve')
   * </pre>
   */
  private void withHistoryTable(String suffix, ThrowingConsumer body) throws Exception {
    String tableName = "dsv2_cdc_catalog_ts_" + suffix + "_" + System.nanoTime();
    withTable(
        new String[] {tableName},
        () -> {
          spark.sql(
              String.format(
                  "CREATE TABLE %s (id BIGINT, name STRING) USING delta TBLPROPERTIES "
                      + "('delta.enableDeletionVectors'='false', 'delta.enableRowTracking'='true')",
                  tableName));
          spark.sql(String.format("INSERT INTO %s VALUES (1, 'Alice')", tableName));
          spark.sql(String.format("INSERT INTO %s VALUES (2, 'Bob')", tableName));
          spark.sql(String.format("INSERT INTO %s VALUES (3, 'Charlie')", tableName));
          spark.sql(String.format("INSERT INTO %s VALUES (4, 'Dave')", tableName));
          spark.sql(String.format("INSERT INTO %s VALUES (5, 'Eve')", tableName));
          // Resolve the managed table's location now, while the session is still in the default
          // mode. DESCRIBE DETAIL would fail under the STRICT mode set below (the table resolves to
          // a V2 DeltaV2Table that the v1 command can't address). The location lets commitTimestamp
          // read commit timestamps through the kernel snapshot manager, mode-independently.
          String location =
              spark
                  .sql("DESCRIBE DETAIL " + tableName)
                  .select("location")
                  .head()
                  .getString(0)
                  .replaceFirst("^file:/+", "/");
          // Read-time CDF requires the V2 connector at read time. Writes above run in the
          // session-default mode (AUTO -> V1 connector for INSERT). The CHANGES read in
          // the test body needs STRICT to ensure loadTable returns a V2 DeltaV2Table.
          withSQLConf(
              "spark.databricks.delta.v2.enableMode",
              "STRICT",
              () -> body.accept(tableName, location));
        });
  }

  @FunctionalInterface
  private interface ThrowingConsumer {
    void accept(String tableName, String tableLocation) throws Exception;
  }

  /**
   * Returns the commit timestamp of {@code version}. Resolves the snapshot directly through the
   * kernel snapshot manager, so it works irrespective of the catalog mode flipped on by the test
   * body (which keeps the catalog in STRICT for the read path of read-time CDF).
   */
  private java.sql.Timestamp commitTimestamp(String tableLocation, long version) {
    // Reads through the kernel snapshot manager, so it works inside the STRICT block the test body
    // runs in. tableLocation is resolved by withHistoryTable before STRICT is set.
    DeltaSnapshotManager snapshotManager =
        SnapshotManagerFactory.create(tableLocation, defaultEngine, Optional.empty());
    long millis = snapshotManager.loadSnapshotAt(version).getTimestamp(defaultEngine);
    return new java.sql.Timestamp(millis);
  }

  /**
   * Returns a wall-clock string strictly between two commit timestamps. Used to exercise {@code
   * getActiveCommitAtTime} with inputs that don't coincide with any commit's exact ts, so bounds
   * inclusivity does not change the resolved version.
   */
  private String betweenCommits(String tableLocation, long earlier, long later) {
    long earlierMs = commitTimestamp(tableLocation, earlier).getTime();
    long laterMs = commitTimestamp(tableLocation, later).getTime();
    return new java.sql.Timestamp(earlierMs + (laterMs - earlierMs) / 2).toString();
  }

  // ===========================================================================================
  // SQL / DataFrame parity
  // ===========================================================================================

  @Test
  public void testSqlAndDataFrameChangesMatchForVersionRange() throws Exception {
    String tableName = "dsv2_cdc_catalog_" + System.nanoTime();

    withTable(
        new String[] {tableName},
        () -> {
          spark.sql(
              String.format(
                  "CREATE TABLE %s (id BIGINT, name STRING) USING delta TBLPROPERTIES "
                      + "('delta.enableDeletionVectors'='false', 'delta.enableRowTracking'='true')",
                  tableName));
          spark.sql(String.format("INSERT INTO %s VALUES (1, 'Alice'), (2, 'Bob')", tableName));
          spark.sql(String.format("DELETE FROM %s WHERE id = 1", tableName));

          // CHANGES read needs the V2 connector; see withHistoryTable for the rationale.
          withSQLConf(
              "spark.databricks.delta.v2.enableMode",
              "STRICT",
              () -> {
                Dataset<Row> sqlDf =
                    spark
                        .sql(
                            String.format(
                                "SELECT id, name, _change_type, _commit_version "
                                    + "FROM %s CHANGES FROM VERSION 1 TO VERSION 2",
                                tableName))
                        .orderBy("_commit_version", "id", "_change_type", "name");

                Dataset<Row> apiDf =
                    spark
                        .read()
                        .option("startingVersion", "1")
                        .option("endingVersion", "2")
                        .changes(tableName)
                        .select("id", "name", "_change_type", "_commit_version")
                        .orderBy("_commit_version", "id", "_change_type", "name");

                List<Row> sqlRows = sqlDf.collectAsList();
                List<Row> apiRows = apiDf.collectAsList();
                assertFalse(
                    sqlRows.isEmpty(), "Expected non-empty CDC output for VERSION 1..2 range");
                assertEquals(
                    sqlRows, apiRows, "SQL CHANGES and DataFrameReader.changes should match");

                List<String> fieldNames = Arrays.asList(sqlDf.schema().fieldNames());
                assertTrue(fieldNames.contains("id"));
                assertTrue(fieldNames.contains("name"));
                assertTrue(fieldNames.contains("_change_type"));
                assertTrue(fieldNames.contains("_commit_version"));
              });
        });
  }

  // ===========================================================================================
  // Connector-mode routing for CHANGES reads
  // ===========================================================================================

  /**
   * Auto-CDF only flows through the V2 connector. {@code ChangelogSupport} re-resolves the
   * table to a {@code DeltaV2Table} for the CHANGES read, so a SQL CHANGES query must succeed
   * without forcing STRICT mode.
   */
  @Test
  public void testAutoModeRoutesChangesToV2() throws Exception {
    String tableName = "dsv2_cdc_catalog_auto_" + System.nanoTime();

    withTable(
        new String[] {tableName},
        () -> {
          spark.sql(
              String.format(
                  "CREATE TABLE %s (id BIGINT, name STRING) USING delta "
                      + "TBLPROPERTIES ('delta.enableDeletionVectors'='false', "
                      + "'delta.enableRowTracking'='true')",
                  tableName));
          spark.sql(String.format("INSERT INTO %s VALUES (1, 'Alice')", tableName));
          spark.sql(String.format("INSERT INTO %s VALUES (2, 'Bob')", tableName));
          spark.sql(String.format("INSERT INTO %s VALUES (3, 'Charlie')", tableName));

          // CREATE/INSERTs above run on the V1 connector under AUTO. The CHANGES read must
          // still succeed: ChangelogSupport re-resolves the table to the V2 connector.
          withSQLConf(
              "spark.databricks.delta.v2.enableMode",
              "AUTO",
              () -> {
                List<Row> rows =
                    spark
                        .sql(
                            String.format(
                                "SELECT id, name, _change_type "
                                    + "FROM %s CHANGES FROM VERSION 1 TO VERSION 3",
                                tableName))
                        .orderBy("_commit_version", "id")
                        .collectAsList();
                assertEquals(
                    3, rows.size(), "Expected three inserts in the v1..v3 range under AUTO");
                for (int i = 0; i < 3; i++) {
                  assertEquals((long) (i + 1), ((Number) rows.get(i).getAs("id")).longValue());
                  assertEquals("insert", rows.get(i).getAs("_change_type"));
                }
              });
        });
  }

  /**
   * STRICT routes every operation to the V2 connector, so loadTable already returns a
   * {@code DeltaV2Table} and the CHANGES read succeeds without any re-resolution.
   */
  @Test
  public void testStrictModeRoutesChangesToV2() throws Exception {
    String tableName = "dsv2_cdc_catalog_strict_" + System.nanoTime();

    withTable(
        new String[] {tableName},
        () -> {
          spark.sql(
              String.format(
                  "CREATE TABLE %s (id BIGINT, name STRING) USING delta "
                      + "TBLPROPERTIES ('delta.enableDeletionVectors'='false', "
                      + "'delta.enableRowTracking'='true')",
                  tableName));
          spark.sql(String.format("INSERT INTO %s VALUES (1, 'Alice')", tableName));
          spark.sql(String.format("INSERT INTO %s VALUES (2, 'Bob')", tableName));
          spark.sql(String.format("INSERT INTO %s VALUES (3, 'Charlie')", tableName));

          withSQLConf(
              "spark.databricks.delta.v2.enableMode",
              "STRICT",
              () -> {
                List<Row> rows =
                    spark
                        .sql(
                            String.format(
                                "SELECT id, name, _change_type "
                                    + "FROM %s CHANGES FROM VERSION 1 TO VERSION 3",
                                tableName))
                        .orderBy("_commit_version", "id")
                        .collectAsList();
                assertEquals(
                    3, rows.size(), "Expected three inserts in the v1..v3 range under STRICT");
                for (int i = 0; i < 3; i++) {
                  assertEquals((long) (i + 1), ((Number) rows.get(i).getAs("id")).longValue());
                  assertEquals("insert", rows.get(i).getAs("_change_type"));
                }
              });
        });
  }

  /**
   * NONE mode keeps every operation on the V1 connector, so the V2 Auto-CDF path is unavailable.
   * A CHANGES read must be rejected with {@code DELTA_CHANGELOG_REQUIRES_V2_TABLE}.
   */
  @Test
  public void testNoneModeRejectsChanges() throws Exception {
    String tableName = "dsv2_cdc_catalog_none_" + System.nanoTime();

    withTable(
        new String[] {tableName},
        () -> {
          spark.sql(
              String.format(
                  "CREATE TABLE %s (id BIGINT, name STRING) USING delta "
                      + "TBLPROPERTIES ('delta.enableDeletionVectors'='false', "
                      + "'delta.enableRowTracking'='true')",
                  tableName));
          spark.sql(String.format("INSERT INTO %s VALUES (1, 'Alice')", tableName));

          withSQLConf(
              "spark.databricks.delta.v2.enableMode",
              "NONE",
              () -> {
                Exception ex =
                    assertThrows(
                        Exception.class,
                        () ->
                            spark
                                .sql(
                                    String.format(
                                        "SELECT * FROM %s CHANGES FROM VERSION 0 TO " + "VERSION 1",
                                        tableName))
                                .collectAsList());
                assertTrue(
                    ex.getMessage().contains("DELTA_CHANGELOG_REQUIRES_V2_TABLE"),
                    "Expected V2-connector-required error under NONE, got: " + ex.getMessage());
              });
        });
  }

  // ===========================================================================================
  // Bounds inclusivity/exclusivity testing
  // ===========================================================================================

  // -------------------- default INCL/INCL --------------------

  @Test
  public void testTimestampRangeReadsAllChanges() throws Exception {
    withHistoryTable(
        "all",
        (tableName, location) -> {
          String startTs = commitTimestamp(location, 0).toString();
          String endTs = commitTimestamp(location, 5).toString();

          Dataset<Row> changes =
              spark
                  .sql(
                      String.format(
                          "SELECT id, name, _change_type "
                              + "FROM %s CHANGES FROM TIMESTAMP '%s' TO TIMESTAMP '%s'",
                          tableName, startTs, endTs))
                  .orderBy("_commit_version", "id");

          List<Row> rows = changes.collectAsList();
          assertEquals(5, rows.size(), "Expected all five inserts in the v0..v5 timestamp range");
          for (int i = 0; i < 5; i++) {
            assertEquals((long) (i + 1), ((Number) rows.get(i).getAs("id")).longValue());
            assertEquals("insert", rows.get(i).getAs("_change_type"));
          }
        });
  }

  @Test
  public void testTimestampRangePartialMiddleCommit() throws Exception {
    withHistoryTable(
        "partial",
        (tableName, location) -> {
          // Both bounds resolve to v3; range = [v3, v3] inclusive.
          String tsV3 = commitTimestamp(location, 3).toString();
          Dataset<Row> changes =
              spark.sql(
                  String.format(
                      "SELECT id, _change_type FROM %s "
                          + "CHANGES FROM TIMESTAMP '%s' TO TIMESTAMP '%s'",
                      tableName, tsV3, tsV3));

          List<Row> rows = changes.collectAsList();
          assertEquals(1, rows.size(), "Expected only the v3 insert in [v3, v3]");
          assertEquals(3L, ((Number) rows.get(0).getAs("id")).longValue());
          assertEquals("insert", rows.get(0).getAs("_change_type"));
        });
  }

  @Test
  public void testTimestampRangeBetweenCommitTimestamps() throws Exception {
    withHistoryTable(
        "between",
        (tableName, location) -> {
          // Start strictly between v1 and v2: getActiveCommitAtTime returns the latest commit
          // with ts <= start, so start resolves to v1.
          // End strictly between v2 and v3: same rule, end resolves to v2.
          // Range = [v1, v2] = Alice + Bob.
          String startTs = betweenCommits(location, 1, 2);
          String endTs = betweenCommits(location, 2, 3);

          Dataset<Row> changes =
              spark
                  .sql(
                      String.format(
                          "SELECT id, _change_type FROM %s "
                              + "CHANGES FROM TIMESTAMP '%s' TO TIMESTAMP '%s'",
                          tableName, startTs, endTs))
                  .orderBy("_commit_version", "id");

          List<Row> rows = changes.collectAsList();
          assertEquals(2, rows.size(), "Expected v1 and v2 inserts in between-commit range");
          assertEquals(1L, ((Number) rows.get(0).getAs("id")).longValue());
          assertEquals(2L, ((Number) rows.get(1).getAs("id")).longValue());
        });
  }

  // -------------------- exclusive-bound variants --------------------

  @Test
  public void testTimestampRangeExclusiveBoundsSkipBoundaryCommits() throws Exception {
    withHistoryTable(
        "excl",
        (tableName, location) -> {
          String tsV1 = commitTimestamp(location, 1).toString();
          String tsV3 = commitTimestamp(location, 3).toString();

          // FROM tsV1 EXCLUSIVE bumps start to v2; TO tsV3 EXCLUSIVE drops end to v2.
          // Range = [v2, v2] = only the (2, 'Bob') insert.
          Dataset<Row> changes =
              spark.sql(
                  String.format(
                      "SELECT id, _change_type FROM %s "
                          + "CHANGES FROM TIMESTAMP '%s' EXCLUSIVE TO TIMESTAMP '%s' EXCLUSIVE",
                      tableName, tsV1, tsV3));

          List<Row> rows = changes.collectAsList();
          assertEquals(1, rows.size(), "Expected only v2 inside exclusive bounds");
          assertEquals(2L, ((Number) rows.get(0).getAs("id")).longValue());
          assertEquals("insert", rows.get(0).getAs("_change_type"));
        });
  }

  @Test
  public void testTimestampRangeMixedBoundsStartExclusiveEndInclusive() throws Exception {
    withHistoryTable(
        "mixed_se_ei",
        (tableName, location) -> {
          String tsV1 = commitTimestamp(location, 1).toString();
          String tsV3 = commitTimestamp(location, 3).toString();

          // FROM tsV1 EXCLUSIVE bumps start to v2; TO tsV3 (default INCLUSIVE) keeps end at v3.
          // Range = [v2, v3] = Bob + Charlie.
          Dataset<Row> changes =
              spark
                  .sql(
                      String.format(
                          "SELECT id, _change_type FROM %s "
                              + "CHANGES FROM TIMESTAMP '%s' EXCLUSIVE TO TIMESTAMP '%s'",
                          tableName, tsV1, tsV3))
                  .orderBy("_commit_version", "id");

          List<Row> rows = changes.collectAsList();
          assertEquals(2, rows.size(), "Expected v2 and v3 inserts");
          assertEquals(2L, ((Number) rows.get(0).getAs("id")).longValue());
          assertEquals(3L, ((Number) rows.get(1).getAs("id")).longValue());
        });
  }

  @Test
  public void testTimestampRangeMixedBoundsStartInclusiveEndExclusive() throws Exception {
    withHistoryTable(
        "mixed_si_ee",
        (tableName, location) -> {
          String tsV1 = commitTimestamp(location, 1).toString();
          String tsV3 = commitTimestamp(location, 3).toString();

          // FROM tsV1 (default INCLUSIVE) keeps start at v1; TO tsV3 EXCLUSIVE drops end to v2.
          // Range = [v1, v2] = Alice + Bob.
          Dataset<Row> changes =
              spark
                  .sql(
                      String.format(
                          "SELECT id, _change_type FROM %s "
                              + "CHANGES FROM TIMESTAMP '%s' TO TIMESTAMP '%s' EXCLUSIVE",
                          tableName, tsV1, tsV3))
                  .orderBy("_commit_version", "id");

          List<Row> rows = changes.collectAsList();
          assertEquals(2, rows.size(), "Expected v1 and v2 inserts");
          assertEquals(1L, ((Number) rows.get(0).getAs("id")).longValue());
          assertEquals(2L, ((Number) rows.get(1).getAs("id")).longValue());
        });
  }

  // -------------------- open-ended end --------------------

  @Test
  public void testTimestampRangeOpenEndedReadsToLatest() throws Exception {
    withHistoryTable(
        "open_incl",
        (tableName, location) -> {
          // FROM tsV1 (default INCLUSIVE) keeps start at v1; no TO clause = read to latest (v5).
          // Range = [v1, v5] = all five inserts.
          String tsV1 = commitTimestamp(location, 1).toString();

          Dataset<Row> changes =
              spark
                  .sql(
                      String.format(
                          "SELECT id, _change_type FROM %s CHANGES FROM TIMESTAMP '%s'",
                          tableName, tsV1))
                  .orderBy("_commit_version", "id");

          List<Row> rows = changes.collectAsList();
          assertEquals(5, rows.size(), "Expected v1..v5 inclusive (all five inserts)");
          for (int i = 0; i < 5; i++) {
            assertEquals((long) (i + 1), ((Number) rows.get(i).getAs("id")).longValue());
          }
        });
  }

  @Test
  public void testTimestampRangeOpenEndedExclusiveStart() throws Exception {
    withHistoryTable(
        "open_excl",
        (tableName, location) -> {
          // FROM tsV1 EXCLUSIVE bumps start to v2; no TO clause = read to latest (v5).
          // Range = [v2, v5] = Bob + Charlie + Dave + Eve.
          String tsV1 = commitTimestamp(location, 1).toString();

          Dataset<Row> changes =
              spark
                  .sql(
                      String.format(
                          "SELECT id, _change_type FROM %s CHANGES FROM TIMESTAMP '%s' EXCLUSIVE",
                          tableName, tsV1))
                  .orderBy("_commit_version", "id");

          List<Row> rows = changes.collectAsList();
          assertEquals(4, rows.size(), "Expected v2..v5 (four inserts) after EXCLUSIVE start");
          for (int i = 0; i < 4; i++) {
            assertEquals((long) (i + 2), ((Number) rows.get(i).getAs("id")).longValue());
          }
        });
  }

  // -------------------- error paths --------------------

  @Test
  public void testTimestampRangeRejectsEmptyExclusiveRange() throws Exception {
    withHistoryTable(
        "empty_excl",
        (tableName, location) -> {
          // Both bounds at tsV3 with EXCL on both sides:
          //   start adjusts to v4, end adjusts to v2 -> start > end -> DELTA_INVALID_CDC_RANGE.
          String tsV3 = commitTimestamp(location, 3).toString();

          Exception ex =
              assertThrows(
                  Exception.class,
                  () ->
                      spark
                          .sql(
                              String.format(
                                  "SELECT * FROM %s "
                                      + "CHANGES FROM TIMESTAMP '%s' EXCLUSIVE "
                                      + "TO TIMESTAMP '%s' EXCLUSIVE",
                                  tableName, tsV3, tsV3))
                          .collectAsList());
          assertTrue(
              ex.getMessage().contains("DELTA_INVALID_CDC_RANGE")
                  || ex.getMessage().contains("end before start"),
              "Expected empty-range CDC error, got: " + ex.getMessage());
        });
  }

  @Test
  public void testTimestampRangeBeforeEarliestCommitFails() throws Exception {
    withHistoryTable(
        "past_ts",
        (tableName, location) -> {
          Exception ex =
              assertThrows(
                  Exception.class,
                  () ->
                      spark
                          .sql(
                              String.format(
                                  "SELECT * FROM %s "
                                      + "CHANGES FROM TIMESTAMP '1900-01-01 00:00:00' "
                                      + "TO TIMESTAMP '1900-01-02 00:00:00'",
                                  tableName))
                          .collectAsList());
          assertTrue(
              ex.getMessage().contains("DELTA_TIMESTAMP_EARLIER_THAN_COMMIT_RETENTION")
                  || ex.getMessage().contains("earlier than")
                  || ex.getMessage().contains("before the earliest available version"),
              "Expected timestamp-before-earliest error, got: " + ex.getMessage());
        });
  }

  @Test
  public void testTimestampRangeAfterLatestCommitFails() throws Exception {
    String tableName = "dsv2_cdc_catalog_ts_future_" + System.nanoTime();

    withTable(
        new String[] {tableName},
        () -> {
          spark.sql(
              String.format(
                  "CREATE TABLE %s (id BIGINT, name STRING) USING delta TBLPROPERTIES "
                      + "('delta.enableDeletionVectors'='false', 'delta.enableRowTracking'='true')",
                  tableName));
          spark.sql(String.format("INSERT INTO %s VALUES (1, 'Alice')", tableName));

          withSQLConf(
              "spark.databricks.delta.v2.enableMode",
              "STRICT",
              () -> {
                Exception ex =
                    assertThrows(
                        Exception.class,
                        () ->
                            spark
                                .sql(
                                    String.format(
                                        "SELECT * FROM %s CHANGES FROM TIMESTAMP "
                                            + "'9999-01-01 00:00:00' TO TIMESTAMP "
                                            + "'9999-01-02 00:00:00'",
                                        tableName))
                                .collectAsList());
                assertTrue(
                    ex.getMessage().contains("DELTA_TIMESTAMP_GREATER_THAN_COMMIT")
                        || ex.getMessage().contains("after the latest version")
                        || ex.getMessage().contains("after the latest available version"),
                    "Expected timestamp-after-latest error, got: " + ex.getMessage());
              });
        });
  }

  @Test
  public void testUnboundedBatchChangesIsRejectedForNow() throws Exception {
    String tableName = "dsv2_cdc_catalog_unbounded_" + System.nanoTime();

    withTable(
        new String[] {tableName},
        () -> {
          spark.sql(
              String.format("CREATE TABLE %s (id BIGINT, name STRING) USING delta", tableName));
          spark.sql(String.format("INSERT INTO %s VALUES (1, 'Alice')", tableName));

          withSQLConf(
              "spark.databricks.delta.v2.enableMode",
              "STRICT",
              () -> {
                AnalysisException ex =
                    assertThrows(
                        AnalysisException.class,
                        () -> spark.read().changes(tableName).collectAsList());
                assertTrue(
                    ex.getMessage().contains("DELTA_CHANGELOG_UNBOUNDED_RANGE"),
                    "Expected loadChangelog rejection for unbounded batch range, got: "
                        + ex.getMessage());
              });
        });
  }

  // ===========================================================================================
  // Mid-range validation: schema drift and row-tracking toggle
  // ===========================================================================================

  /**
   * A CHANGES read on a table that does not have row tracking enabled at the end version must be
   * rejected with the {@code DELTA_CHANGELOG_REQUIRES_ROW_TRACKING} error class. The check happens
   * eagerly in {@code DeltaChangelogScanBuilder.build} against the end-version snapshot.
   */
  @Test
  public void testChangelogRejectsTableWithoutRowTracking() throws Exception {
    String tableName = "dsv2_cdc_catalog_no_rt_" + System.nanoTime();

    withTable(
        new String[] {tableName},
        () -> {
          // Table created without delta.enableRowTracking.
          spark.sql(
              String.format(
                  "CREATE TABLE %s (id BIGINT, name STRING) USING delta "
                      + "TBLPROPERTIES ('delta.enableDeletionVectors'='false')",
                  tableName));
          spark.sql(String.format("INSERT INTO %s VALUES (1, 'Alice')", tableName));

          withSQLConf(
              "spark.databricks.delta.v2.enableMode",
              "STRICT",
              () -> {
                Exception ex =
                    assertThrows(
                        Exception.class,
                        () ->
                            spark
                                .sql(
                                    String.format(
                                        "SELECT * FROM %s CHANGES FROM VERSION 0 TO " + "VERSION 1",
                                        tableName))
                                .collectAsList());
                assertTrue(
                    ex.getMessage().contains("DELTA_CHANGELOG_REQUIRES_ROW_TRACKING"),
                    "Expected row-tracking required error, got: " + ex.getMessage());
              });
        });
  }

  /**
   * Range ends in an RT-disabled state. The eager end-snapshot check in {@code
   * DeltaChangelogScanBuilder.build} must reject with {@code DELTA_CHANGELOG_REQUIRES_ROW_TRACKING}
   * before the per-commit loop runs.
   */
  @Test
  public void testChangelogRejectsRowTrackingDisabledAtEnd() throws Exception {
    String tableName = "dsv2_cdc_catalog_rt_disabled_end_" + System.nanoTime();

    withTable(
        new String[] {tableName},
        () -> {
          // v0: CREATE with row tracking enabled.
          spark.sql(
              String.format(
                  "CREATE TABLE %s (id BIGINT, name STRING) USING delta "
                      + "TBLPROPERTIES "
                      + "('delta.enableDeletionVectors'='false', "
                      + "'delta.enableRowTracking'='true')",
                  tableName));
          // v1: INSERT (row tracking still on).
          spark.sql(String.format("INSERT INTO %s VALUES (1, 'Alice')", tableName));
          // v2: disable row tracking via ALTER TBLPROPERTIES.
          spark.sql(
              String.format(
                  "ALTER TABLE %s SET TBLPROPERTIES ('delta.enableRowTracking'='false')",
                  tableName));

          withSQLConf(
              "spark.databricks.delta.v2.enableMode",
              "STRICT",
              () -> {
                Exception ex =
                    assertThrows(
                        Exception.class,
                        () ->
                            spark
                                .sql(
                                    String.format(
                                        "SELECT * FROM %s CHANGES FROM VERSION 0 TO " + "VERSION 2",
                                        tableName))
                                .collectAsList());
                assertTrue(
                    ex.getMessage().contains("DELTA_CHANGELOG_REQUIRES_ROW_TRACKING"),
                    "Expected eager end-snapshot RT error, got: " + ex.getMessage());
              });
        });
  }

  /**
   * Range starts and ends with row tracking enabled, but a mid-range commit carries a Metadata
   * action that disables row tracking. The per-commit Metadata loop in {@code
   * DeltaChangelogBatch.planInputPartitions} must reject with {@code
   * DELTA_CHANGELOG_ROW_TRACKING_DISABLED_IN_RANGE}, because the eager boundary checks see only
   * RT-enabled endpoints.
   */
  @Test
  public void testChangelogRejectsRowTrackingDisabledMidRange() throws Exception {
    String tableName = "dsv2_cdc_catalog_rt_disabled_mid_" + System.nanoTime();

    withTable(
        new String[] {tableName},
        () -> {
          // v0: CREATE with row tracking enabled.
          spark.sql(
              String.format(
                  "CREATE TABLE %s (id BIGINT, name STRING) USING delta "
                      + "TBLPROPERTIES "
                      + "('delta.enableDeletionVectors'='false', "
                      + "'delta.enableRowTracking'='true')",
                  tableName));
          // v1: INSERT (RT still on).
          spark.sql(String.format("INSERT INTO %s VALUES (1, 'Alice')", tableName));
          // v2: ALTER TBLPROPERTIES sets RT off (Metadata commit inside the range).
          spark.sql(
              String.format(
                  "ALTER TABLE %s SET TBLPROPERTIES ('delta.enableRowTracking'='false')",
                  tableName));
          // v3: ALTER TBLPROPERTIES turns RT back on, so the end-snapshot check passes
          // and the failure must come from the per-commit loop at v2.
          spark.sql(
              String.format(
                  "ALTER TABLE %s SET TBLPROPERTIES ('delta.enableRowTracking'='true')",
                  tableName));

          withSQLConf(
              "spark.databricks.delta.v2.enableMode",
              "STRICT",
              () -> {
                Exception ex =
                    assertThrows(
                        Exception.class,
                        () ->
                            spark
                                .sql(
                                    String.format(
                                        "SELECT * FROM %s CHANGES FROM VERSION 0 TO " + "VERSION 3",
                                        tableName))
                                .collectAsList());
                assertTrue(
                    ex.getMessage().contains("DELTA_CHANGELOG_ROW_TRACKING_DISABLED_IN_RANGE"),
                    "Expected per-commit mid-range RT error, got: " + ex.getMessage());
              });
        });
  }

  /**
   * Row tracking is toggled off and back on entirely BEFORE the requested range. The read range
   * itself stays row-tracking-enabled, so the out-of-range toggle must not fail the read.
   */
  @Test
  public void testChangelogAllowsRowTrackingToggleBeforeRange() throws Exception {
    String tableName = "dsv2_cdc_catalog_rt_toggle_before_" + System.nanoTime();

    withTable(
        new String[] {tableName},
        () -> {
          // v0: CREATE with row tracking enabled.
          spark.sql(
              String.format(
                  "CREATE TABLE %s (id BIGINT, name STRING) USING delta TBLPROPERTIES "
                      + "('delta.enableDeletionVectors'='false', 'delta.enableRowTracking'='true')",
                  tableName));
          // v1: INSERT (row tracking on).
          spark.sql(String.format("INSERT INTO %s VALUES (1, 'Alice')", tableName));
          // v2: disable row tracking, v3: re-enable it -- both before the read range.
          spark.sql(
              String.format(
                  "ALTER TABLE %s SET TBLPROPERTIES ('delta.enableRowTracking'='false')",
                  tableName));
          spark.sql(
              String.format(
                  "ALTER TABLE %s SET TBLPROPERTIES ('delta.enableRowTracking'='true')", tableName));
          // v4, v5: INSERTs inside the row-tracking-enabled read range.
          spark.sql(String.format("INSERT INTO %s VALUES (2, 'Bob')", tableName));
          spark.sql(String.format("INSERT INTO %s VALUES (3, 'Charlie')", tableName));

          withSQLConf(
              "spark.databricks.delta.v2.enableMode",
              "STRICT",
              () -> {
                List<Row> rows =
                    spark
                        .sql(
                            String.format(
                                "SELECT id, name, _change_type FROM %s "
                                    + "CHANGES FROM VERSION 4 TO VERSION 5",
                                tableName))
                        .orderBy("id")
                        .collectAsList();
                assertEquals(2, rows.size(), "Expected the two in-range inserts to be returned");
                assertEquals(2L, rows.get(0).getLong(0), "Expected id of Bob to be 2");
                assertEquals("Bob", rows.get(0).getString(1), "Expected name to be Bob");
                assertEquals(3L, rows.get(1).getLong(0), "Expected id of Charlie to be 3");
                assertEquals("Charlie", rows.get(1).getString(1), "Expected name to be Charlie");
              });
        });
  }

  /**
   * Row tracking is disabled at a commit AFTER the requested range. The read range stays
   * row-tracking-enabled, so the later toggle must not fail the read.
   */
  @Test
  public void testChangelogAllowsRowTrackingDisabledAfterRange() throws Exception {
    String tableName = "dsv2_cdc_catalog_rt_disabled_after_" + System.nanoTime();

    withTable(
        new String[] {tableName},
        () -> {
          // v0: CREATE with row tracking enabled.
          spark.sql(
              String.format(
                  "CREATE TABLE %s (id BIGINT, name STRING) USING delta TBLPROPERTIES "
                      + "('delta.enableDeletionVectors'='false', 'delta.enableRowTracking'='true')",
                  tableName));
          // v1, v2: INSERTs inside the row-tracking-enabled read range.
          spark.sql(String.format("INSERT INTO %s VALUES (1, 'Alice')", tableName));
          spark.sql(String.format("INSERT INTO %s VALUES (2, 'Bob')", tableName));
          // v3: disable row tracking after the range.
          spark.sql(
              String.format(
                  "ALTER TABLE %s SET TBLPROPERTIES ('delta.enableRowTracking'='false')",
                  tableName));

          withSQLConf(
              "spark.databricks.delta.v2.enableMode",
              "STRICT",
              () -> {
                List<Row> rows =
                    spark
                        .sql(
                            String.format(
                                "SELECT id, name, _change_type FROM %s "
                                    + "CHANGES FROM VERSION 1 TO VERSION 2",
                                tableName))
                        .orderBy("id")
                        .collectAsList();
                assertEquals(2, rows.size(), "Expected the two in-range inserts to be returned");
                assertEquals(1L, rows.get(0).getLong(0), "Expected id of Alice to be 1");
                assertEquals("Alice", rows.get(0).getString(1), "Expected name to be Alice");
                assertEquals(2L, rows.get(1).getLong(0), "Expected id of Bob to be 2");
                assertEquals("Bob", rows.get(1).getString(1), "Expected name to be Bob");
              });
        });
  }

  /**
   * A CHANGES read across an additive mid-range schema change (adding a nullable column) is read
   * compatible and must succeed. Rows from before the change are read with the end schema, leaving
   * the added column null.
   */
  @Test
  public void testChangelogAllowsAdditiveSchemaChangeMidRange() throws Exception {
    String tableName = "dsv2_cdc_catalog_schema_change_" + System.nanoTime();

    withTable(
        new String[] {tableName},
        () -> {
          spark.sql(
              String.format(
                  "CREATE TABLE %s (id BIGINT, name STRING) USING delta TBLPROPERTIES "
                      + "('delta.enableDeletionVectors'='false', 'delta.enableRowTracking'='true')",
                  tableName));
          spark.sql(String.format("INSERT INTO %s VALUES (1, 'Alice')", tableName));
          // Additive schema change mid-range: add a nullable column.
          spark.sql(String.format("ALTER TABLE %s ADD COLUMN extra STRING", tableName));
          spark.sql(String.format("INSERT INTO %s VALUES (2, 'Bob', 'x')", tableName));

          withSQLConf(
              "spark.databricks.delta.v2.enableMode",
              "STRICT",
              () -> {
                List<Row> rows =
                    spark
                        .sql(
                            String.format(
                                "SELECT id, name, extra, _change_type FROM %s "
                                    + "CHANGES FROM VERSION 1 TO VERSION 3",
                                tableName))
                        .orderBy("id")
                        .collectAsList();
                assertEquals(2, rows.size(), "Additive schema change should not fail the read");
                assertEquals(1L, rows.get(0).getLong(0), "Expected id of Alice to be 1");
                assertEquals("Alice", rows.get(0).getString(1), "Expected name to be Alice");
                assertTrue(rows.get(0).isNullAt(2), "extra is null for the pre-change Alice row");
                assertEquals(2L, rows.get(1).getLong(0), "Expected id of Bob to be 2");
                assertEquals("Bob", rows.get(1).getString(1), "Expected name to be Bob");
                assertEquals("x", rows.get(1).getString(2), "Expected Bob's extra value to be x");
              });
        });
  }

  /**
   * A non-read-compatible mid-range schema change (dropping a column) must be rejected with {@code
   * DELTA_CHANGELOG_SCHEMA_CHANGE_IN_RANGE}: the end schema can no longer read the data written
   * before the drop.
   */
  @Test
  public void testChangelogRejectsIncompatibleSchemaChangeMidRange() throws Exception {
    String tableName = "dsv2_cdc_catalog_drop_col_" + System.nanoTime();

    withTable(
        new String[] {tableName},
        () -> {
          // Column mapping is required to drop a column.
          spark.sql(
              String.format(
                  "CREATE TABLE %s (id BIGINT, name STRING, extra STRING) USING delta TBLPROPERTIES "
                      + "('delta.enableDeletionVectors'='false', 'delta.enableRowTracking'='true', "
                      + "'delta.columnMapping.mode'='name')",
                  tableName));
          spark.sql(String.format("INSERT INTO %s VALUES (1, 'Alice', 'x')", tableName));
          // Non-additive schema change mid-range: drop a column.
          spark.sql(String.format("ALTER TABLE %s DROP COLUMN extra", tableName));
          spark.sql(String.format("INSERT INTO %s VALUES (2, 'Bob')", tableName));

          withSQLConf(
              "spark.databricks.delta.v2.enableMode",
              "STRICT",
              () -> {
                Exception ex =
                    assertThrows(
                        Exception.class,
                        () ->
                            spark
                                .sql(
                                    String.format(
                                        "SELECT * FROM %s CHANGES FROM VERSION 1 TO VERSION 3",
                                        tableName))
                                .collectAsList());
                assertTrue(
                    ex.getMessage().contains("DELTA_CHANGELOG_SCHEMA_CHANGE_IN_RANGE"),
                    "Expected schema-change error, got: " + ex.getMessage());
              });
        });
  }

  /**
   * A supported type-widening mid-range change (INT to LONG, with delta.enableTypeWidening) is read
   * compatible: rows written before the change are upcast to the end (wider) type. This exercises
   * the changelog wiring; the widening rules themselves are covered by SchemaUtils.isReadCompatible.
   */
  @Test
  public void testChangelogAllowsTypeWideningMidRange() throws Exception {
    String tableName = "dsv2_cdc_catalog_widen_" + System.nanoTime();

    withTable(
        new String[] {tableName},
        () -> {
          spark.sql(
              String.format(
                  "CREATE TABLE %s (id BIGINT, val INT) USING delta TBLPROPERTIES "
                      + "('delta.enableDeletionVectors'='false', 'delta.enableRowTracking'='true', "
                      + "'delta.enableTypeWidening'='true')",
                  tableName));
          spark.sql(String.format("INSERT INTO %s VALUES (1, 10)", tableName)); // v1: val is INT
          spark.sql(
              String.format("ALTER TABLE %s ALTER COLUMN val TYPE BIGINT", tableName)); // v2: widen
          spark.sql(String.format("INSERT INTO %s VALUES (2, 20)", tableName)); // v3: val is LONG

          withSQLConf(
              "spark.databricks.delta.v2.enableMode",
              "STRICT",
              () -> {
                List<Row> rows =
                    spark
                        .sql(
                            String.format(
                                "SELECT id, val, _change_type FROM %s "
                                    + "CHANGES FROM VERSION 1 TO VERSION 3",
                                tableName))
                        .orderBy("id")
                        .collectAsList();
                assertEquals(2, rows.size(), "Expected both inserts across the widening");
                // v1 was written as INT; it must read back as the widened LONG value.
                assertEquals(10L, rows.get(0).getLong(1), "v1 val should read back as LONG 10");
                assertEquals(20L, rows.get(1).getLong(1), "v3 val should be LONG 20");
              });
        });
  }
}
