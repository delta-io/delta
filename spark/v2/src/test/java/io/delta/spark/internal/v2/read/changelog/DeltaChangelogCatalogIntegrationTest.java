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
    String tablePath = System.getProperty("java.io.tmpdir") + "/" + tableName;
    withTable(
        tablePath,
        () ->
            withTable(
                new String[] {tableName},
                () -> {
                  spark.sql(
                      String.format(
                          "CREATE TABLE %s (id BIGINT, name STRING) USING delta LOCATION '%s' TBLPROPERTIES "
                              + "('delta.enableDeletionVectors'='false', 'delta.enableRowTracking'='true')",
                          tableName, tablePath));
                  spark.sql(String.format("INSERT INTO %s VALUES (1, 'Alice')", tableName));
                  spark.sql(String.format("INSERT INTO %s VALUES (2, 'Bob')", tableName));
                  spark.sql(String.format("INSERT INTO %s VALUES (3, 'Charlie')", tableName));
                  spark.sql(String.format("INSERT INTO %s VALUES (4, 'Dave')", tableName));
                  spark.sql(String.format("INSERT INTO %s VALUES (5, 'Eve')", tableName));
                  // Auto-CDF requires the V2 connector at read time. Writes above run in the
                  // session-default mode (AUTO → V1 connector for INSERT). The CHANGES read in
                  // the test body needs STRICT to ensure loadTable returns a V2 SparkTable.
                  withSQLConf(
                      "spark.databricks.delta.v2.enableMode",
                      "STRICT",
                      () -> body.accept(tableName, tablePath));
                }));
  }

  @FunctionalInterface
  private interface ThrowingConsumer {
    void accept(String tableName, String tablePath) throws Exception;
  }

  /**
   * Returns the commit timestamp of {@code version}. Resolves the snapshot directly through the
   * kernel snapshot manager, so it works irrespective of the catalog mode flipped on by the test
   * body (which keeps the catalog in STRICT for the Auto-CDF read path).
   */
  private java.sql.Timestamp commitTimestamp(String tablePath, long version) {
    DeltaSnapshotManager snapshotManager =
        SnapshotManagerFactory.create(tablePath, defaultEngine, Optional.empty());
    long millis = snapshotManager.loadSnapshotAt(version).getTimestamp(defaultEngine);
    return new java.sql.Timestamp(millis);
  }

  /**
   * Returns a wall-clock string strictly between two commit timestamps. Used to exercise {@code
   * getActiveCommitAtTime} with inputs that don't coincide with any commit's exact ts, so bounds
   * inclusivity does not change the resolved version.
   */
  private String betweenCommits(String tablePath, long earlier, long later) {
    long earlierMs = commitTimestamp(tablePath, earlier).getTime();
    long laterMs = commitTimestamp(tablePath, later).getTime();
    return new java.sql.Timestamp(earlierMs + (laterMs - earlierMs) / 2).toString();
  }

  // ===========================================================================================
  // SQL / DataFrame parity
  // ===========================================================================================

  @Test
  public void testSqlAndDataFrameChangesMatchForVersionRange() throws Exception {
    String tableName = "dsv2_cdc_catalog_" + System.nanoTime();
    String tablePath = System.getProperty("java.io.tmpdir") + "/" + tableName;

    withTable(
        tablePath,
        () ->
            withTable(
                new String[] {tableName},
                () -> {
                  spark.sql(
                      String.format(
                          "CREATE TABLE %s (id BIGINT, name STRING) USING delta LOCATION '%s' TBLPROPERTIES "
                              + "('delta.enableDeletionVectors'='false', 'delta.enableRowTracking'='true')",
                          tableName, tablePath));
                  spark.sql(
                      String.format("INSERT INTO %s VALUES (1, 'Alice'), (2, 'Bob')", tableName));
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
                            sqlRows.isEmpty(),
                            "Expected non-empty CDC output for VERSION 1..2 range");
                        assertEquals(
                            sqlRows,
                            apiRows,
                            "SQL CHANGES and DataFrameReader.changes should match");

                        List<String> fieldNames = Arrays.asList(sqlDf.schema().fieldNames());
                        assertTrue(fieldNames.contains("id"));
                        assertTrue(fieldNames.contains("name"));
                        assertTrue(fieldNames.contains("_change_type"));
                        assertTrue(fieldNames.contains("_commit_version"));
                      });
                }));
  }

  // ===========================================================================================
  // Bounds inclusivity/exclusivity testing
  // ===========================================================================================

  // -------------------- default INCL/INCL --------------------

  @Test
  public void testTimestampRangeReadsAllChanges() throws Exception {
    withHistoryTable(
        "all",
        (tableName, tablePath) -> {
          String startTs = commitTimestamp(tablePath, 0).toString();
          String endTs = commitTimestamp(tablePath, 5).toString();

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
        (tableName, tablePath) -> {
          // Both bounds resolve to v3; range = [v3, v3] inclusive.
          String tsV3 = commitTimestamp(tablePath, 3).toString();
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
        (tableName, tablePath) -> {
          // Start strictly between v1 and v2: getActiveCommitAtTime returns the latest commit
          // with ts <= start, so start resolves to v1.
          // End strictly between v2 and v3: same rule, end resolves to v2.
          // Range = [v1, v2] = Alice + Bob.
          String startTs = betweenCommits(tablePath, 1, 2);
          String endTs = betweenCommits(tablePath, 2, 3);

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
        (tableName, tablePath) -> {
          String tsV1 = commitTimestamp(tablePath, 1).toString();
          String tsV3 = commitTimestamp(tablePath, 3).toString();

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
        (tableName, tablePath) -> {
          String tsV1 = commitTimestamp(tablePath, 1).toString();
          String tsV3 = commitTimestamp(tablePath, 3).toString();

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
        (tableName, tablePath) -> {
          String tsV1 = commitTimestamp(tablePath, 1).toString();
          String tsV3 = commitTimestamp(tablePath, 3).toString();

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
        (tableName, tablePath) -> {
          // FROM tsV1 (default INCLUSIVE) keeps start at v1; no TO clause = read to latest (v5).
          // Range = [v1, v5] = all five inserts.
          String tsV1 = commitTimestamp(tablePath, 1).toString();

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
        (tableName, tablePath) -> {
          // FROM tsV1 EXCLUSIVE bumps start to v2; no TO clause = read to latest (v5).
          // Range = [v2, v5] = Bob + Charlie + Dave + Eve.
          String tsV1 = commitTimestamp(tablePath, 1).toString();

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
        (tableName, tablePath) -> {
          // Both bounds at tsV3 with EXCL on both sides:
          //   start adjusts to v4, end adjusts to v2 -> start > end -> DELTA_INVALID_CDC_RANGE.
          String tsV3 = commitTimestamp(tablePath, 3).toString();

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
        (tableName, tablePath) -> {
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
    String tablePath = System.getProperty("java.io.tmpdir") + "/" + tableName;

    withTable(
        tablePath,
        () ->
            withTable(
                new String[] {tableName},
                () -> {
                  spark.sql(
                      String.format(
                          "CREATE TABLE %s (id BIGINT, name STRING) USING delta LOCATION '%s' TBLPROPERTIES "
                              + "('delta.enableDeletionVectors'='false', 'delta.enableRowTracking'='true')",
                          tableName, tablePath));
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
                }));
  }

  @Test
  public void testUnboundedBatchChangesIsRejectedForNow() throws Exception {
    String tableName = "dsv2_cdc_catalog_unbounded_" + System.nanoTime();
    String tablePath = System.getProperty("java.io.tmpdir") + "/" + tableName;

    withTable(
        tablePath,
        () ->
            withTable(
                new String[] {tableName},
                () -> {
                  spark.sql(
                      String.format(
                          "CREATE TABLE %s (id BIGINT, name STRING) USING delta LOCATION '%s'",
                          tableName, tablePath));
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
                }));
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
    String tablePath = System.getProperty("java.io.tmpdir") + "/" + tableName;

    withTable(
        tablePath,
        () ->
            withTable(
                new String[] {tableName},
                () -> {
                  // Table created without delta.enableRowTracking.
                  spark.sql(
                      String.format(
                          "CREATE TABLE %s (id BIGINT, name STRING) USING delta LOCATION '%s' "
                              + "TBLPROPERTIES ('delta.enableDeletionVectors'='false')",
                          tableName, tablePath));
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
                                                "SELECT * FROM %s CHANGES FROM VERSION 0 TO "
                                                    + "VERSION 1",
                                                tableName))
                                        .collectAsList());
                        assertTrue(
                            ex.getMessage().contains("DELTA_CHANGELOG_REQUIRES_ROW_TRACKING"),
                            "Expected row-tracking required error, got: " + ex.getMessage());
                      });
                }));
  }

  /**
   * A CHANGES read across a range where row tracking is toggled off mid-range (via a setting of
   * {@code delta.enableRowTracking=false}) must be rejected with {@code
   * DELTA_CHANGELOG_ROW_TRACKING_DISABLED_IN_RANGE}.
   */
  @Test
  public void testChangelogRejectsRowTrackingToggleMidRange() throws Exception {
    String tableName = "dsv2_cdc_catalog_rt_toggle_" + System.nanoTime();
    String tablePath = System.getProperty("java.io.tmpdir") + "/" + tableName;

    withTable(
        tablePath,
        () ->
            withTable(
                new String[] {tableName},
                () -> {
                  // v0: CREATE with row tracking enabled.
                  spark.sql(
                      String.format(
                          "CREATE TABLE %s (id BIGINT, name STRING) USING delta LOCATION '%s' "
                              + "TBLPROPERTIES "
                              + "('delta.enableDeletionVectors'='false', "
                              + "'delta.enableRowTracking'='true')",
                          tableName, tablePath));
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
                                                "SELECT * FROM %s CHANGES FROM VERSION 0 TO "
                                                    + "VERSION 2",
                                                tableName))
                                        .collectAsList());
                        // Either the eager end-snapshot check (RT off at end) or the per-commit
                        // mid-range check fires; both errors are user-actionable.
                        assertTrue(
                            ex.getMessage()
                                    .contains("DELTA_CHANGELOG_ROW_TRACKING_DISABLED_IN_RANGE")
                                || ex.getMessage()
                                    .contains("DELTA_CHANGELOG_REQUIRES_ROW_TRACKING"),
                            "Expected row-tracking-toggle error, got: " + ex.getMessage());
                      });
                }));
  }

  /**
   * A CHANGES read across a range where the table schema evolves mid-range must be rejected with
   * {@code DELTA_CHANGELOG_SCHEMA_CHANGE_IN_RANGE}.
   */
  @Test
  public void testChangelogRejectsSchemaChangeMidRange() throws Exception {
    String tableName = "dsv2_cdc_catalog_schema_change_" + System.nanoTime();
    String tablePath = System.getProperty("java.io.tmpdir") + "/" + tableName;

    withTable(
        tablePath,
        () ->
            withTable(
                new String[] {tableName},
                () -> {
                  spark.sql(
                      String.format(
                          "CREATE TABLE %s (id BIGINT, name STRING) USING delta LOCATION '%s' "
                              + "TBLPROPERTIES "
                              + "('delta.enableDeletionVectors'='false', "
                              + "'delta.enableRowTracking'='true')",
                          tableName, tablePath));
                  spark.sql(String.format("INSERT INTO %s VALUES (1, 'Alice')", tableName));
                  // Schema change mid-range: add a column.
                  spark.sql(String.format("ALTER TABLE %s ADD COLUMN extra STRING", tableName));
                  spark.sql(String.format("INSERT INTO %s VALUES (2, 'Bob', 'x')", tableName));

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
                                                "SELECT * FROM %s CHANGES FROM VERSION 1 TO "
                                                    + "VERSION 3",
                                                tableName))
                                        .collectAsList());
                        assertTrue(
                            ex.getMessage().contains("DELTA_CHANGELOG_SCHEMA_CHANGE_IN_RANGE"),
                            "Expected schema-change error, got: " + ex.getMessage());
                      });
                }));
  }
}
