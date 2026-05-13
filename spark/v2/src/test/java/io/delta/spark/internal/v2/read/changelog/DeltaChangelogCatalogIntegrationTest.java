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

import io.delta.spark.internal.v2.DeltaV2TestBase;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for catalog-routed CDC entrypoint (TableCatalog.loadChangelog).
 *
 * <p>These tests intentionally exercise SQL/DataFrame paths (not direct DeltaChangelog construction)
 * so they validate analyzer -> catalog -> changelog wiring.
 */
public class DeltaChangelogCatalogIntegrationTest extends DeltaV2TestBase {

  // ===========================================================================================
  // Fixtures and helpers
  // ===========================================================================================

  /**
   * Creates a row-tracking-enabled Delta table with 5 INSERT commits on top of CREATE, runs
   * the given body with the table name, and drops the table + path on completion.
   * <p>
   * Resulting commit history (used by all timestamp-range tests below):
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
    withTable(tablePath, () -> withTable(new String[] {tableName}, () -> {
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
      body.accept(tableName);
    }));
  }

  @FunctionalInterface
  private interface ThrowingConsumer {
    void accept(String tableName) throws Exception;
  }

  /**
   * Returns the commit timestamp of {@code version} as a {@link java.sql.Timestamp}. Uses {@code
   * Row.getTimestamp(idx)} (typed accessor) rather than {@code getAs} to avoid the raw
   * long-microsecond surfacing by the underlying internal row.
   */
  private java.sql.Timestamp commitTimestamp(String tableName, long version) {
    // DESCRIBE HISTORY cannot appear inside a subquery in Spark SQL; materialize it first,
    // then filter via the DataFrame API.
    Dataset<Row> row =
        spark
            .sql(String.format("DESCRIBE HISTORY %s", tableName))
            .filter(String.format("version = %d", version))
            .select("timestamp");
    return row.collectAsList().get(0).getTimestamp(0);
  }

  /**
   * Returns a wall-clock string strictly between two commit timestamps. Used to exercise {@code
   * getActiveCommitAtTime} with inputs that don't coincide with any commit's exact ts, so bounds
   * inclusivity does not change the resolved version.
   */
  private String betweenCommits(String tableName, long earlier, long later) {
    long earlierMs = commitTimestamp(tableName, earlier).getTime();
    long laterMs = commitTimestamp(tableName, later).getTime();
    return new java.sql.Timestamp(earlierMs + (laterMs - earlierMs) / 2).toString();
  }

  // ===========================================================================================
  // SQL / DataFrame parity
  // ===========================================================================================

  @Test
  public void testSqlAndDataFrameChangesMatchForVersionRange() throws Exception {
    String tableName = "dsv2_cdc_catalog_" + System.nanoTime();
    String tablePath = System.getProperty("java.io.tmpdir") + "/" + tableName;

    withTable(tablePath, () -> withTable(new String[] {tableName}, () -> {
      spark.sql(
          String.format(
              "CREATE TABLE %s (id BIGINT, name STRING) USING delta LOCATION '%s' TBLPROPERTIES "
                  + "('delta.enableDeletionVectors'='false', 'delta.enableRowTracking'='true')",
              tableName, tablePath));
      spark.sql(String.format("INSERT INTO %s VALUES (1, 'Alice'), (2, 'Bob')", tableName));
      spark.sql(String.format("DELETE FROM %s WHERE id = 1", tableName));

      Dataset<Row> sqlDf =
          spark.sql(
                  String.format(
                      "SELECT id, name, _change_type, _commit_version "
                          + "FROM %s CHANGES FROM VERSION 1 TO VERSION 2",
                      tableName))
              .orderBy("_commit_version", "id", "_change_type", "name");

      Dataset<Row> apiDf =
          spark.read()
              .option("startingVersion", "1")
              .option("endingVersion", "2")
              .changes(tableName)
              .select("id", "name", "_change_type", "_commit_version")
              .orderBy("_commit_version", "id", "_change_type", "name");

      List<Row> sqlRows = sqlDf.collectAsList();
      List<Row> apiRows = apiDf.collectAsList();
      assertFalse(sqlRows.isEmpty(), "Expected non-empty CDC output for VERSION 1..2 range");
      assertEquals(sqlRows, apiRows, "SQL CHANGES and DataFrameReader.changes should match");

      List<String> fieldNames = Arrays.asList(sqlDf.schema().fieldNames());
      assertTrue(fieldNames.contains("id"));
      assertTrue(fieldNames.contains("name"));
      assertTrue(fieldNames.contains("_change_type"));
      assertTrue(fieldNames.contains("_commit_version"));
    }));
  }

  // ===========================================================================================
  // Bounds inclusivity/exclusivity testing
  // ===========================================================================================

  // -------------------- default INCL/INCL --------------------

  @Test
  public void testTimestampRangeReadsAllChanges() throws Exception {
    withHistoryTable("all", tableName -> {
      String startTs = commitTimestamp(tableName, 0).toString();
      String endTs = commitTimestamp(tableName, 5).toString();

      Dataset<Row> changes =
          spark.sql(
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
    withHistoryTable("partial", tableName -> {
      // Both bounds resolve to v3; range = [v3, v3] inclusive.
      String tsV3 = commitTimestamp(tableName, 3).toString();
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
    withHistoryTable("between", tableName -> {
      // Start strictly between v1 and v2: getActiveCommitAtTime returns the latest commit
      // with ts <= start, so start resolves to v1.
      // End strictly between v2 and v3: same rule, end resolves to v2.
      // Range = [v1, v2] = Alice + Bob.
      String startTs = betweenCommits(tableName, 1, 2);
      String endTs = betweenCommits(tableName, 2, 3);

      Dataset<Row> changes =
          spark.sql(
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
    withHistoryTable("excl", tableName -> {
      String tsV1 = commitTimestamp(tableName, 1).toString();
      String tsV3 = commitTimestamp(tableName, 3).toString();

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
    withHistoryTable("mixed_se_ei", tableName -> {
      String tsV1 = commitTimestamp(tableName, 1).toString();
      String tsV3 = commitTimestamp(tableName, 3).toString();

      // FROM tsV1 EXCLUSIVE bumps start to v2; TO tsV3 (default INCLUSIVE) keeps end at v3.
      // Range = [v2, v3] = Bob + Charlie.
      Dataset<Row> changes =
          spark.sql(
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
    withHistoryTable("mixed_si_ee", tableName -> {
      String tsV1 = commitTimestamp(tableName, 1).toString();
      String tsV3 = commitTimestamp(tableName, 3).toString();

      // FROM tsV1 (default INCLUSIVE) keeps start at v1; TO tsV3 EXCLUSIVE drops end to v2.
      // Range = [v1, v2] = Alice + Bob.
      Dataset<Row> changes =
          spark.sql(
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
    withHistoryTable("open_incl", tableName -> {
      // FROM tsV1 (default INCLUSIVE) keeps start at v1; no TO clause = read to latest (v5).
      // Range = [v1, v5] = all five inserts.
      String tsV1 = commitTimestamp(tableName, 1).toString();

      Dataset<Row> changes =
          spark.sql(
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
    withHistoryTable("open_excl", tableName -> {
      // FROM tsV1 EXCLUSIVE bumps start to v2; no TO clause = read to latest (v5).
      // Range = [v2, v5] = Bob + Charlie + Dave + Eve.
      String tsV1 = commitTimestamp(tableName, 1).toString();

      Dataset<Row> changes =
          spark.sql(
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
    withHistoryTable("empty_excl", tableName -> {
      // Both bounds at tsV3 with EXCL on both sides:
      //   start adjusts to v4, end adjusts to v2 -> start > end -> DELTA_INVALID_CDC_RANGE.
      String tsV3 = commitTimestamp(tableName, 3).toString();

      Exception ex =
          assertThrows(
              Exception.class,
              () ->
                  spark.sql(
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
    withHistoryTable("past_ts", tableName -> {
      Exception ex =
          assertThrows(
              Exception.class,
              () ->
                  spark.sql(
                          String.format(
                              "SELECT * FROM %s "
                                  + "CHANGES FROM TIMESTAMP '1900-01-01 00:00:00' "
                                  + "TO TIMESTAMP '1900-01-02 00:00:00'",
                              tableName))
                      .collectAsList());
      assertTrue(
          ex.getMessage().contains("DELTA_TIMESTAMP_EARLIER_THAN_COMMIT_RETENTION")
              || ex.getMessage().contains("earlier than"),
          "Expected timestamp-before-earliest error, got: " + ex.getMessage());
    });
  }

  @Test
  public void testTimestampRangeAfterLatestCommitFails() throws Exception {
    String tableName = "dsv2_cdc_catalog_ts_future_" + System.nanoTime();
    String tablePath = System.getProperty("java.io.tmpdir") + "/" + tableName;

    withTable(tablePath, () -> withTable(new String[] {tableName}, () -> {
      spark.sql(
          String.format(
              "CREATE TABLE %s (id BIGINT, name STRING) USING delta LOCATION '%s' TBLPROPERTIES "
                  + "('delta.enableDeletionVectors'='false', 'delta.enableRowTracking'='true')",
              tableName, tablePath));
      spark.sql(String.format("INSERT INTO %s VALUES (1, 'Alice')", tableName));

      Exception ex =
          assertThrows(
              Exception.class,
              () ->
                  spark.sql(
                          String.format(
                              "SELECT * FROM %s CHANGES FROM TIMESTAMP '9999-01-01 00:00:00' "
                                  + "TO TIMESTAMP '9999-01-02 00:00:00'",
                              tableName))
                      .collectAsList());
      assertTrue(
          ex.getMessage().contains("DELTA_TIMESTAMP_GREATER_THAN_COMMIT")
              || ex.getMessage().contains("after the latest version"),
          "Expected timestamp-after-latest error, got: " + ex.getMessage());
    }));
  }

  @Test
  public void testUnboundedBatchChangesIsRejectedForNow() throws Exception {
    String tableName = "dsv2_cdc_catalog_unbounded_" + System.nanoTime();
    String tablePath = System.getProperty("java.io.tmpdir") + "/" + tableName;

    withTable(tablePath, () -> withTable(new String[] {tableName}, () -> {
      spark.sql(
          String.format(
              "CREATE TABLE %s (id BIGINT, name STRING) USING delta LOCATION '%s'",
              tableName, tablePath));
      spark.sql(String.format("INSERT INTO %s VALUES (1, 'Alice')", tableName));

      AnalysisException ex =
          assertThrows(
              AnalysisException.class,
              () -> spark.read().changes(tableName).collectAsList());
      assertTrue(
          ex.getMessage().contains("Delta CDC does not support this range"),
          "Expected loadChangelog rejection for unbounded batch range, got: "
              + ex.getMessage());
    }));
  }
}
