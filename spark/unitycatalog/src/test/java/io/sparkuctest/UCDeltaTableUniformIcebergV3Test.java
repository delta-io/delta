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

import java.util.List;
import java.util.Map;
import org.apache.spark.sql.delta.Snapshot;
import org.apache.spark.sql.delta.actions.AddFile;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/** Integration tests for Iceberg V3 metadata and row lineage on UC managed Delta tables. */
public class UCDeltaTableUniformIcebergV3Test extends UCDeltaTableUniformIcebergTestBase {

  @Override
  protected int icebergCompatVersion() {
    return 3;
  }

  @Test
  public void uniformIcebergV3CTASRowLineage() throws Exception {
    Assumptions.assumeTrue(
        Boolean.getBoolean("supportIceberg"),
        "Skipping: Iceberg support not available for this Spark version");
    String fullTableName = fullTableName("uniform_iceberg_v3_ctas");
    sql("DROP TABLE IF EXISTS %s", fullTableName);
    try {
      sql(
          "CREATE TABLE %s USING DELTA"
              + " TBLPROPERTIES ('delta.feature.catalogManaged'='supported', %s)"
              + " AS SELECT 1 AS id, 'a' AS data",
          fullTableName, uniformTableProperties());

      check(fullTableName, List.of(row("1", "a")));
      Assertions.assertEquals(
          1L,
          currentVersion(fullTableName),
          "Iceberg V3 CTAS should commit CREATE at v0 and data at v1");
      IcebergMeta meta = verifyNonIncrementalUniForm(fullTableName, 1L, /* expectSnapshot= */ true);
      assertIcebergDataFileCount(meta.table, 1L);
      assertIcebergRowTrackingMatchesDelta(fullTableName, meta.table);
    } finally {
      sql("DROP TABLE IF EXISTS %s", fullTableName);
    }
  }

  @Test
  public void uniformIcebergV3UpdateRowLineage() throws Exception {
    runRowLineageDmlTest(
        "uniform_iceberg_v3_update",
        "UPDATE",
        fullTableName -> {
          sql("UPDATE %s SET id = 1000 WHERE id = 1", fullTableName);
          check(
              sql("SELECT COUNT(*), MIN(id), MAX(id) FROM %s", fullTableName),
              List.of(row("100", "0", "1000")));
        });
  }

  @Test
  public void uniformIcebergV3DeleteRowLineage() throws Exception {
    runRowLineageDmlTest(
        "uniform_iceberg_v3_delete",
        "DELETE",
        fullTableName -> {
          sql("DELETE FROM %s WHERE id = 1", fullTableName);
          check(
              sql("SELECT COUNT(*), MIN(id), MAX(id) FROM %s", fullTableName),
              List.of(row("99", "0", "99")));
        });
  }

  @Test
  public void uniformIcebergV3MergeRowLineage() throws Exception {
    runRowLineageDmlTest(
        "uniform_iceberg_v3_merge",
        "MERGE",
        fullTableName -> {
          sql(
              "MERGE INTO %s AS target "
                  + "USING (SELECT 1 AS id, 1000 AS new_id "
                  + "UNION ALL SELECT 100 AS id, 100 AS new_id) AS source "
                  + "ON target.id = source.id "
                  + "WHEN MATCHED THEN UPDATE SET id = source.new_id "
                  + "WHEN NOT MATCHED THEN INSERT (id) VALUES (source.new_id)",
              fullTableName);
          check(sql("SELECT COUNT(*) FROM %s", fullTableName), List.of(row("101")));
          check(
              sql("SELECT id FROM %s WHERE id IN (1, 100, 1000) ORDER BY id", fullTableName),
              List.of(row("100"), row("1000")));
        });
  }

  private void runRowLineageDmlTest(String tableName, String operationName, TestCode operation)
      throws Exception {
    Assumptions.assumeTrue(
        Boolean.getBoolean("supportIceberg"),
        "Skipping: Iceberg support not available for this Spark version");
    withNewTable(
        tableName,
        "id INT",
        null,
        TableType.MANAGED,
        uniformTableProperties(),
        fullTableName -> {
          long createVersion = currentVersion(fullTableName);
          IcebergMeta createMeta =
              verifyNonIncrementalUniForm(
                  fullTableName, createVersion, /* expectSnapshot= */ false);
          assertNoUniformPropsOnServer(fullTableName);
          assertV3TableProperties(fullTableName);

          // Verify the insert layout before relying on the DML being a partial-file operation. A
          // fully replaced one-row file is removed instead of receiving a DV.
          sql("INSERT INTO %s SELECT CAST(id AS INT) FROM range(0, 100, 1, 1)", fullTableName);
          long insertVersion = currentVersion(fullTableName);
          List<AddFile> insertFiles = loadDeltaSnapshot(fullTableName).allFiles().collectAsList();
          Assertions.assertEquals(
              1, insertFiles.size(), "Single-partition INSERT should produce one Delta data file");
          IcebergMeta insertMeta =
              verifyIncrementalUniForm(
                  fullTableName, insertVersion, createVersion, createMeta.currentSnapshotId);

          operation.run(fullTableName);
          long operationVersion = currentVersion(fullTableName);
          assertNoUniformPropsOnServer(fullTableName);
          IcebergMeta operationMeta =
              verifyIncrementalUniForm(
                  fullTableName, operationVersion, insertVersion, insertMeta.currentSnapshotId);

          Snapshot deltaSnapshot = loadDeltaSnapshot(fullTableName);
          List<AddFile> activeDeltaFiles = deltaSnapshot.allFiles().collectAsList();
          Assertions.assertFalse(activeDeltaFiles.isEmpty(), "Expected active Delta data files");
          Assertions.assertTrue(
              activeDeltaFiles.stream().anyMatch(file -> file.deletionVector() != null),
              operationName + " should produce an active Delta file with a deletion vector");

          Assertions.assertNotNull(
              operationMeta.table.currentSnapshot(),
              "Expected an Iceberg snapshot after " + operationName);
          Assertions.assertFalse(
              operationMeta
                  .table
                  .currentSnapshot()
                  .deleteManifests(operationMeta.table.io())
                  .isEmpty(),
              operationName + " deletion vector should produce Iceberg delete metadata");
          assertIcebergRowTrackingMatchesDelta(fullTableName, operationMeta.table);
        });
  }

  private void assertV3TableProperties(String fullTableName) throws Exception {
    Map<String, String> properties = deltaTableProperties(fullTableName);
    Assertions.assertEquals("true", properties.get("delta.enableIcebergCompatV3"));
    Assertions.assertEquals(
        "true",
        properties.get("delta.enableDeletionVectors"),
        "UC should require deletion vectors for a managed V3 table");
    Assertions.assertEquals(
        "true",
        properties.get("delta.enableRowTracking"),
        "IcebergCompatV3 should enable row tracking");

    Snapshot snapshot = loadDeltaSnapshot(fullTableName);
    Assertions.assertTrue(
        snapshot.protocol().getReaderFeatures().contains("deletionVectors"),
        "V3 protocol should support deletion vectors");
    Assertions.assertTrue(
        snapshot.protocol().getWriterFeatures().contains("rowTracking"),
        "V3 protocol should support row tracking");
    Assertions.assertTrue(
        snapshot.protocol().getWriterFeatures().contains("icebergCompatV3"),
        "V3 protocol should support IcebergCompatV3");
  }
}
