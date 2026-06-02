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

package io.sparkuctest;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * CDF (Change Data Feed) test suite for Delta tables through Unity Catalog coordinated commits.
 *
 * <p>Validates that UPDATE, DELETE, and MERGE operations generate proper CDF change data files
 * (update_preimage, update_postimage, delete) when the table uses UC-managed coordinated commits.
 *
 * <p>Reproduces the scenario from LC-14524 where OSS Delta DML through the UC commit coordinator
 * does not generate CDF change data files — only insert records appear.
 */
public class UCDeltaTableCDFTest extends UCDeltaTableIntegrationBaseTest {

  private static final String CDF_TBLPROPS = "'delta.enableChangeDataFeed'='true'";

  /**
   * Helper: query CDF change records for a specific version using table_changes(). Returns rows as
   * [col1, col2, ..., _change_type, _commit_version, _commit_timestamp].
   */
  private List<List<String>> queryCDF(String tableName, long version) {
    return sql(
        "SELECT * FROM table_changes('%s', %d, %d) ORDER BY _change_type, 1",
        tableName, version, version);
  }

  /** Extract the set of distinct _change_type values from CDF results. */
  private Set<String> changeTypes(List<List<String>> cdfRows, int changeTypeColIndex) {
    return cdfRows.stream().map(row -> row.get(changeTypeColIndex)).collect(Collectors.toSet());
  }

  @TestAllTableTypes
  public void testUpdateGeneratesCDF(TableType tableType) throws Exception {
    withNewTable(
        "cdf_update_test",
        "key INT, value INT",
        null,
        tableType,
        CDF_TBLPROPS,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 10), (2, 20), (3, 30)", tableName);
          long preUpdateVersion = currentVersion(tableName);

          sql("UPDATE %s SET value = -1 WHERE key = 1", tableName);
          long updateVersion = currentVersion(tableName);
          assertTrue(updateVersion > preUpdateVersion, "UPDATE should create a new version");

          List<List<String>> cdf = queryCDF(tableName, updateVersion);
          // Schema: key, value, _change_type, _commit_version, _commit_timestamp
          Set<String> types = changeTypes(cdf, 2);

          assertTrue(
              types.contains("update_preimage"),
              "Expected update_preimage in CDF but got: " + types + "\nRows: " + cdf);
          assertTrue(
              types.contains("update_postimage"),
              "Expected update_postimage in CDF but got: " + types + "\nRows: " + cdf);
        });
  }

  @TestAllTableTypes
  public void testDeleteGeneratesCDF(TableType tableType) throws Exception {
    withNewTable(
        "cdf_delete_test",
        "key INT, value INT",
        null,
        tableType,
        CDF_TBLPROPS,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 10), (2, 20), (3, 30)", tableName);
          long preDeleteVersion = currentVersion(tableName);

          sql("DELETE FROM %s WHERE key = 2", tableName);
          long deleteVersion = currentVersion(tableName);
          assertTrue(deleteVersion > preDeleteVersion, "DELETE should create a new version");

          List<List<String>> cdf = queryCDF(tableName, deleteVersion);
          Set<String> types = changeTypes(cdf, 2);

          assertTrue(
              types.contains("delete"),
              "Expected delete in CDF but got: " + types + "\nRows: " + cdf);
        });
  }

  @TestAllTableTypes
  public void testMergeGeneratesCDF(TableType tableType) throws Exception {
    withNewTable(
        "cdf_merge_test",
        "key INT, value INT",
        null,
        tableType,
        CDF_TBLPROPS,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 10), (2, 20), (3, 30)", tableName);
          long preMergeVersion = currentVersion(tableName);

          withNewTable(
              "cdf_merge_source",
              "key INT, value INT",
              tableType,
              tableName2 -> {
                sql("INSERT INTO %s VALUES (1, 100), (4, 40)", tableName2);
                sql(
                    "MERGE INTO %s t USING %s s ON t.key = s.key "
                        + "WHEN MATCHED THEN UPDATE SET t.value = s.value "
                        + "WHEN NOT MATCHED THEN INSERT *",
                    tableName, tableName2);
              });

          long mergeVersion = currentVersion(tableName);
          assertTrue(mergeVersion > preMergeVersion, "MERGE should create a new version");

          List<List<String>> cdf = queryCDF(tableName, mergeVersion);
          Set<String> types = changeTypes(cdf, 2);

          assertTrue(
              types.contains("update_preimage"),
              "Expected update_preimage in CDF from MERGE but got: " + types + "\nRows: " + cdf);
          assertTrue(
              types.contains("update_postimage"),
              "Expected update_postimage in CDF from MERGE but got: " + types + "\nRows: " + cdf);
          assertTrue(
              types.contains("insert"),
              "Expected insert in CDF from MERGE but got: " + types + "\nRows: " + cdf);
        });
  }

  @TestAllTableTypes
  public void testFullDMLLifecycleWithCDF(TableType tableType) throws Exception {
    withNewTable(
        "cdf_lifecycle_test",
        "id INT, val STRING",
        null,
        tableType,
        CDF_TBLPROPS,
        tableName -> {
          // INSERT
          sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')", tableName);
          long insertVersion = currentVersion(tableName);

          // UPDATE
          sql("UPDATE %s SET val = 'updated' WHERE id <= 2", tableName);
          long updateVersion = currentVersion(tableName);

          List<List<String>> updateCDF = queryCDF(tableName, updateVersion);
          Set<String> updateTypes = changeTypes(updateCDF, 2);
          assertTrue(
              updateTypes.contains("update_preimage"),
              "UPDATE should produce update_preimage, got: " + updateTypes);
          assertTrue(
              updateTypes.contains("update_postimage"),
              "UPDATE should produce update_postimage, got: " + updateTypes);

          // DELETE
          sql("DELETE FROM %s WHERE id = 3", tableName);
          long deleteVersion = currentVersion(tableName);

          List<List<String>> deleteCDF = queryCDF(tableName, deleteVersion);
          Set<String> deleteTypes = changeTypes(deleteCDF, 2);
          assertTrue(
              deleteTypes.contains("delete"),
              "DELETE should produce delete CDF record, got: " + deleteTypes);

          // MERGE
          withNewTable(
              "cdf_lifecycle_merge_src",
              "id INT, val STRING",
              tableType,
              srcTable -> {
                sql("INSERT INTO %s VALUES (1, 'merged'), (6, 'new')", srcTable);
                sql(
                    "MERGE INTO %s t USING %s s ON t.id = s.id "
                        + "WHEN MATCHED THEN UPDATE SET t.val = s.val "
                        + "WHEN NOT MATCHED THEN INSERT *",
                    tableName, srcTable);
              });
          long mergeVersion = currentVersion(tableName);

          List<List<String>> mergeCDF = queryCDF(tableName, mergeVersion);
          Set<String> mergeTypes = changeTypes(mergeCDF, 2);
          assertTrue(
              mergeTypes.contains("update_preimage"),
              "MERGE should produce update_preimage, got: " + mergeTypes);
          assertTrue(
              mergeTypes.contains("update_postimage"),
              "MERGE should produce update_postimage, got: " + mergeTypes);
          assertTrue(
              mergeTypes.contains("insert"),
              "MERGE should produce insert for unmatched rows, got: " + mergeTypes);

          // Verify full CDF range covers all operations
          List<List<String>> allCDF =
              sql(
                  "SELECT _change_type, COUNT(*) as cnt FROM table_changes('%s', %d, %d) "
                      + "GROUP BY _change_type ORDER BY _change_type",
                  tableName, updateVersion, mergeVersion);

          Set<String> allTypes = allCDF.stream().map(row -> row.get(0)).collect(Collectors.toSet());

          Set<String> expectedTypes = new HashSet<>();
          expectedTypes.add("update_preimage");
          expectedTypes.add("update_postimage");
          expectedTypes.add("delete");
          expectedTypes.add("insert");
          assertEquals(
              expectedTypes,
              allTypes,
              "Full CDF range should contain all change types. Got: " + allCDF);
        });
  }

  /**
   * Verifies that DELETE produces correct CDF data values (not just inferred change types). When
   * CDF is properly generated via AddCDCFile, the deleted row data is preserved exactly. This test
   * ensures the deleted row's actual column values appear in the CDF output.
   */
  @TestAllTableTypes
  public void testDeleteCDFContainsCorrectDeletedRowData(TableType tableType) throws Exception {
    withNewTable(
        "cdf_dv_delete_test",
        "key INT, value INT",
        null,
        tableType,
        CDF_TBLPROPS,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 10), (2, 20), (3, 30)", tableName);

          sql("DELETE FROM %s WHERE key = 2", tableName);
          long deleteVersion = currentVersion(tableName);

          List<List<String>> cdf = queryCDF(tableName, deleteVersion);
          Set<String> types = changeTypes(cdf, 2);
          assertTrue(
              types.contains("delete"),
              "Expected delete in CDF but got: " + types + "\nRows: " + cdf);

          // Verify the specific deleted row (key=2, value=20) is in the CDF output
          List<List<String>> deleteRows =
              cdf.stream().filter(row -> "delete".equals(row.get(2))).collect(Collectors.toList());
          assertEquals(1, deleteRows.size(), "Expected exactly 1 delete row, got: " + deleteRows);
          assertEquals("2", deleteRows.get(0).get(0), "Deleted row key should be 2");
          assertEquals("20", deleteRows.get(0).get(1), "Deleted row value should be 20");

          // Verify table state after delete
          check(tableName, List.of(List.of("1", "10"), List.of("3", "30")));
        });
  }
}
