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

import org.apache.spark.sql.delta.shims.VariantTypeShims;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * Tests that variant table feature interactions are blocked in Unity Catalog on Spark 4.0.
 *
 * <p>The DeltaCatalog sets {@code variant.disableVariantTableFeatureForSpark40} to true when
 * running under Unity Catalog. On Spark 4.0 (which lacks parquet variant logical type annotation
 * support), this causes the variant table features to be treated as unsupported, blocking all
 * interactions with variant tables including table creation.
 */
public class UCDeltaTableVariantTest extends UCDeltaTableIntegrationBaseTest {

  @TestAllTableTypes
  public void testVariantTableCreationBlockedOnSpark40(TableType tableType) throws Exception {
    Assumptions.assumeFalse(
        VariantTypeShims.SUPPORTS_VARIANT_LOGICAL_TYPE_ANNOTATION(),
        "This test only applies to Spark 4.0");
    // Table creation itself should fail on Spark 4.0 because the variant table feature
    // is treated as unsupported.
    String fullTableName = fullTableName("variant_create_blocked_test");
    assertThrowsWithCauseContaining(
        "variantType",
        () -> {
          if (tableType == TableType.EXTERNAL) {
            withTempDir(
                dir -> {
                  sql(
                      "CREATE TABLE %s (id INT, v VARIANT) USING DELTA LOCATION '%s'",
                      fullTableName, dir.toString() + "/variant_create_blocked_test");
                });
          } else {
            sql(
                "CREATE TABLE %s (id INT, v VARIANT) USING DELTA "
                    + "TBLPROPERTIES ('delta.feature.catalogManaged'='supported')",
                fullTableName);
          }
        });
  }

  @TestAllTableTypes
  public void testVariantTableAllowedOnSpark41(TableType tableType) throws Exception {
    Assumptions.assumeTrue(
        VariantTypeShims.SUPPORTS_VARIANT_LOGICAL_TYPE_ANNOTATION(),
        "This test only applies to Spark 4.1+");
    withNewTable(
        "variant_allowed_test",
        "id INT, v VARIANT",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, parse_json('42'))", tableName);
          check(tableName, java.util.List.of(java.util.List.of("1", "42")));
        });
  }
}
