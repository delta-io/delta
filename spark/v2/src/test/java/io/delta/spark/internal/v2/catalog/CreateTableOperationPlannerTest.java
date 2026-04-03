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
package io.delta.spark.internal.v2.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.spark.internal.v2.DeltaV2TestBase;
import io.delta.spark.internal.v2.snapshot.PathBasedSnapshotManager;
import io.delta.spark.internal.v2.snapshot.unitycatalog.UCManagedTableSnapshotManager;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class CreateTableOperationPlannerTest extends DeltaV2TestBase {
  private static final String TEST_CATALOG = "planner_uc_catalog";
  private static final String TEST_UC_URI = "https://planner.example.com/api/2.1/unity-catalog";
  private static final String TEST_UC_TOKEN = "dapi_planner_uc_token";

  private final CreateTableOperationPlanner planner = new CreateTableOperationPlanner();

  @Test
  public void testPlanCreateTable_usesPathIdentifierLocationAndFiltersDsv2Props(
      @TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath();
    Identifier ident = Identifier.of(new String[0], tablePath);
    StructType schema = new StructType().add("id", DataTypes.IntegerType);
    Map<String, String> properties = new HashMap<>();
    properties.put("provider", "delta");
    properties.put("comment", "ignored");
    properties.put("delta.appendOnly", "true");
    properties.put("fs.s3a.access.key", "secret");
    properties.put("option.fs.s3a.secret.key", "also-secret");

    PreparedCreateTableOperation operation =
        planner.planCreateTable(
            ident, schema, new Transform[0], properties, spark, TEST_CATALOG, true);

    assertEquals(tablePath, operation.getLocation());
    assertInstanceOf(PathBasedSnapshotManager.class, operation.getSnapshotManager());
    assertEquals("true", operation.getTableProperties().get("delta.appendOnly"));
    assertFalse(operation.getTableProperties().containsKey("provider"));
    assertFalse(operation.getTableProperties().containsKey("comment"));
    assertFalse(operation.getTableProperties().containsKey("fs.s3a.access.key"));
    assertFalse(operation.getDataLayoutSpec().isPresent());
  }

  @Test
  public void testPlanCreateTable_buildsUCManagedOperationAndSeedsDefaults(@TempDir File tempDir) {
    String previousCatalogImpl = spark.conf().get("spark.sql.catalog." + TEST_CATALOG, null);
    String previousUri = spark.conf().get("spark.sql.catalog." + TEST_CATALOG + ".uri", null);
    String previousToken = spark.conf().get("spark.sql.catalog." + TEST_CATALOG + ".token", null);
    try {
      spark
          .conf()
          .set("spark.sql.catalog." + TEST_CATALOG, "io.unitycatalog.spark.UCSingleCatalog");
      spark.conf().set("spark.sql.catalog." + TEST_CATALOG + ".uri", TEST_UC_URI);
      spark.conf().set("spark.sql.catalog." + TEST_CATALOG + ".token", TEST_UC_TOKEN);

      Map<String, String> properties = new HashMap<>();
      properties.put("location", tempDir.getAbsolutePath());
      properties.put("delta.feature.catalogManaged", "supported");
      properties.put("io.unitycatalog.tableId", "planner-table-id");

      PreparedCreateTableOperation operation =
          planner.planCreateTable(
              Identifier.of(new String[] {"demo"}, "tbl"),
              new StructType().add("id", DataTypes.IntegerType),
              new Transform[0],
              properties,
              spark,
              TEST_CATALOG,
              false);

      assertInstanceOf(UCManagedTableSnapshotManager.class, operation.getSnapshotManager());
      assertEquals("supported", operation.getTableProperties().get("delta.feature.catalogManaged"));
      assertEquals(
          "planner-table-id", operation.getTableProperties().get("io.unitycatalog.tableId"));
      assertEquals("true", operation.getTableProperties().get("delta.enableDeletionVectors"));
      assertEquals("v2", operation.getTableProperties().get("delta.checkpointPolicy"));
      assertEquals("true", operation.getTableProperties().get("delta.enableRowTracking"));
    } finally {
      restoreConf("spark.sql.catalog." + TEST_CATALOG, previousCatalogImpl);
      restoreConf("spark.sql.catalog." + TEST_CATALOG + ".uri", previousUri);
      restoreConf("spark.sql.catalog." + TEST_CATALOG + ".token", previousToken);
    }
  }

  @Test
  public void testCanRepresentWithKernel_rejectsNonIdentityPartitioning() {
    Transform[] unsupported =
        new Transform[] {
          new Transform() {
            @Override
            public String name() {
              return "bucket";
            }

            @Override
            public org.apache.spark.sql.connector.expressions.NamedReference[] references() {
              return new org.apache.spark.sql.connector.expressions.NamedReference[0];
            }

            @Override
            public org.apache.spark.sql.connector.expressions.Expression[] arguments() {
              return new org.apache.spark.sql.connector.expressions.Expression[0];
            }
          }
        };

    assertTrue(CreateTableOperationPlanner.canRepresentWithKernel(new Transform[0]));
    assertFalse(CreateTableOperationPlanner.canRepresentWithKernel(unsupported));
  }

  private static void restoreConf(String key, String value) {
    if (value == null) {
      spark.conf().unset(key);
    } else {
      spark.conf().set(key, value);
    }
  }
}
