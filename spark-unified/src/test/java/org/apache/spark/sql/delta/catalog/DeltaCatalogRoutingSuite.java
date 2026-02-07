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
package org.apache.spark.sql.delta.catalog;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.delta.DeltaSQLCommandJavaTest;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.Utils;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DeltaCatalogRoutingSuite implements DeltaSQLCommandJavaTest {

  private transient SparkSession spark;

  @Before
  public void setUp() {
    spark = buildSparkSession();
    spark.conf().set("spark.databricks.delta.v2.enableMode", "STRICT");
  }

  @After
  public void tearDown() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  // Ensures STRICT mode routes path-based CREATE to Kernel staged table and commits a log.
  @Test
  public void testStageCreateStrictUsesKernelForPathIdentifier() throws Exception {
    File tempDir = Utils.createTempDir(System.getProperty("java.io.tmpdir"), "spark");
    try {
      String path = tempDir.getAbsolutePath();
      Identifier ident = Identifier.of(new String[] {"delta"}, path);
      StructType schema = new StructType().add("id", DataTypes.IntegerType);
      Map<String, String> properties = new HashMap<>();
      properties.put(TableCatalog.PROP_PROVIDER, "delta");
      Transform[] partitions = new Transform[] {Expressions.identity("id")};

      StagedTable staged = getCatalog().stageCreate(ident, schema, partitions, properties);

      assertTrue(staged instanceof KernelStagedDeltaTable);
      staged.commitStagedChanges();
      assertTrue(new File(path, "_delta_log/00000000000000000000.json").exists());
    } finally {
      Utils.deleteRecursively(tempDir);
    }
  }

  // Ensures STRICT mode routes UC-managed CREATE to Kernel staged table when UC markers exist.
  @Test
  public void testStageCreateStrictUsesKernelForUcManaged() throws Exception {
    File tempDir = Utils.createTempDir(System.getProperty("java.io.tmpdir"), "spark");
    try {
      Identifier ident = Identifier.of(new String[] {"default"}, "uc_table");
      StructType schema = new StructType().add("id", DataTypes.IntegerType);
      Map<String, String> properties = new HashMap<>();
      properties.put(TableCatalog.PROP_PROVIDER, "delta");
      properties.put(TableCatalog.PROP_LOCATION, tempDir.getAbsolutePath());
      properties.put("delta.feature.catalogManaged", "supported");
      properties.put(UCCommitCoordinatorClient.UC_TABLE_ID_KEY, UUID.randomUUID().toString());
      Transform[] partitions = new Transform[] {Expressions.identity("id")};

      StagedTable staged = getCatalog().stageCreate(ident, schema, partitions, properties);

      assertTrue(staged instanceof KernelStagedDeltaTable);
    } finally {
      Utils.deleteRecursively(tempDir);
    }
  }

  // Ensures STRICT mode falls back to non-kernel staging for non-delta providers.
  @Test
  public void testStageCreateStrictFallsBackForNonDeltaProvider() {
    Identifier ident = Identifier.of(new String[] {"default"}, "parquet_table");
    StructType schema = new StructType().add("id", DataTypes.IntegerType);
    Map<String, String> properties = new HashMap<>();
    properties.put(TableCatalog.PROP_PROVIDER, "parquet");
    Transform[] partitions = new Transform[] {Expressions.identity("id")};

    StagedTable staged = getCatalog().stageCreate(ident, schema, partitions, properties);

    assertFalse(staged instanceof KernelStagedDeltaTable);
  }

  private DeltaCatalog getCatalog() {
    return (DeltaCatalog) spark.sessionState().catalogManager().v2SessionCatalog();
  }
}
