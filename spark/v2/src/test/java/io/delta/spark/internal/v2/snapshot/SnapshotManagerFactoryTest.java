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
package io.delta.spark.internal.v2.snapshot;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.spark.internal.v2.snapshot.unitycatalog.UCManagedTableSnapshotManager;
import io.delta.spark.internal.v2.utils.CatalogTableTestUtils;
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import scala.Option;

/**
 * Tests for {@link SnapshotManagerFactory} routing logic.
 *
 * <p>Verifies that {@code forCreateTable} and {@code forExistingTable} return the correct {@link
 * DeltaSnapshotManager} implementation based on the presence or absence of Unity Catalog metadata.
 */
public class SnapshotManagerFactoryTest {

  private static SparkSession spark;

  private static final String FEATURE_CATALOG_MANAGED =
      TableFeatures.SET_TABLE_FEATURE_SUPPORTED_PREFIX
          + TableFeatures.CATALOG_MANAGED_RW_FEATURE.featureName();
  private static final String FEATURE_SUPPORTED = TableFeatures.SET_TABLE_FEATURE_SUPPORTED_VALUE;
  private static final String UC_TABLE_ID_KEY = UCCommitCoordinatorClient.UC_TABLE_ID_KEY;
  private static final String UC_CATALOG_CONNECTOR = "io.unitycatalog.spark.UCSingleCatalog";

  @BeforeAll
  public static void setUp() {
    spark =
        SparkSession.builder()
            .master("local[*]")
            .appName("SnapshotManagerFactoryTest")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtensionV1")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalogV1")
            .getOrCreate();
  }

  @AfterAll
  public static void tearDown() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  private Engine kernelEngine() {
    return DefaultEngine.create(spark.sessionState().newHadoopConf());
  }

  /**
   * Sets Spark configs for a UC catalog, runs the test, then restores the original config values.
   */
  private void withUCCatalogConfig(
      String catalogName, String ucUri, String ucToken, Runnable test) {
    String[][] configs = {
      {"spark.sql.catalog." + catalogName, UC_CATALOG_CONNECTOR},
      {"spark.sql.catalog." + catalogName + ".uri", ucUri},
      {"spark.sql.catalog." + catalogName + ".token", ucToken}
    };
    String[] originals = new String[configs.length];
    for (int i = 0; i < configs.length; i++) {
      try {
        originals[i] = spark.conf().get(configs[i][0]);
      } catch (Exception e) {
        originals[i] = null;
      }
      spark.conf().set(configs[i][0], configs[i][1]);
    }
    try {
      test.run();
    } finally {
      for (int i = 0; i < configs.length; i++) {
        if (originals[i] != null) {
          spark.conf().set(configs[i][0], originals[i]);
        } else {
          spark.conf().unset(configs[i][0]);
        }
      }
    }
  }

  // ==================== forCreateTable ====================

  @Test
  public void testForCreateTable_nonUCProperties_returnsPathBased() {
    Map<String, String> props = new HashMap<>();
    DeltaSnapshotManager manager =
        SnapshotManagerFactory.forCreateTable(
            "/some/path", kernelEngine(), props, "any_catalog", spark);
    assertInstanceOf(PathBasedSnapshotManager.class, manager);
  }

  @Test
  public void testForCreateTable_ucProperties_returnsUCManaged() {
    String catalogName = "uc_catalog_factory_create";
    String ucUri = "https://uc-factory-create.example.com/api/2.1/unity-catalog";
    String ucToken = "dapi_factory_create_token_7kP3";

    Map<String, String> props = new HashMap<>();
    props.put(FEATURE_CATALOG_MANAGED, FEATURE_SUPPORTED);
    props.put(UC_TABLE_ID_KEY, "factory_create_table_id_9x2b");

    withUCCatalogConfig(
        catalogName,
        ucUri,
        ucToken,
        () -> {
          DeltaSnapshotManager manager =
              SnapshotManagerFactory.forCreateTable(
                  "/some/path", kernelEngine(), props, catalogName, spark);
          assertInstanceOf(UCManagedTableSnapshotManager.class, manager);
        });
  }

  // ==================== forExistingTable ====================

  @Test
  public void testForExistingTable_noCatalogTable_returnsPathBased() {
    DeltaSnapshotManager manager =
        SnapshotManagerFactory.forExistingTable(
            "/warehouse/db/customers_v3", kernelEngine(), Optional.empty());
    assertInstanceOf(PathBasedSnapshotManager.class, manager);
  }

  @Test
  public void testForExistingTable_nonUCCatalogTable_returnsPathBased() {
    CatalogTable catalogTable =
        CatalogTableTestUtils.createCatalogTable(
            "plain_hive_table",
            Option.empty(),
            new HashMap<>(),
            new HashMap<>(),
            Option.apply(URI.create("hdfs://namenode/warehouse/db/plain_hive_table")),
            false,
            false);

    DeltaSnapshotManager manager =
        SnapshotManagerFactory.forExistingTable(
            "hdfs://namenode/warehouse/db/plain_hive_table",
            kernelEngine(),
            Optional.of(catalogTable));
    assertInstanceOf(PathBasedSnapshotManager.class, manager);
  }

  @Test
  public void testForExistingTable_ucFlaggedCatalogTable_returnsUCManaged() {
    String catalogName = "uc_catalog_factory_existing";
    String ucUri = "https://uc-factory-existing.example.com/api/2.1/unity-catalog";
    String ucToken = "dapi_factory_existing_token_4mR9";
    String tablePath = "s3://delta-warehouse/db/uc_managed_orders";
    String ucTableId = "uc_table_id_existing_8f3b";

    Map<String, String> storageProps = new HashMap<>();
    storageProps.put(FEATURE_CATALOG_MANAGED, FEATURE_SUPPORTED);
    storageProps.put(UC_TABLE_ID_KEY, ucTableId);

    CatalogTable catalogTable =
        CatalogTableTestUtils.createCatalogTable(
            "uc_managed_orders",
            Option.apply(catalogName),
            new HashMap<>(),
            storageProps,
            Option.apply(URI.create(tablePath)),
            false,
            false);

    withUCCatalogConfig(
        catalogName,
        ucUri,
        ucToken,
        () -> {
          DeltaSnapshotManager manager =
              SnapshotManagerFactory.forExistingTable(
                  tablePath, kernelEngine(), Optional.of(catalogTable));
          assertInstanceOf(UCManagedTableSnapshotManager.class, manager);
        });
  }
}
