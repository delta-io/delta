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
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Tests for {@link SnapshotManagerFactory} routing logic. */
public class SnapshotManagerFactoryTest {

  private static SparkSession spark;

  private static final String FEATURE_CATALOG_MANAGED =
      TableFeatures.SET_TABLE_FEATURE_SUPPORTED_PREFIX
          + TableFeatures.CATALOG_MANAGED_RW_FEATURE.featureName();
  private static final String FEATURE_SUPPORTED = TableFeatures.SET_TABLE_FEATURE_SUPPORTED_VALUE;
  private static final String UC_TABLE_ID_KEY = UCCommitCoordinatorClient.UC_TABLE_ID_KEY;
  private static final String UC_CATALOG_NAME = "uc_catalog_factory_create";
  private static final String UC_CATALOG_CONNECTOR = "io.unitycatalog.spark.UCSingleCatalog";
  private static final String UC_CATALOG_URI =
      "https://uc-factory-create.example.com/api/2.1/unity-catalog";
  private static final String UC_CATALOG_TOKEN = "dapi_factory_create_token_7kP3";

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
            .config("spark.sql.catalog." + UC_CATALOG_NAME, UC_CATALOG_CONNECTOR)
            .config("spark.sql.catalog." + UC_CATALOG_NAME + ".uri", UC_CATALOG_URI)
            .config("spark.sql.catalog." + UC_CATALOG_NAME + ".token", UC_CATALOG_TOKEN)
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
    Map<String, String> props = new HashMap<>();
    props.put(FEATURE_CATALOG_MANAGED, FEATURE_SUPPORTED);
    props.put(UC_TABLE_ID_KEY, "factory_create_table_id_9x2b");

    DeltaSnapshotManager manager =
        SnapshotManagerFactory.forCreateTable(
            "/some/path", kernelEngine(), props, UC_CATALOG_NAME, spark);
    assertInstanceOf(UCManagedTableSnapshotManager.class, manager);
  }
}
