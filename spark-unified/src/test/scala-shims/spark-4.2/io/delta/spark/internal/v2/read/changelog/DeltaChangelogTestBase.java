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
package io.delta.spark.internal.v2.read.changelog;

import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.spark.internal.v2.DeltaV2TestBase;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;

/**
 * Test base for the V2 changelog tests.
 *
 * <p>Swaps the default {@link DeltaV2TestBase} Spark session for one configured with:
 *
 * <ul>
 *   <li>The hybrid {@code DeltaCatalog} (spark-unified) as {@code spark_catalog}, so {@code
 *       TableCatalog.loadChangelog} routes into the {@code ChangelogSupport} trait.
 *   <li>{@code spark.databricks.delta.changelogV2.enabled = true}, the SQLConf gate behind which
 *       Auto-CDF is hidden in production.
 * </ul>
 *
 * <p>Keeping these two settings out of {@code DeltaV2TestBase} means the rest of the V2 test suites
 * continue to exercise the V1-only catalog and the gated-off catalog default that production users
 * hit.
 */
public abstract class DeltaChangelogTestBase extends DeltaV2TestBase {

  @BeforeAll
  public static void setUpChangelogSparkAndEngine() {
    if (spark != null) {
      spark.stop();
    }
    spark =
        SparkSession.builder()
            .master("local[*]")
            .appName("SparkKernelDsv2ChangelogTests")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtensionV1")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.catalog.dsv2", "io.delta.spark.internal.v2.catalog.TestCatalog")
            .config("spark.sql.catalog.dsv2.base_path", System.getProperty("java.io.tmpdir"))
            .config("spark.databricks.delta.changelogV2.enabled", "true")
            .getOrCreate();
    defaultEngine = DefaultEngine.create(spark.sessionState().newHadoopConf());
  }
}
