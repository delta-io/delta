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
import io.delta.kernel.engine.Engine;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/**
 * Test base for the V2 changelog tests.
 *
 * <p>Configures a Spark session with:
 *
 * <ul>
 *   <li>The hybrid {@code DeltaCatalog} (spark-unified) as {@code spark_catalog}, so {@code
 *       TableCatalog.loadChangelog} routes into the {@code ChangelogSupport} trait.
 *   <li>{@code spark.databricks.delta.changelogV2.enabled = true}, the SQLConf gate behind which
 *       Auto-CDF is hidden in production.
 * </ul>
 *
 * <p>Lives in {@code spark-unified} (not {@code sparkV2}) so the in-tree hybrid {@code
 * DeltaCatalog.class} is on the test classpath. Running these tests from {@code sparkV2}'s test
 * scope is unsafe: {@code sparkV2} transitively pulls the released {@code
 * delta-spark_2.13-4.0.0.jar} via {@code kernelUnityCatalog(test->test) ->
 * kernelDefaults(test->test) -> "io.delta" %% "delta-spark" % "4.0.0" % "test"}. That released jar
 * carries a pre-#5320 {@code DeltaCatalog.class} (no {@code AbstractDeltaCatalog} ancestor) which
 * shadows the in-tree class at the same FQN and produces a runtime {@code ClassCastException} from
 * {@code SupportsPathIdentifier}'s self-type cast. {@code spark-unified} does not depend on {@code
 * sparkV2} at {@code test->test}, so the released jar is not on its test classpath.
 *
 * <p>This class is intentionally self-contained (does not extend {@code DeltaV2TestBase}) because
 * {@code DeltaV2TestBase} lives in {@code sparkV2}'s test scope and is not visible from {@code
 * spark-unified}.
 */
public abstract class DeltaChangelogTestBase {

  protected static SparkSession spark;
  protected static Engine defaultEngine;

  @BeforeAll
  public static void setUpChangelogSparkAndEngine() {
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

  @AfterAll
  public static void tearDownSpark() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  /** A runnable that can throw checked exceptions, for use with {@link #withSQLConf}. */
  @FunctionalInterface
  protected interface ThrowingRunnable {
    void run() throws Exception;
  }

  /**
   * Runs the given action with a Spark SQL configuration temporarily set, then restores the
   * original value afterwards (similar to Scala's {@code withSQLConf}).
   */
  protected void withSQLConf(String key, String value, ThrowingRunnable action) throws Exception {
    scala.Option<String> original = spark.conf().getOption(key);
    spark.conf().set(key, value);
    try {
      action.run();
    } finally {
      if (original.isDefined()) {
        spark.conf().set(key, original.get());
      } else {
        spark.conf().unset(key);
      }
    }
  }

  /**
   * Runs the given action and drops the specified tables afterwards, similar to Scala's {@code
   * withTable}.
   */
  protected void withTable(String[] tableNames, ThrowingRunnable action) throws Exception {
    try {
      action.run();
    } finally {
      for (String tableName : tableNames) {
        spark.sql(String.format("DROP TABLE IF EXISTS %s", tableName));
      }
    }
  }

  /** Runs the given action and removes the table directory afterwards. */
  protected void withTable(String tablePath, ThrowingRunnable action) throws Exception {
    try {
      action.run();
    } finally {
      try {
        org.apache.commons.io.FileUtils.deleteDirectory(new java.io.File(tablePath));
      } catch (java.io.IOException ignored) {
        // Test cleanup best-effort.
      }
    }
  }
}
