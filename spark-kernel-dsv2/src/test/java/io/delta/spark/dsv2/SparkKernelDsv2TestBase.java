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
package io.delta.spark.dsv2;

import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public abstract class SparkKernelDsv2TestBase {

  protected static SparkSession spark;
  protected static Engine defaultEngine;

  @BeforeAll
  public static void setUpSparkAndEngine() {
    spark =
        SparkSession.builder()
            .master("local[*]")
            .appName("SparkKernelDsv2Tests")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
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
}
