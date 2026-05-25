/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta

import java.io.File

import org.apache.spark.sql.delta.catalog.DeltaCatalog

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

/**
 * Distributed (multi-JVM) coverage for `CONVERT TO DELTA`'s parquet schema inference, which runs
 * inside `mapPartitions` on executors. Unlike the rest of the Delta test suite this runs against a
 * `local-cluster` master so executors are separate JVMs - the only setup that exercises the
 * executor code path faithfully. Heavier than a unit test; the cheap, single-JVM guard lives in
 * `DeltaFileOperationsParquetConverterSuite`.
 */
class ConvertToDeltaDistributedSuite extends SparkFunSuite {

  /**
   * Run `f` with a freshly built `local-cluster` SparkSession (2 single-core executor JVMs),
   * stopping it afterwards. A new session per test keeps executor state from leaking between
   * tests and mirrors how Spark's own multi-JVM suites are structured.
   */
  private def withLocalClusterSession(f: SparkSession => Unit): Unit = {
    // local-cluster launches executor JVMs via `$SPARK_HOME/bin/spark-class`, so it only works
    // when a Spark distribution is available. Spark's own multi-JVM suites rely on the
    // `spark.test.home` system property for this; the Delta build does not set it by default, so
    // skip (rather than fail) when it is absent. Provide it via, e.g.,
    // `-Dspark.test.home=<spark-dist>` to exercise this suite.
    assume(
      sys.props.get("spark.test.home").exists(_.nonEmpty),
      "spark.test.home must point to a Spark distribution to launch local-cluster executors")

    val warehouse = Utils.createTempDir()
    val spark = SparkSession.builder()
      .master("local-cluster[2, 1, 1024]")
      .appName("ConvertToDeltaDistributedSuite")
      .config(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key, classOf[DeltaCatalog].getName)
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.warehouse.dir", warehouse.getAbsolutePath)
      // Ship the driver's classpath (Delta + its deps) to the separately-launched executor JVMs;
      // the Spark distribution at spark.test.home only provides Spark itself.
      .config("spark.executor.extraClassPath", sys.props("java.class.path"))
      // Keep the test light: no need to spread work, and shuffle output stays small.
      .config(SQLConf.SHUFFLE_PARTITIONS.key, "5")
      .getOrCreate()
    try {
      f(spark)
    } finally {
      try spark.stop() finally {
        Utils.deleteRecursively(warehouse)
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
      }
    }
  }

  test("CONVERT TO DELTA infers parquet schema on executors without an active session") {
    withLocalClusterSession { spark =>
      val dir = Utils.createTempDir()
      try {
        val path = new File(dir, "parquet_tbl").getCanonicalPath
        // Write a plain parquet directory (no Delta log) from a distributed job.
        spark.range(0, 100)
          .selectExpr("id", "cast(id as string) as s")
          .repartition(4)
          .write.format("parquet").save(path)

        // Lists files and reads their footers via ParquetFileManifest inside `mapPartitions`.
        spark.sql(s"CONVERT TO DELTA parquet.`$path`")

        val converted = spark.read.format("delta").load(path)
        assert(converted.count() === 100)
        assert(converted.schema.fieldNames.toSet === Set("id", "s"))
      } finally {
        Utils.deleteRecursively(dir)
      }
    }
  }
}
