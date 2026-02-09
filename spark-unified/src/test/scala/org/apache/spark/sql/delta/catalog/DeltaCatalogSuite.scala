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

package org.apache.spark.sql.delta.catalog

import io.delta.spark.internal.v2.catalog.DeltaKernelStagedDDLTable
import io.delta.spark.internal.v2.catalog.SparkTable
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import java.io.File
import java.util.Locale

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.connector.catalog.TableCatalog

/**
 * Unit tests for DeltaCatalog's V2 connector routing logic.
 *
 * Verifies that DeltaCatalog correctly routes table loading based on
 * DeltaSQLConf.V2_ENABLE_MODE:
 * - STRICT mode: Kernel's SparkTable (V2 connector)
 * - NONE mode (default): DeltaTableV2 (V1 connector)
 */
class DeltaCatalogSuite extends DeltaSQLCommandTest {

  private val modeTestCases = Seq(
    ("STRICT", classOf[SparkTable], "Kernel SparkTable"),
    ("NONE", classOf[DeltaTableV2], "DeltaTableV2")
  )

  modeTestCases.foreach { case (mode, expectedClass, description) =>
    test(s"catalog-based table with mode=$mode returns $description") {
      withTempDir { tempDir =>
        val tableName = s"test_catalog_${mode.toLowerCase(Locale.ROOT)}"
        val location = new File(tempDir, tableName).getAbsolutePath

        withSQLConf(DeltaSQLConf.V2_ENABLE_MODE.key -> mode) {
          sql(s"CREATE TABLE $tableName (id INT, name STRING) USING delta LOCATION '$location'")

          val catalog = spark.sessionState.catalogManager.v2SessionCatalog
            .asInstanceOf[DeltaCatalog]
          val ident = org.apache.spark.sql.connector.catalog.Identifier
            .of(Array("default"), tableName)
          val table = catalog.loadTable(ident)

          assert(table.getClass == expectedClass,
            s"Mode $mode should return ${expectedClass.getSimpleName}")
        }
      }
    }
  }

  modeTestCases.foreach { case (mode, expectedClass, description) =>
    test(s"path-based table with mode=$mode returns $description") {
      withTempDir { tempDir =>
        val path = tempDir.getAbsolutePath

        withSQLConf(DeltaSQLConf.V2_ENABLE_MODE.key -> mode) {
          sql(s"CREATE TABLE delta.`$path` (id INT, name STRING) USING delta")

          val catalog = spark.sessionState.catalogManager.v2SessionCatalog
            .asInstanceOf[DeltaCatalog]
          val ident = org.apache.spark.sql.connector.catalog.Identifier
            .of(Array("delta"), path)
          val table = catalog.loadTable(ident)

          assert(table.getClass == expectedClass,
            s"Mode $mode should return ${expectedClass.getSimpleName} for path-based table")
        }
      }
    }
  }

  private def isKernelEngineInfo(engineInfo: String): Boolean = {
    engineInfo != null && engineInfo.endsWith(DeltaKernelStagedDDLTable.ENGINE_INFO)
  }

  private def listDeltaJsonCommits(tablePath: String): Seq[File] = {
    val localPath = {
      val uri = new org.apache.hadoop.fs.Path(tablePath).toUri
      if (uri.getScheme == null || uri.getScheme != "file") tablePath else uri.getPath
    }
    val logDir = new File(localPath, "_delta_log")
    assert(logDir.isDirectory, s"Missing _delta_log at $logDir")
    val commits = logDir.listFiles().toSeq
      .filter(f => f.isFile && f.getName.endsWith(".json"))
      .sortBy(_.getName)
    assert(commits.nonEmpty, s"No delta json commits in $logDir")
    commits
  }

  private def readSingleActionField[T](tablePath: String, actionCol: String, field: String): T = {
    val commitFile = listDeltaJsonCommits(tablePath).head.getAbsolutePath
    val df = spark.read.json(commitFile)
      .where(s"$actionCol is not null")
      .select(s"$actionCol.$field")
    val rows = df.collect()
    assert(rows.length == 1, s"Expected single $actionCol action in $commitFile")
    rows.head.getAs[T](0)
  }

  private def readEngineInfo(tablePath: String): String = {
    readSingleActionField[String](tablePath, "commitInfo", "engineInfo")
  }

  private def readPartitionColumns(tablePath: String): Seq[String] = {
    val commitFile = listDeltaJsonCommits(tablePath).head.getAbsolutePath
    val df = spark.read.json(commitFile)
      .where("metaData is not null")
      .select("metaData.partitionColumns")
    val rows = df.collect()
    assert(rows.length == 1, s"Expected single metaData action in $commitFile")
    rows.head.get(0).asInstanceOf[scala.collection.Seq[String]].toSeq
  }

  private def readMetadataConfiguration(tablePath: String): Map[String, String] = {
    val commitFile = listDeltaJsonCommits(tablePath).head.getAbsolutePath
    val df = spark.read.json(commitFile)
      .where("metaData is not null")
      .select("metaData.configuration")
    val rows = df.collect()
    assert(rows.length == 1, s"Expected single metaData action in $commitFile")
    val confRow = rows.head.getAs[org.apache.spark.sql.Row](0)
    confRow.schema.fieldNames.zipWithIndex
      .flatMap { case (k, idx) =>
        if (confRow.isNullAt(idx)) None else Some(k -> confRow.getString(idx))
      }
      .toMap
  }

  test("STRICT: CREATE TABLE commits via Kernel (catalog-based external)") {
    withTempDir { tempDir =>
      val tableName = "strict_kernel_catalog_external"
      val location = new File(tempDir, tableName).getAbsolutePath

      withSQLConf(DeltaSQLConf.V2_ENABLE_MODE.key -> "STRICT") {
        sql(s"CREATE TABLE $tableName (id INT, name STRING) USING delta LOCATION '$location'")

        assert(isKernelEngineInfo(readEngineInfo(location)))
      }
    }
  }

  test("STRICT: CREATE TABLE commits via Kernel (path-based external)") {
    withTempDir { tempDir =>
      val path = tempDir.getAbsolutePath
      withSQLConf(DeltaSQLConf.V2_ENABLE_MODE.key -> "STRICT") {
        sql(s"CREATE TABLE delta.`$path` (id INT, name STRING) USING delta")
        assert(isKernelEngineInfo(readEngineInfo(path)))
      }
    }
  }

  test("STRICT: CTAS falls back to V1 staged create (engineInfo is not kernel)") {
    withTempDir { tempDir =>
      val tableName = "strict_ctas_v1"
      val location = new File(tempDir, tableName).getAbsolutePath

      withSQLConf(DeltaSQLConf.V2_ENABLE_MODE.key -> "STRICT") {
        intercept[Exception] {
          sql(
            s"CREATE TABLE $tableName USING delta LOCATION '$location' AS SELECT 1 AS id, 'a' AS name")
        }
      }
    }
  }

  test("STRICT: CREATE TABLE with partitioning commits partition columns via Kernel") {
    withTempDir { tempDir =>
      val tableName = "strict_kernel_partitioned"
      val location = new File(tempDir, tableName).getAbsolutePath

      withSQLConf(DeltaSQLConf.V2_ENABLE_MODE.key -> "STRICT") {
        sql(
          s"CREATE TABLE $tableName (id INT, name STRING) USING delta " +
            s"PARTITIONED BY (id) LOCATION '$location'")
        assert(readPartitionColumns(location) == Seq("id"))
      }
    }
  }

  test("STRICT: CREATE TABLE persists user table properties and filters Spark reserved keys") {
    withTempDir { tempDir =>
      val tableName = "strict_kernel_properties"
      val location = new File(tempDir, tableName).getAbsolutePath

      withSQLConf(DeltaSQLConf.V2_ENABLE_MODE.key -> "STRICT") {
        sql(
          s"CREATE TABLE $tableName (id INT) USING delta LOCATION '$location' " +
            "TBLPROPERTIES ('k1'='v1', 'k2'='v2')")

        val conf = readMetadataConfiguration(location)
        assert(conf.get("k1").contains("v1"))
        assert(conf.get("k2").contains("v2"))

        // Spark-reserved keys should not be persisted into metadata.configuration.
        assert(!conf.contains(TableCatalog.PROP_LOCATION))
        assert(!conf.contains(TableCatalog.PROP_PROVIDER))
        assert(!conf.contains(TableCatalog.PROP_IS_MANAGED_LOCATION))
        assert(!conf.contains("path"))
        assert(!conf.contains("option.path"))
      }
    }
  }

  test("STRICT: CREATE TABLE throws TableAlreadyExistsException when table exists") {
    withTempDir { tempDir =>
      val path = tempDir.getAbsolutePath
      withSQLConf(DeltaSQLConf.V2_ENABLE_MODE.key -> "STRICT") {
        sql(s"CREATE TABLE delta.`$path` (id INT) USING delta")
        intercept[org.apache.spark.sql.AnalysisException] {
          sql(s"CREATE TABLE delta.`$path` (id INT) USING delta")
        }
      }
    }
  }

  test("STRICT: CREATE TABLE without LOCATION uses default managed location and commits via Kernel") {
    withSQLConf(DeltaSQLConf.V2_ENABLE_MODE.key -> "STRICT") {
      val tableName = "strict_kernel_managed_default_location"
      sql(s"CREATE TABLE $tableName (id INT) USING delta")
      val catalogTable =
        spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName, Some("default")))
      val tablePath = new org.apache.hadoop.fs.Path(catalogTable.location).toString
      assert(isKernelEngineInfo(readEngineInfo(tablePath)))
    }
  }

  test("NONE: CREATE TABLE routes through V1 (engineInfo is not kernel)") {
    withTempDir { tempDir =>
      val tableName = "none_mode_v1"
      val location = new File(tempDir, tableName).getAbsolutePath

      withSQLConf(DeltaSQLConf.V2_ENABLE_MODE.key -> "NONE") {
        sql(s"CREATE TABLE $tableName (id INT) USING delta LOCATION '$location'")
        assert(!isKernelEngineInfo(readEngineInfo(location)))
      }
    }
  }
}
