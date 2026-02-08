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

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.JsonNode
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.connector.catalog.Identifier
import scala.jdk.CollectionConverters._

/**
 * Unit tests for DeltaCatalog's V2 connector routing logic.
 *
 * Verifies that DeltaCatalog correctly routes table loading based on
 * DeltaSQLConf.V2_ENABLE_MODE:
 * - STRICT mode: Kernel's SparkTable (V2 connector)
 * - NONE mode (default): DeltaTableV2 (V1 connector)
 */
class DeltaCatalogSuite extends DeltaSQLCommandTest {

  private val jsonMapper = new ObjectMapper()

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

  test("STRICT: CREATE TABLE commits via Kernel (catalog-based external)") {
    withTempDir { tempDir =>
      val tableName = "test_kernel_create_catalog_external"
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
      val tableName = "test_v1_ctas_fallback"
      val location = new File(tempDir, tableName).getAbsolutePath

      withSQLConf(DeltaSQLConf.V2_ENABLE_MODE.key -> "STRICT") {
        sql(s"CREATE TABLE $tableName USING delta LOCATION '$location' AS SELECT 1 AS id")
        assert(!isKernelEngineInfo(readEngineInfo(location)))
        assert(sql(s"SELECT * FROM $tableName").collect().toSeq == Seq(org.apache.spark.sql.Row(1)))
      }
    }
  }

  test("STRICT: CREATE TABLE IF NOT EXISTS is a no-op when table already exists") {
    withTempDir { tempDir =>
      val tableName = "test_kernel_create_if_not_exists"
      val location = new File(tempDir, tableName).getAbsolutePath

      withSQLConf(DeltaSQLConf.V2_ENABLE_MODE.key -> "STRICT") {
        sql(s"CREATE TABLE $tableName (id INT) USING delta LOCATION '$location'")
        val jsonFilesBefore = listDeltaJsonCommits(location)

        sql(s"CREATE TABLE IF NOT EXISTS $tableName (id INT) USING delta LOCATION '$location'")
        val jsonFilesAfter = listDeltaJsonCommits(location)

        assert(isKernelEngineInfo(readEngineInfo(location)))
        assert(jsonFilesAfter == jsonFilesBefore)
      }
    }
  }

  test("STRICT: CREATE TABLE with partitioning commits partition columns via Kernel") {
    withTempDir { tempDir =>
      val tableName = "test_kernel_create_partitioned"
      val location = new File(tempDir, tableName).getAbsolutePath

      withSQLConf(DeltaSQLConf.V2_ENABLE_MODE.key -> "STRICT") {
        sql(
          s"CREATE TABLE $tableName (id INT, dt STRING) USING delta " +
            s"PARTITIONED BY (dt) LOCATION '$location'")

        assert(isKernelEngineInfo(readEngineInfo(location)))
        assert(readPartitionColumns(location) == Seq("dt"))
      }
    }
  }

  test("STRICT: CREATE TABLE persists user table properties and filters Spark reserved keys") {
    withTempDir { tempDir =>
      val tableName = "test_kernel_create_properties"
      val location = new File(tempDir, tableName).getAbsolutePath

      withSQLConf(DeltaSQLConf.V2_ENABLE_MODE.key -> "STRICT") {
        sql(
          s"CREATE TABLE $tableName (id INT) USING delta LOCATION '$location' " +
            s"TBLPROPERTIES ('delta.appendOnly'='true')")

        val conf = readMetadataConfiguration(location)
        assert(conf.get("delta.appendOnly").contains("true"))

        Seq(
          "provider",
          "location",
          "is_managed_location",
          "comment",
          "owner",
          "external",
          "path",
          "option.path").foreach { k =>
          assert(!conf.contains(k), s"metadata.configuration unexpectedly contains reserved key: $k")
        }
      }
    }
  }

  test("STRICT: CREATE TABLE throws TableAlreadyExistsException when table exists") {
    withTempDir { tempDir =>
      val tableName = "test_kernel_create_already_exists"
      val location = new File(tempDir, tableName).getAbsolutePath

      withSQLConf(DeltaSQLConf.V2_ENABLE_MODE.key -> "STRICT") {
        sql(s"CREATE TABLE $tableName (id INT) USING delta LOCATION '$location'")
        intercept[org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException] {
          sql(s"CREATE TABLE $tableName (id INT) USING delta LOCATION '$location'")
        }
      }
    }
  }

  test("STRICT: CREATE TABLE without LOCATION uses default managed location and commits via Kernel") {
    withTempDir { tempDir =>
      val dbName = "test_kernel_create_db_managed"
      val tableName = "test_kernel_create_catalog_managed"
      val dbLocation = new File(tempDir, "db").getAbsolutePath

      withSQLConf(
        DeltaSQLConf.V2_ENABLE_MODE.key -> "STRICT") {
        sql(s"CREATE DATABASE $dbName LOCATION '$dbLocation'")
        try {
          sql(s"USE $dbName")
          sql(s"CREATE TABLE $tableName (id INT) USING delta")

          val catalog = spark.sessionState.catalogManager.v2SessionCatalog.asInstanceOf[DeltaCatalog]
          val ident = Identifier.of(Array(dbName), tableName)
          val table = catalog.loadTable(ident)
          assert(table.isInstanceOf[SparkTable])

          val tableLocation = spark.sessionState.catalog
            .getTableMetadata(org.apache.spark.sql.catalyst.TableIdentifier(tableName, Some(dbName)))
            .location
            .toString
          assert(isKernelEngineInfo(readEngineInfo(tableLocation)))
        } finally {
          sql("USE default")
          sql(s"DROP TABLE IF EXISTS $dbName.$tableName")
          sql(s"DROP DATABASE IF EXISTS $dbName")
        }
      }
    }
  }

  test("NONE: CREATE TABLE routes through V1 (engineInfo is not kernel)") {
    withTempDir { tempDir =>
      val tableName = "test_v1_create_none_mode"
      val location = new File(tempDir, tableName).getAbsolutePath

      withSQLConf(DeltaSQLConf.V2_ENABLE_MODE.key -> "NONE") {
        sql(s"CREATE TABLE $tableName (id INT) USING delta LOCATION '$location'")
        assert(!isKernelEngineInfo(readEngineInfo(location)))
      }
    }
  }

  test("STRICT: CREATE TABLE with non-Delta provider falls through to V1 delegate") {
    withTempDir { tempDir =>
      val tableName = "test_parquet_strict_fallback"
      val location = new File(tempDir, tableName).getAbsolutePath

      withSQLConf(DeltaSQLConf.V2_ENABLE_MODE.key -> "STRICT") {
        sql(s"CREATE TABLE $tableName (id INT) USING parquet LOCATION '$location'")
        // Table should exist and be readable (created via V1/delegate, not Kernel)
        assert(sql(s"SELECT * FROM $tableName").collect().isEmpty)
      }
    }
  }

  test("STRICT: CREATE TABLE with multi-column partitioning commits all partition columns") {
    withTempDir { tempDir =>
      val tableName = "test_kernel_create_multi_partition"
      val location = new File(tempDir, tableName).getAbsolutePath

      withSQLConf(DeltaSQLConf.V2_ENABLE_MODE.key -> "STRICT") {
        sql(
          s"CREATE TABLE $tableName (id INT, year INT, month INT) USING delta " +
            s"PARTITIONED BY (year, month) LOCATION '$location'")

        assert(isKernelEngineInfo(readEngineInfo(location)))
        assert(readPartitionColumns(location) == Seq("year", "month"))
      }
    }
  }

  private def isKernelEngineInfo(engineInfoOpt: Option[String]): Boolean = {
    engineInfoOpt.exists(_.endsWith(DeltaKernelStagedDDLTable.ENGINE_INFO))
  }

  private def readEngineInfo(tablePath: String): Option[String] = {
    readSingleAction(tablePath, "commitInfo").flatMap { commitInfo =>
      val engineInfo = commitInfo.get("engineInfo")
      if (engineInfo != null) Option(engineInfo.asText()) else None
    }
  }

  private def readPartitionColumns(tablePath: String): Seq[String] = {
    val metadataOpt = readSingleAction(tablePath, "metaData")
    val cols = metadataOpt.map(_.get("partitionColumns")).orNull
    if (cols == null || !cols.isArray) {
      Nil
    } else {
      cols.elements().asScala.map(_.asText()).toSeq
    }
  }

  private def readMetadataConfiguration(tablePath: String): Map[String, String] = {
    val metadataOpt = readSingleAction(tablePath, "metaData")
    val conf = metadataOpt.map(_.get("configuration")).orNull
    if (conf == null || !conf.isObject) {
      Map.empty
    } else {
      conf.fields().asScala.map { e => e.getKey -> e.getValue.asText() }.toMap
    }
  }

  private def readSingleAction(tablePath: String, actionKey: String): Option[JsonNode] = {
    val logPath = new Path(new File(tablePath, "_delta_log/00000000000000000000.json").getPath)
    val fs = logPath.getFileSystem(spark.sessionState.newHadoopConf())
    val in = fs.open(logPath)
    val source = scala.io.Source.fromInputStream(in, "UTF-8")
    try {
      source
        .getLines()
        .flatMap { line =>
          val node = jsonMapper.readTree(line)
          val action = node.get(actionKey)
          if (action != null) Option(action) else None
        }
        .toSeq
        .headOption
    } finally {
      source.close() // also closes the underlying InputStream
    }
  }

  private def listDeltaJsonCommits(tablePath: String): Seq[String] = {
    val logDir = new Path(new File(tablePath, "_delta_log").getPath)
    val fs = logDir.getFileSystem(spark.sessionState.newHadoopConf())
    fs.listStatus(logDir)
      .map(_.getPath.getName)
      .filter(_.endsWith(".json"))
      .sorted
      .toSeq
  }
}
