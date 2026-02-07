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
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.connector.catalog.Identifier

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

  private def isKernelEngineInfo(engineInfoOpt: Option[String]): Boolean = {
    engineInfoOpt.exists(_.endsWith(DeltaKernelStagedDDLTable.ENGINE_INFO))
  }

  private def readEngineInfo(tablePath: String): Option[String] = {
    val logPath = new Path(new File(tablePath, "_delta_log/00000000000000000000.json").getPath)
    val fs = logPath.getFileSystem(spark.sessionState.newHadoopConf())
    val in = fs.open(logPath)
    try {
      val it = scala.io.Source.fromInputStream(in, "UTF-8").getLines()
      it.flatMap { line =>
        val node = jsonMapper.readTree(line)
        val commitInfo = node.get("commitInfo")
        if (commitInfo != null && commitInfo.has("engineInfo")) {
          Option(commitInfo.get("engineInfo").asText())
        } else {
          None
        }
      }.toSeq.headOption
    } finally {
      in.close()
    }
  }
}
