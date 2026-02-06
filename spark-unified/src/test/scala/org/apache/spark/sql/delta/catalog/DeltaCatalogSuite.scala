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

import io.delta.spark.internal.v2.catalog.DeltaKernelStagedCreateTable
import io.delta.spark.internal.v2.catalog.SparkTable
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import java.io.File
import java.net.URI
import java.util.Locale

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.connector.catalog.TableCatalog

import scala.util.Using

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

  private def isKernelEngineInfo(engineInfoOpt: Option[String]): Boolean = {
    engineInfoOpt.exists { engineInfo =>
      // Kernel prefixes the provided engineInfo with its own version information:
      // e.g. "Kernel-<ver>/<provided-engine-info>".
      engineInfo == DeltaKernelStagedCreateTable.ENGINE_INFO ||
      engineInfo.endsWith(s"/${DeltaKernelStagedCreateTable.ENGINE_INFO}")
    }
  }

  private def toLocalFile(pathOrUri: String): File = {
    if (pathOrUri.startsWith("file:")) {
      new File(new URI(pathOrUri))
    } else {
      new File(pathOrUri)
    }
  }

  private def commit0File(tablePathOrUri: String): File = {
    val root = toLocalFile(tablePathOrUri)
    new File(new File(root, "_delta_log"), "00000000000000000000.json")
  }

  private def readCommit0Lines(tablePathOrUri: String): Seq[String] = {
    val f = commit0File(tablePathOrUri)
    assert(f.isFile, s"Expected commit file to exist: ${f.getAbsolutePath}")
    Using.resource(scala.io.Source.fromFile(f))(_.getLines().toSeq)
  }

  private def readEngineInfoFromCommit0(tablePathOrUri: String): Option[String] = {
    val commitInfoLine = readCommit0Lines(tablePathOrUri)
      .find(_.contains("\"commitInfo\""))
      .getOrElse(fail(s"commitInfo action not found in ${commit0File(tablePathOrUri)}"))
    val commitInfoNode = jsonMapper.readTree(commitInfoLine).get("commitInfo")
    Option(commitInfoNode).flatMap(n => Option(n.get("engineInfo"))).map(_.asText())
  }

  private def readMetadataConfigurationFromCommit0(tablePathOrUri: String): Map[String, String] = {
    val metadataLine = readCommit0Lines(tablePathOrUri)
      .find(_.contains("\"metaData\""))
      .getOrElse(fail(s"metaData action not found in ${commit0File(tablePathOrUri)}"))
    val metadataNode = jsonMapper.readTree(metadataLine).get("metaData")
    val confNode = metadataNode.get("configuration")
    if (confNode == null || !confNode.isObject) {
      Map.empty
    } else {
      import scala.collection.JavaConverters._
      confNode.fields().asScala.map(e => e.getKey -> e.getValue.asText()).toMap
    }
  }

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

  test(
    "STRICT mode CREATE TABLE commits metadata via Kernel (engineInfo) for path-based tables") {
    withTempDir { tempDir =>
      val path = tempDir.getAbsolutePath
      withSQLConf(DeltaSQLConf.V2_ENABLE_MODE.key -> "STRICT") {
        sql(s"CREATE TABLE delta.`$path` (id INT, name STRING) USING delta")
        val engineInfo = readEngineInfoFromCommit0(path)
        assert(
          isKernelEngineInfo(engineInfo),
          s"Expected engineInfo to end with /${DeltaKernelStagedCreateTable.ENGINE_INFO} " +
            s"but got $engineInfo")
      }
    }
  }

  test(
    "STRICT mode CREATE TABLE commits metadata via Kernel (engineInfo) for catalog-based tables") {
    withTempDir { tempDir =>
      val tableName = "test_catalog_strict_engineinfo"
      val location = new File(tempDir, tableName).getAbsolutePath

      withSQLConf(DeltaSQLConf.V2_ENABLE_MODE.key -> "STRICT") {
        withTable(tableName) {
          sql(
            s"""
               |CREATE TABLE $tableName (id INT, name STRING)
               |USING delta
               |LOCATION '$location'
               |TBLPROPERTIES ('userKey' = 'userVal')
               |""".stripMargin)

          val engineInfo = readEngineInfoFromCommit0(location)
          assert(
            isKernelEngineInfo(engineInfo),
            s"Expected engineInfo to end with /${DeltaKernelStagedCreateTable.ENGINE_INFO} " +
              s"but got $engineInfo")

          val config = readMetadataConfigurationFromCommit0(location)
          assert(config.get("userKey").contains("userVal"))
          assert(!config.contains(TableCatalog.PROP_PROVIDER))
          assert(!config.contains(TableCatalog.PROP_LOCATION))
          assert(!config.contains("option.path"))
        }
      }
    }
  }

  test(
    "STRICT mode CREATE TABLE commits metadata via Kernel (engineInfo) for managed tables") {
    withSQLConf(DeltaSQLConf.V2_ENABLE_MODE.key -> "STRICT") {
      withTable("test_managed_strict_engineinfo") {
        sql("CREATE TABLE test_managed_strict_engineinfo (id INT, name STRING) USING delta")

        val catalogTable = spark.sessionState.catalog.getTableMetadata(
          new TableIdentifier("test_managed_strict_engineinfo"))
        val location = catalogTable.location.toString

        val engineInfo = readEngineInfoFromCommit0(location)
        assert(
          isKernelEngineInfo(engineInfo),
          s"Expected engineInfo to end with /${DeltaKernelStagedCreateTable.ENGINE_INFO} " +
            s"but got $engineInfo")
      }
    }
  }

  test("NONE mode CREATE TABLE does not use Kernel engineInfo") {
    withTempDir { tempDir =>
      val tableName = "test_catalog_none_engineinfo"
      val location = new File(tempDir, tableName).getAbsolutePath

      withSQLConf(DeltaSQLConf.V2_ENABLE_MODE.key -> "NONE") {
        withTable(tableName) {
          sql(s"CREATE TABLE $tableName (id INT, name STRING) USING delta LOCATION '$location'")
          val engineInfo = readEngineInfoFromCommit0(location)
          assert(!isKernelEngineInfo(engineInfo),
            s"Expected non-kernel engineInfo, but got $engineInfo")
        }
      }
    }
  }

  test("STRICT mode CREATE TABLE fails when the target table already exists") {
    withTempDir { tempDir =>
      val path = tempDir.getAbsolutePath
      withSQLConf(DeltaSQLConf.V2_ENABLE_MODE.key -> "STRICT") {
        sql(s"CREATE TABLE delta.`$path` (id INT) USING delta")
        intercept[TableAlreadyExistsException] {
          sql(s"CREATE TABLE delta.`$path` (id INT) USING delta")
        }
      }
    }
  }

  test("STRICT mode CREATE TABLE IF NOT EXISTS is idempotent for path-based tables") {
    withTempDir { tempDir =>
      val path = tempDir.getAbsolutePath
      withSQLConf(DeltaSQLConf.V2_ENABLE_MODE.key -> "STRICT") {
        sql(s"CREATE TABLE delta.`$path` (id INT) USING delta")
        sql(s"CREATE TABLE IF NOT EXISTS delta.`$path` (id INT) USING delta")

        val logDir = new File(toLocalFile(path), "_delta_log")
        val v1 = new File(logDir, "00000000000000000001.json")
        assert(!v1.exists(), s"Expected no additional commit, but found ${v1.getAbsolutePath}")
      }
    }
  }
}
