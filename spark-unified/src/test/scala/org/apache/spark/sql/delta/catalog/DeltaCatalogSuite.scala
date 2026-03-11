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

import io.delta.spark.internal.v2.catalog.SparkTable
import org.apache.spark.sql.delta.commands.TableCreationModes
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import java.io.File
import java.net.URI
import java.util
import java.util.Locale

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}

/**
 * Unit tests for DeltaCatalog's V2 connector routing logic.
 *
 * Verifies that DeltaCatalog correctly routes table loading based on
 * DeltaSQLConf.V2_ENABLE_MODE:
 * - STRICT mode: Kernel's SparkTable (V2 connector)
 * - NONE mode (default): DeltaTableV2 (V1 connector)
 */
class DeltaCatalogSuite extends DeltaSQLCommandTest {

  private class TrackingDeltaCatalog extends AbstractDeltaCatalog {
    var sessionLookups = 0
    var sessionResult: Option[CatalogTable] = None

    override protected[delta] def lookupExistingSessionCatalogTable(
        id: TableIdentifier): Option[CatalogTable] = {
      sessionLookups += 1
      sessionResult
    }
  }

  test("existing-table handoff parser strips internal properties") {
    val props = new util.HashMap[String, String]()
    props.put(ExistingTableHandoffContext.locationKey, "file:/tmp/handoff")
    props.put(s"option.${ExistingTableHandoffContext.tableTypeKey}", "MANAGED")
    props.put(ExistingTableHandoffContext.tableIdKey, "table-id")
    props.put("delta.appendOnly", "true")

    val handoff = ExistingTableHandoffContext.parseAndStrip(props, TableCreationModes.Replace)

    assert(handoff.contains(
      ExistingTableHandoffContext(
        URI.create("file:/tmp/handoff"),
        CatalogTableType.MANAGED,
        "table-id")))
    assert(!props.containsKey(ExistingTableHandoffContext.locationKey))
    assert(!props.containsKey(ExistingTableHandoffContext.tableTypeKey))
    assert(!props.containsKey(s"option.${ExistingTableHandoffContext.tableTypeKey}"))
    assert(!props.containsKey(ExistingTableHandoffContext.tableIdKey))
    assert(props.get("delta.appendOnly") == "true")
  }

  test("existing-table handoff parser rejects partial bundles") {
    val props = new util.HashMap[String, String]()
    props.put(ExistingTableHandoffContext.locationKey, "file:/tmp/handoff")
    props.put(ExistingTableHandoffContext.tableTypeKey, "MANAGED")

    val err = intercept[org.apache.spark.sql.delta.DeltaIllegalStateException] {
      ExistingTableHandoffContext.parseAndStrip(props, TableCreationModes.Replace)
    }
    assert(err.getErrorClass == "DELTA_INVALID_UNITY_CATALOG_EXISTING_TABLE_HANDOFF")
    assert(err.getMessage.contains("incomplete handoff bundle"))
  }

  test("existing-table handoff parser rejects bundles outside Unity Catalog") {
    val props = new util.HashMap[String, String]()
    props.put(ExistingTableHandoffContext.locationKey, "file:/tmp/handoff")
    props.put(s"option.${ExistingTableHandoffContext.tableTypeKey}", "MANAGED")
    props.put(ExistingTableHandoffContext.tableIdKey, "table-id")

    val err = intercept[org.apache.spark.sql.delta.DeltaIllegalStateException] {
      ExistingTableHandoffContext.parseAndStrip(
        props,
        TableCreationModes.Replace,
        allowExistingTableHandoff = false)
    }

    assert(err.getErrorClass == "DELTA_INVALID_UNITY_CATALOG_EXISTING_TABLE_HANDOFF")
    assert(err.getMessage.contains("outside Unity Catalog"))
  }

  test("existing-table handoff parser rejects partial bundles outside Unity Catalog") {
    val props = new util.HashMap[String, String]()
    props.put(s"option.${ExistingTableHandoffContext.locationKey}", "file:/tmp/handoff")

    val err = intercept[org.apache.spark.sql.delta.DeltaIllegalStateException] {
      ExistingTableHandoffContext.parseAndStrip(
        props,
        TableCreationModes.CreateOrReplace,
        allowExistingTableHandoff = false)
    }

    assert(err.getErrorClass == "DELTA_INVALID_UNITY_CATALOG_EXISTING_TABLE_HANDOFF")
    assert(err.getMessage.contains("outside Unity Catalog"))
  }

  test("create-when-missing marker strips internal property") {
    val props = new util.HashMap[String, String]()
    props.put(s"option.${ExistingTableHandoffContext.createWhenMissingKey}", "true")
    props.put("delta.appendOnly", "true")

    val createWhenMissing = ExistingTableHandoffContext.parseCreateWhenMissingAndStrip(
      props,
      TableCreationModes.CreateOrReplace)

    assert(createWhenMissing)
    assert(!props.containsKey(ExistingTableHandoffContext.createWhenMissingKey))
    assert(!props.containsKey(s"option.${ExistingTableHandoffContext.createWhenMissingKey}"))
    assert(props.get("delta.appendOnly") == "true")
  }

  test("create-when-missing marker rejects non-UC paths") {
    val props = new util.HashMap[String, String]()
    props.put(ExistingTableHandoffContext.createWhenMissingKey, "true")

    val err = intercept[org.apache.spark.sql.delta.DeltaIllegalStateException] {
      ExistingTableHandoffContext.parseCreateWhenMissingAndStrip(
        props,
        TableCreationModes.CreateOrReplace,
        allowExistingTableHandoff = false)
    }

    assert(err.getErrorClass == "DELTA_INVALID_UNITY_CATALOG_EXISTING_TABLE_HANDOFF")
    assert(err.getMessage.contains("outside Unity Catalog"))
  }

  test("existing-table handoff avoids delegated catalog reload") {
    val catalog = new TrackingDeltaCatalog
    val tableId = TableIdentifier("t", Some("default"))
    val handoff = Some(
      ExistingTableHandoffContext(
        URI.create("file:/tmp/handoff"),
        CatalogTableType.EXTERNAL,
        "table-id"))

    val resolved = catalog.resolveExistingTableContext(
      tableId,
      TableCreationModes.Replace,
      handoff)

    assert(resolved.isDefined)
    assert(resolved.get.identifier == tableId)
    assert(resolved.get.tableType == CatalogTableType.EXTERNAL)
    assert(resolved.get.storage.locationUri.contains(URI.create("file:/tmp/handoff")))
    assert(catalog.sessionLookups == 1)
  }

  test("strict existing-table handoff bypasses session catalog lookup") {
    val catalog = new TrackingDeltaCatalog
    val tableId = TableIdentifier("t", Some("default"))
    catalog.sessionResult = Some(CatalogTable(
      identifier = tableId,
      tableType = CatalogTableType.EXTERNAL,
      storage = CatalogStorageFormat.empty,
      schema = new org.apache.spark.sql.types.StructType(),
      provider = Some("delta")))
    val handoff = Some(
      ExistingTableHandoffContext(
        URI.create("file:/tmp/handoff"),
        CatalogTableType.MANAGED,
        "table-id"))

    val resolved = catalog.resolveExistingTableContext(
      tableId,
      TableCreationModes.Replace,
      handoff,
      requireExistingTableHandoff = true)

    assert(resolved.isDefined)
    assert(resolved.get.tableType == CatalogTableType.MANAGED)
    assert(resolved.get.storage.locationUri.contains(URI.create("file:/tmp/handoff")))
    assert(catalog.sessionLookups == 0)
  }

  test("strict external existing-table handoff bypasses session catalog lookup") {
    val catalog = new TrackingDeltaCatalog
    val tableId = TableIdentifier("t", Some("default"))
    catalog.sessionResult = Some(CatalogTable(
      identifier = tableId,
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty.copy(locationUri = Some(URI.create("file:/tmp/other"))),
      schema = new org.apache.spark.sql.types.StructType(),
      provider = Some("delta")))
    val handoff = Some(
      ExistingTableHandoffContext(
        URI.create("file:/tmp/external"),
        CatalogTableType.EXTERNAL,
        "table-id"))

    val resolved = catalog.resolveExistingTableContext(
      tableId,
      TableCreationModes.Replace,
      handoff,
      requireExistingTableHandoff = true)

    assert(resolved.isDefined)
    assert(resolved.get.tableType == CatalogTableType.EXTERNAL)
    assert(resolved.get.storage.locationUri.contains(URI.create("file:/tmp/external")))
    assert(catalog.sessionLookups == 0)
  }

  test("existing-table handoff does not rediscover existing table when absent") {
    val catalog = new TrackingDeltaCatalog
    val tableId = TableIdentifier("t", Some("default"))

    val resolved = catalog.resolveExistingTableContext(
      tableId,
      TableCreationModes.CreateOrReplace,
      existingTableHandoff = None)

    assert(resolved.isEmpty)
    assert(catalog.sessionLookups == 1)
  }

  test("existing-table handoff is required when strict mode is enabled") {
    val catalog = new TrackingDeltaCatalog
    val tableId = TableIdentifier("t", Some("default"))

    val err = intercept[org.apache.spark.sql.delta.DeltaIllegalStateException] {
      catalog.resolveExistingTableContext(
        tableId,
        TableCreationModes.Replace,
        existingTableHandoff = None,
        requireExistingTableHandoff = true)
    }

    assert(err.getErrorClass == "DELTA_INVALID_UNITY_CATALOG_EXISTING_TABLE_HANDOFF")
    assert(err.getMessage.contains("missing handoff bundle"))
    assert(catalog.sessionLookups == 0)
  }

  test("create-when-missing bypasses session catalog lookup") {
    val catalog = new TrackingDeltaCatalog
    val tableId = TableIdentifier("t", Some("default"))
    catalog.sessionResult = Some(CatalogTable(
      identifier = tableId,
      tableType = CatalogTableType.EXTERNAL,
      storage = CatalogStorageFormat.empty,
      schema = new org.apache.spark.sql.types.StructType(),
      provider = Some("delta")))

    val resolved = catalog.resolveExistingTableContext(
      tableId,
      TableCreationModes.CreateOrReplace,
      existingTableHandoff = None,
      skipExistingTableLookup = true)

    assert(resolved.isEmpty)
    assert(catalog.sessionLookups == 0)
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
}
