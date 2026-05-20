/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package io.delta.internal

import java.net.URI
import java.util.{HashMap => JHashMap}

import scala.jdk.CollectionConverters._

import io.delta.kernel.internal.SnapshotImpl
import io.delta.spark.internal.v2.catalog.SparkTable
import io.delta.spark.internal.v2.snapshot.PathBasedSnapshotManager
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTableType}
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.delta.{DeltaLog, DeltaOptions}
import org.apache.spark.sql.delta.Relocated.StreamingRelation
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.sources.{DeltaSourceMetadataTrackingLog, DeltaSourceUtils, DeltaSQLConf, PersistedMetadata}
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class ApplyV2ReadOptionsSuite extends DeltaSQLCommandTest {

  // Mirrors the rule order in DeltaSparkSessionExtension: ApplyV2Streaming runs first to produce
  // a StreamingRelationV2, then ApplyV2ReadOptions plumbs read options into the table.
  private def applyRules(plan: LogicalPlan): LogicalPlan = {
    val afterStreaming = new ApplyV2Streaming(spark).apply(plan)
    new ApplyV2ReadOptions().apply(afterStreaming)
  }

  private def assertV2(result: LogicalPlan): Unit = {
    result match {
      case StreamingRelationV2(_, _, _: SparkTable, _, _, _, _, v1Relation) =>
        assert(v1Relation.isEmpty)
      case other =>
        fail(s"Expected StreamingRelationV2, got $other")
    }
  }

  private def createDeltaTable(path: String): Unit = {
    spark.range(1).selectExpr("id").write.format("delta").save(path)
  }

  private def createPartitionedDeltaTable(path: String): Unit = {
    spark.range(1)
      .selectExpr("cast(id as int) as id", "'2024-01-01' as date", "'foo' as name")
      .write.format("delta").partitionBy("date").save(path)
  }

  private def createCatalogTable(locationUri: URI, ucManaged: Boolean): CatalogTable = {
    val storageProps = new JHashMap[String, String]()
    if (ucManaged) {
      storageProps.put(UCCommitCoordinatorClient.UC_TABLE_ID_KEY, "uc-table-id")
      storageProps.put("delta.feature.catalogManaged", "supported")
    }
    val identifier = TableIdentifier("tbl", Some("default"), Some("spark_catalog"))
    val storage = CatalogStorageFormat(
      locationUri = Some(locationUri),
      inputFormat = None,
      outputFormat = None,
      serde = None,
      compressed = false,
      properties = storageProps.asScala.toMap)
    CatalogTable(
      identifier = identifier,
      tableType = CatalogTableType.MANAGED,
      storage = storage,
      schema = new StructType(),
      provider = None,
      partitionColumnNames = Seq.empty,
      bucketSpec = None,
      properties = Map.empty)
  }

  private val cdcColumnNames = Set(
    CDCReader.CDC_TYPE_COLUMN_NAME,
    CDCReader.CDC_COMMIT_VERSION,
    CDCReader.CDC_COMMIT_TIMESTAMP)

  test("Case 1: StreamingRelation with readChangeFeed=true includes CDC columns") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      createDeltaTable(path)
      val catalogTable = createCatalogTable(dir.toURI, ucManaged = false)
      val dataSource = DataSource(
        sparkSession = spark,
        userSpecifiedSchema = None,
        className = "delta",
        options = Map("path" -> path, DeltaOptions.CDC_READ_OPTION -> "true"),
        catalogTable = Some(catalogTable))
      val plan = StreamingRelation(dataSource)

      withSQLConf(DeltaSQLConf.V2_ENABLE_MODE.key -> "STRICT") {
        val result = applyRules(plan)
        assertV2(result)
        val outputNames = result.output.map(_.name).toSet
        assert(cdcColumnNames.subsetOf(outputNames),
          s"Output should contain CDC columns. Got: ${outputNames}")
      }
    }
  }

  test("Case 1: StreamingRelation without readChangeFeed excludes CDC columns") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      createDeltaTable(path)
      val catalogTable = createCatalogTable(dir.toURI, ucManaged = false)
      val dataSource = DataSource(
        sparkSession = spark,
        userSpecifiedSchema = None,
        className = "delta",
        options = Map("path" -> path),
        catalogTable = Some(catalogTable))
      val plan = StreamingRelation(dataSource)

      withSQLConf(DeltaSQLConf.V2_ENABLE_MODE.key -> "STRICT") {
        val result = applyRules(plan)
        assertV2(result)
        val outputNames = result.output.map(_.name).toSet
        assert(cdcColumnNames.intersect(outputNames).isEmpty,
          s"Output should not contain CDC columns. Got: ${outputNames}")
      }
    }
  }

  test("Case 2: augments StreamingRelationV2 with CDC columns appended after data columns") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      createPartitionedDeltaTable(path)
      val catalogTable = createCatalogTable(dir.toURI, ucManaged = false)
      val ident = Identifier.of(
        catalogTable.identifier.database.toArray,
        catalogTable.identifier.table)
      // Build the input table without the CDC option so its schema lacks CDC columns. The rule
      // should rebuild the table with the CDC option from extraOptions and append CDC columns.
      val table = new SparkTable(ident, catalogTable, new JHashMap[String, String]())
      val cdcOpts = new JHashMap[String, String]()
      cdcOpts.put(DeltaOptions.CDC_READ_OPTION, "true")
      val extraOptions = new CaseInsensitiveStringMap(cdcOpts)

      val plan = StreamingRelationV2(
        source = None,
        sourceName = DeltaSourceUtils.NAME,
        table = table,
        extraOptions = extraOptions,
        output = toAttributes(table.schema()),
        catalog = None,
        identifier = Some(ident),
        v1Relation = None)

      withSQLConf(DeltaSQLConf.V2_ENABLE_MODE.key -> "STRICT") {
        val result = applyRules(plan)
        val outputNames = result.output.map(_.name)
        val expected = Seq(
          "id", "date", "name",
          CDCReader.CDC_TYPE_COLUMN_NAME,
          CDCReader.CDC_COMMIT_VERSION,
          CDCReader.CDC_COMMIT_TIMESTAMP)
        assert(outputNames == expected,
          s"Expected original column order + CDC, got: $outputNames")
      }
    }
  }

  test("Case 2: StreamingRelationV2 without readChangeFeed is unchanged") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      createDeltaTable(path)
      val catalogTable = createCatalogTable(dir.toURI, ucManaged = false)
      val ident = Identifier.of(
        catalogTable.identifier.database.toArray,
        catalogTable.identifier.table)
      val table = new SparkTable(ident, catalogTable, new JHashMap[String, String]())

      val plan = StreamingRelationV2(
        source = None,
        sourceName = DeltaSourceUtils.NAME,
        table = table,
        extraOptions = CaseInsensitiveStringMap.empty,
        output = toAttributes(table.schema()),
        catalog = None,
        identifier = Some(ident),
        v1Relation = None)

      withSQLConf(DeltaSQLConf.V2_ENABLE_MODE.key -> "STRICT") {
        val result = applyRules(plan)
        assert(result eq plan, "Plan should be returned unchanged")
      }
    }
  }

  test("ApplyV2ReadOptions is idempotent: applying the rule twice has no effect") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      createPartitionedDeltaTable(path)
      val catalogTable = createCatalogTable(dir.toURI, ucManaged = false)
      val ident = Identifier.of(
        catalogTable.identifier.database.toArray,
        catalogTable.identifier.table)
      val table = new SparkTable(ident, catalogTable, new JHashMap[String, String]())
      val cdcOpts = new JHashMap[String, String]()
      cdcOpts.put(DeltaOptions.CDC_READ_OPTION, "true")
      val extraOptions = new CaseInsensitiveStringMap(cdcOpts)

      val plan = StreamingRelationV2(
        source = None,
        sourceName = DeltaSourceUtils.NAME,
        table = table,
        extraOptions = extraOptions,
        output = toAttributes(table.schema()),
        catalog = None,
        identifier = Some(ident),
        v1Relation = None)

      withSQLConf(DeltaSQLConf.V2_ENABLE_MODE.key -> "STRICT") {
        val rule = new ApplyV2ReadOptions()
        val once = rule.apply(plan)
        val twice = rule.apply(once)
        assert(twice eq once, "Second application should return the same plan instance")
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Rebuild StreamingRelationV2 if provided schema tracking log provided
  // ---------------------------------------------------------------------------

  /** The data-schema seeded into the tracking log by [[seedSchemaLogWithExtraColumn]]. */
  private val seededFieldNames: Seq[String] = Seq("id", "extra")

  private def buildStreamingRelationV2(
      table: SparkTable, extraOptions: Map[String, String]): StreamingRelationV2 = {
    StreamingRelationV2(
      source = None,
      sourceName = "delta",
      table = table,
      extraOptions = new CaseInsensitiveStringMap(extraOptions.asJava),
      output = toAttributes(table.schema),
      catalog = None,
      identifier = Some(table.getIdentifier),
      v1Relation = None)
  }

  /**
   * Pre-seed the schema-tracking log at `schemaLogPath` with a 2-column schema
   * (`id LONG, extra STRING`) that differs from the underlying snapshot's 1-column schema
   */
  private def seedSchemaLogWithExtraColumn(tablePath: String, schemaLogPath: String): Unit = {
    val deltaLog = DeltaLog.forTable(spark, tablePath)
    val snapshotManager =
      new PathBasedSnapshotManager(tablePath, deltaLog.newDeltaHadoopConf())
    val tableId =
      snapshotManager.loadLatestSnapshot.asInstanceOf[SnapshotImpl].getMetadata.getId
    val trackingLog = DeltaSourceMetadataTrackingLog.create(
      spark, schemaLogPath, tableId, tablePath, parameters = Map.empty[String, String])
    val customSchemaJson =
      """{"type":"struct","fields":[
        |{"name":"id","type":"long","nullable":true,"metadata":{}},
        |{"name":"extra","type":"string","nullable":true,"metadata":{}}]}""".stripMargin
    val emptyPartitionJson = """{"type":"struct","fields":[]}"""
    val seededEntry = PersistedMetadata(
      tableId,
      deltaCommitVersion = 0L,
      dataSchemaJson = customSchemaJson,
      partitionSchemaJson = emptyPartitionJson,
      sourceMetadataPath = tablePath + "/_delta_log/_streaming_metadata")
    trackingLog.writeNewMetadata(seededEntry, replaceCurrent = false)
  }

  /** Asserts the table's schema matches the entry written by [[seedSchemaLogWithExtraColumn]]. */
  private def assertSchemaMatchesSeededLogEntry(table: SparkTable): Unit = {
    assert(table.schema.fieldNames.toSeq == seededFieldNames)
    assert(table.schema.fields(1).dataType == StringType)
  }

  /**
   * Build a catalog-backed SparkTable rooted at `tableLocationUri`. Mirrors the common production
   * path through DeltaCatalog and is the default for tests that do not specifically distinguish
   * between path-based and catalog-based construction.
   */
  private def buildCatalogBasedSparkTable(
      tableLocationUri: URI, options: JHashMap[String, String]): SparkTable = {
    val catalogTable = createCatalogTable(tableLocationUri, ucManaged = false)
    val identifier = Identifier.of(
      catalogTable.identifier.database.toArray, catalogTable.identifier.table)
    new SparkTable(identifier, catalogTable, options)
  }

  private def applyReadOptions(plan: LogicalPlan): LogicalPlan = {
    new ApplyV2ReadOptions().apply(plan)
  }

  test("schema-tracking rebuild: path-based SparkTable picks up the persisted schema") {
    withTempDir { tableDir =>
      withTempDir { schemaLogDir =>
        val tablePath = tableDir.getCanonicalPath
        createDeltaTable(tablePath) // snapshot schema: id BIGINT
        val schemaLogPath = schemaLogDir.getCanonicalPath
        seedSchemaLogWithExtraColumn(tablePath, schemaLogPath)

        val identifier = Identifier.of(Array("default"), "tbl")
        val table = new SparkTable(identifier, tablePath)
        assert(!table.getOptions.containsKey(DeltaOptions.SCHEMA_TRACKING_LOCATION))

        val plan = buildStreamingRelationV2(
          table, Map(DeltaOptions.SCHEMA_TRACKING_LOCATION -> schemaLogPath))
        val result = applyReadOptions(plan)
        assertV2(result)
        val rebuiltTable = result.asInstanceOf[StreamingRelationV2].table.asInstanceOf[SparkTable]

        assert(rebuiltTable ne table, "rebuild should produce a new SparkTable")
        assert(rebuiltTable.getOptions.containsKey(DeltaOptions.SCHEMA_TRACKING_LOCATION))
        assert(rebuiltTable.getOptions.get(DeltaOptions.SCHEMA_TRACKING_LOCATION) ==
          schemaLogPath)
        assert(!rebuiltTable.getCatalogTable.isPresent,
          "path branch should not have catalogTable")
        // Rebuilt schema is driven by the persisted entry, not the snapshot.
        assertSchemaMatchesSeededLogEntry(rebuiltTable)
        // And the rule's output is re-derived from that rebuilt schema.
        assert(result.output.map(_.name) == seededFieldNames)

        // Idempotent: re-applying the rule does not rebuild a second time.
        val reappliedResult = applyReadOptions(result).asInstanceOf[StreamingRelationV2]
        assert(reappliedResult.table eq rebuiltTable, "re-applying rule should not rebuild")
      }
    }
  }

  test("schema-tracking rebuild: catalog-based SparkTable picks up the persisted schema and " +
      "keeps its CatalogTable") {
    withTempDir { tableDir =>
      withTempDir { schemaLogDir =>
        val tablePath = tableDir.getCanonicalPath
        createDeltaTable(tablePath)
        val schemaLogPath = schemaLogDir.getCanonicalPath
        seedSchemaLogWithExtraColumn(tablePath, schemaLogPath)

        val table = buildCatalogBasedSparkTable(tableDir.toURI, new JHashMap[String, String]())
        assert(!table.getOptions.containsKey(DeltaOptions.SCHEMA_TRACKING_LOCATION))
        assert(table.getCatalogTable.isPresent)

        val plan = buildStreamingRelationV2(
          table, Map(DeltaOptions.SCHEMA_TRACKING_LOCATION -> schemaLogPath))
        val result = applyReadOptions(plan)
        assertV2(result)
        val rebuiltTable = result.asInstanceOf[StreamingRelationV2].table.asInstanceOf[SparkTable]

        assert(rebuiltTable.getOptions.containsKey(DeltaOptions.SCHEMA_TRACKING_LOCATION))
        assert(rebuiltTable.getCatalogTable.isPresent,
          "catalog branch should keep CatalogTable")
        assertSchemaMatchesSeededLogEntry(rebuiltTable)
      }
    }
  }

  test("schema-tracking rebuild: triggered by SCHEMA_TRACKING_LOCATION_ALIAS option key") {
    withTempDir { tableDir =>
      withTempDir { schemaLogDir =>
        val tablePath = tableDir.getCanonicalPath
        createDeltaTable(tablePath)
        val schemaLogPath = schemaLogDir.getCanonicalPath
        seedSchemaLogWithExtraColumn(tablePath, schemaLogPath)

        val table = buildCatalogBasedSparkTable(tableDir.toURI, new JHashMap[String, String]())

        val plan = buildStreamingRelationV2(
          table, Map(DeltaOptions.SCHEMA_TRACKING_LOCATION_ALIAS -> schemaLogPath))
        val result = applyReadOptions(plan)
        assertV2(result)
        val rebuiltTable = result.asInstanceOf[StreamingRelationV2].table.asInstanceOf[SparkTable]

        assert(rebuiltTable.getOptions.containsKey(DeltaOptions.SCHEMA_TRACKING_LOCATION_ALIAS))
        assert(rebuiltTable.getOptions.get(DeltaOptions.SCHEMA_TRACKING_LOCATION_ALIAS) ==
          schemaLogPath)
        assertSchemaMatchesSeededLogEntry(rebuiltTable)
      }
    }
  }

  test("schema-tracking rebuild: skipped when extraOptions has no schema-tracking option") {
    withTempDir { tableDir =>
      val tablePath = tableDir.getCanonicalPath
      createDeltaTable(tablePath)
      val table = buildCatalogBasedSparkTable(tableDir.toURI, new JHashMap[String, String]())

      val plan = buildStreamingRelationV2(table, Map.empty)
      val result = applyReadOptions(plan)
      assert(result eq plan, "no rebuild expected when schema-tracking option not present")
    }
  }

  test("schema-tracking rebuild: skipped when SparkTable already carries the " +
      "schema-tracking option") {
    withTempDir { tableDir =>
      withTempDir { schemaLogDir =>
        val tablePath = tableDir.getCanonicalPath
        createDeltaTable(tablePath)
        val schemaLogPath = schemaLogDir.getCanonicalPath
        val tableOptions = new JHashMap[String, String]()
        tableOptions.put(DeltaOptions.SCHEMA_TRACKING_LOCATION, schemaLogPath)
        val table = buildCatalogBasedSparkTable(tableDir.toURI, tableOptions)
        assert(table.getOptions.containsKey(DeltaOptions.SCHEMA_TRACKING_LOCATION))

        val plan = buildStreamingRelationV2(
          table, Map(DeltaOptions.SCHEMA_TRACKING_LOCATION -> schemaLogPath))
        val result = applyReadOptions(plan)
        assert(result eq plan, "no rebuild expected when table already carries the option")
      }
    }
  }

  test("schema-tracking via V1 StreamingRelation: option propagates through V1 -> V2 conversion") {
    // Counterpart to the V2 rebuild tests above: those start from StreamingRelationV2 and exercise
    // the rebuild branch. This test starts from a V1 StreamingRelation carrying the schema-tracking
    // option in dataSource.options, and verifies the V1 -> V2 conversion branch hands the option to
    // the new SparkTable so its schema is driven by the persisted log entry.
    withTempDir { tableDir =>
      withTempDir { schemaLogDir =>
        val tablePath = tableDir.getCanonicalPath
        createDeltaTable(tablePath)
        val schemaLogPath = schemaLogDir.getCanonicalPath
        seedSchemaLogWithExtraColumn(tablePath, schemaLogPath)

        val catalogTable = createCatalogTable(tableDir.toURI, ucManaged = false)
        val dataSource = DataSource(
          sparkSession = spark,
          userSpecifiedSchema = None,
          className = "delta",
          options = Map(
            "path" -> tablePath,
            DeltaOptions.SCHEMA_TRACKING_LOCATION -> schemaLogPath),
          catalogTable = Some(catalogTable))
        val plan = StreamingRelation(dataSource)

        // STRICT mode forces V1 -> V2 conversion in ApplyV2Streaming.
        withSQLConf(DeltaSQLConf.V2_ENABLE_MODE.key -> "STRICT") {
          val result = applyRules(plan)
          assertV2(result)
          val convertedTable =
            result.asInstanceOf[StreamingRelationV2].table.asInstanceOf[SparkTable]
          assert(convertedTable.getOptions.containsKey(DeltaOptions.SCHEMA_TRACKING_LOCATION))
          assert(convertedTable.getOptions.get(DeltaOptions.SCHEMA_TRACKING_LOCATION) ==
            schemaLogPath)
          // Schema is driven by the seeded log entry, not the underlying snapshot.
          assertSchemaMatchesSeededLogEntry(convertedTable)
          assert(result.output.map(_.name) == seededFieldNames)
        }
      }
    }
  }

  test("CDC + schema-tracking co-occurrence: single rebuild applies both transformations") {
    withTempDir { tableDir =>
      withTempDir { schemaLogDir =>
        val tablePath = tableDir.getCanonicalPath
        createDeltaTable(tablePath)
        val schemaLogPath = schemaLogDir.getCanonicalPath
        seedSchemaLogWithExtraColumn(tablePath, schemaLogPath)

        val table = buildCatalogBasedSparkTable(tableDir.toURI, new JHashMap[String, String]())
        val plan = buildStreamingRelationV2(table, Map(
          DeltaOptions.SCHEMA_TRACKING_LOCATION -> schemaLogPath,
          DeltaOptions.CDC_READ_OPTION -> "true"))

        val result = applyReadOptions(plan)
        assertV2(result)
        val rebuilt = result.asInstanceOf[StreamingRelationV2].table.asInstanceOf[SparkTable]

        // Both options land on the rebuilt table in a single pass.
        assert(rebuilt.getOptions.get(DeltaOptions.SCHEMA_TRACKING_LOCATION) == schemaLogPath)
        assert(rebuilt.getOptions.get(DeltaOptions.CDC_READ_OPTION) == "true")
        // Schema reflects both transformations: seeded log columns + appended CDC columns.
        val expected = Seq(
          "id", "extra",
          CDCReader.CDC_TYPE_COLUMN_NAME,
          CDCReader.CDC_COMMIT_VERSION,
          CDCReader.CDC_COMMIT_TIMESTAMP)
        assert(result.output.map(_.name) == expected,
          s"Expected $expected, got: ${result.output.map(_.name)}")

        // Idempotent: a second pass leaves the rebuilt table in place.
        val reapplied = applyReadOptions(result).asInstanceOf[StreamingRelationV2]
        assert(reapplied.table eq rebuilt, "re-applying rule should not rebuild")
      }
    }
  }
}
