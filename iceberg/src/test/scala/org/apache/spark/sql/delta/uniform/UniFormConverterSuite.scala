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

package org.apache.spark.sql.delta.uniform

import com.databricks.spark.util.Log4jUsageLogger
import shadedForDelta.org.apache.iceberg.hadoop.HadoopTables

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.delta.{CurrentTransactionInfo, DeltaLog, DeltaOperations, DeltaTableReadPredicate, IcebergConstants, Snapshot}
import org.apache.spark.sql.delta.DeltaTestUtils.filterUsageRecords
import org.apache.spark.sql.delta.NonSparkReadIceberg
import org.apache.spark.sql.delta.actions.{Action, AddFile, CommitInfo, DomainMetadata, Metadata}
import org.apache.spark.sql.delta.icebergShaded.{IcebergConverter, UNIFORM_CC_MODE, UNIFORM_POST_COMMIT_MODE}
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.spark.sql.test.SharedSparkSession

class IcebergConverterForTest extends IcebergConverter {
  def convertSnapshotAndReturnMetadataPath(
      snapshotToConvert: Snapshot,
      catalogTable: CatalogTable): String = {
    val icebergTxn = convertSnapshotInternal(
      snapshotToConvert,
      readSnapshotOpt = None,
      lastConvertedInfo = LastConvertedIcebergInfo(
        icebergTable = None,
        icebergSnapshotId = None,
        deltaVersionConverted = None,
        baseMetadataLocationOpt = None
      ),
      conversionContext = new ConversionContext(
        conversionMode = UNIFORM_POST_COMMIT_MODE,
        additionalDeltaActionsToCommit = None,
        opType = "delta.iceberg.conversion.convertSnapshot"
      ),
      catalogTable
    )
    icebergTxn.getConvertedIcebergMetadata._1
  }
}

class UniFormConverterSuite extends
  QueryTest with SharedSparkSession with DeltaSQLCommandTest with NonSparkReadIceberg {
  def constructDummyAddFile(path: String = "s3://path1/1.parquet"): AddFile = {
    AddFile(
      path = path,
      dataChange = true,
      partitionValues = Map.empty[String, String],
      size = 100,
      modificationTime = System.currentTimeMillis(),
      stats = """{"numRecords":10}"""
    )
  }

  def constructDummyTxnInfo(
      version: Long,
      readSnapshot: Snapshot,
      newActions: Seq[Action],
      catalogTable: CatalogTable,
      newMetadata: Metadata): CurrentTransactionInfo = {
    new CurrentTransactionInfo(
      txnId = s"test-txn-001",
      readPredicates = Vector.empty,
      readFiles = Set.empty,
      readWholeTable = false,
      readAppIds = Set.empty,
      metadata = newMetadata,
      protocol = readSnapshot.protocol,
      actions = newActions,
      readSnapshot = readSnapshot,
      commitInfo = Some(CommitInfo.empty(Some(version))),
      readRowIdHighWatermark = 0L,
      catalogTable = Some(catalogTable),
      domainMetadata = Seq.empty,
      op = DeltaOperations.Write(mode = org.apache.spark.sql.SaveMode.Append)
    )
  }

  def catalogTableWithDeltaUniformIceberg(
      catalogTable: CatalogTable,
      metadataPath: String,
      convertedDeltaVersion: Long): CatalogTable =
    catalogTable.copy(
      properties = catalogTable.properties +
        (IcebergConstants.CATALOG_TABLE_ICEBERG_METADATA_LOCATION_PROP -> metadataPath) +
        (IcebergConstants.CATALOG_TABLE_ICEBERG_CONVERTED_DELTA_VERSION_PROP ->
          convertedDeltaVersion.toString)
    )

  def assertDeltaCommitRangeEvent(
      events: Seq[com.databricks.spark.util.UsageRecord],
      expectedFromVersion: Long,
      expectedToVersion: Long): Unit = {
    val rangeEvents = filterUsageRecords(events, "delta.iceberg.conversion.deltaCommitRange")
    assert(rangeEvents.nonEmpty, "Expected deltaCommitRange event for incremental conversion")
    val eventData = JsonUtils.fromJson[Map[String, Any]](rangeEvents.head.blob)
    assert(eventData("fromVersion") === expectedFromVersion)
    assert(eventData("toVersion") === expectedToVersion)
  }

  test("Verify convertSnapshot writes Iceberg metadata") {
    val tableName = "test_iceberg_converter"
    withTable(tableName) {
      spark.sql(
        s"""CREATE TABLE $tableName (id INT, name STRING) USING DELTA
           |TBLPROPERTIES (
           |  'delta.columnMapping.mode' = 'name',
           |  'delta.enableIcebergCompatV2' = 'true',
           |  'delta.universalFormat.enabledFormats' = 'iceberg'
           |)""".stripMargin)
      // Do write
      spark.sql(s"INSERT INTO $tableName VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")
      val tableId = TableIdentifier(tableName)
      val deltaLog = DeltaLog.forTable(spark, tableId)
      val snapshot = deltaLog.update()
      val catalogTable = spark.sessionState.catalog.getTableMetadata(tableId)
      // Trigger conversion
      val converter = new IcebergConverterForTest()
      val metadataPath = converter.convertSnapshotAndReturnMetadataPath(snapshot, catalogTable)
      // Check match
      val icebergTable =
        new HadoopTables(deltaLog.newDeltaHadoopConf()).load(metadataPath)
      val numFilesInIceberg =
        icebergTable.currentSnapshot().summary().get("total-data-files").toInt
      assert(
        numFilesInIceberg == snapshot.numOfFiles,
        s"Iceberg total-data-files ($numFilesInIceberg) must equal " +
          s"Delta numOfFiles (${snapshot.numOfFiles})")
      verifyReadByPath(metadataPath,
        snapshot.schema, fields = "id", orderBy = "id", Seq(Row(1), Row(2), Row(3))
      )
    }
  }

  test("Verify convertSnapshot after multiple inserts") {
    val tableName = "test_iceberg_converter_multi"
    withTable(tableName) {
      spark.sql(
        s"""CREATE TABLE $tableName (id INT) USING DELTA
           |TBLPROPERTIES (
           |  'delta.columnMapping.mode' = 'name',
           |  'delta.enableIcebergCompatV2' = 'true',
           |  'delta.universalFormat.enabledFormats' = 'iceberg'
           |)""".stripMargin)
      // Do some writes
      spark.sql(s"INSERT INTO $tableName VALUES (1)")
      spark.sql(s"INSERT INTO $tableName VALUES (2)")
      spark.sql(s"INSERT INTO $tableName VALUES (3)")
      val tableId = TableIdentifier(tableName)
      val deltaLog = DeltaLog.forTable(spark, tableId)
      val snapshot = deltaLog.update()
      val catalogTable = spark.sessionState.catalog.getTableMetadata(tableId)
      assert(snapshot.numOfFiles == 3)
      // Trigger conversion
      val converter = new IcebergConverterForTest()
      val metadataPath = converter.convertSnapshotAndReturnMetadataPath(snapshot, catalogTable)
      // Check match
      val icebergTable = new HadoopTables(deltaLog.newDeltaHadoopConf()).load(metadataPath)
      val numFilesInIceberg =
        icebergTable.currentSnapshot().summary().get("total-data-files").toInt
      assert(
        numFilesInIceberg == snapshot.numOfFiles,
        s"Iceberg total-data-files ($numFilesInIceberg) must equal " +
          s"Delta numOfFiles (${snapshot.numOfFiles})")
      verifyReadByPath(metadataPath,
        snapshot.schema, fields = "id", orderBy = "id", Seq(Row(1), Row(2), Row(3))
      )
    }
  }

  test("Field ID consistency for CREATE_TABLE with nested schema and partition") {
    val tableName = "test_field_id_nested_v2"
    withTable(tableName) {
      // Iceberg CREATE_TABLE reassigns the field id in schema, which
      // is not consistent with Delta in edge cases (For nested schemas)
      // This test checks that UniForm conversion handles this case well so that
      // converted Iceberg is utilizing the Delta assigned field for schema
      spark.sql(
        s"""CREATE TABLE $tableName
           |(col1 INT,
           | col2 STRUCT<f1: STRUCT<f2: INT, f3: STRUCT<f4: INT, f5: INT>, f6: INT>, f7: INT>,
           | col3 INT)
           |USING DELTA
           |PARTITIONED BY (col3)
           |TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
           |""".stripMargin)

      import org.apache.spark.sql.types._
      val schema = StructType(Seq(
        StructField("col1", IntegerType),
        StructField("col2", StructType(Seq(
          StructField("f1", StructType(Seq(
            StructField("f2", IntegerType),
            StructField("f3", StructType(Seq(
              StructField("f4", IntegerType),
              StructField("f5", IntegerType)))),
            StructField("f6", IntegerType)))),
          StructField("f7", IntegerType)))),
        StructField("col3", IntegerType)))

      val data = Seq(Row(1, Row(Row(2, Row(3, 4), 5), 6), 7))
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
        .write.format("delta").mode("append").saveAsTable(tableName)
      spark.sql(
        s"""ALTER TABLE $tableName SET TBLPROPERTIES (
           |  'delta.enableIcebergCompatV2' = 'true',
           |  'delta.universalFormat.enabledFormats' = 'iceberg'
           |)""".stripMargin)

      val tableId = TableIdentifier(tableName)
      val deltaLog = DeltaLog.forTable(spark, tableId)
      val snapshot = deltaLog.update()
      val catalogTable = spark.sessionState.catalog.getTableMetadata(tableId)
      // Trigger an Iceberg full conversion
      val converter = new IcebergConverterForTest()
      val metadataPath = converter.convertSnapshotAndReturnMetadataPath(snapshot, catalogTable)

      // Without the special fix, Iceberg reassigns field IDs during CREATE_TABLE
      // so the read would fail
      verifyReadByPath(metadataPath, schema, fields = "col1, col2, col3", orderBy = "col1", data)
    }
  }


  test("IcebergConverter convertUncommitedTxn - initial conversion") {
    val tableName = "test_table_1"
    withTable(tableName) {
      spark.sql(
        s"""CREATE TABLE $tableName (id INT) USING DELTA
           |TBLPROPERTIES (
           |  'delta.columnMapping.mode' = 'name',
           |  'delta.enableIcebergCompatV2' = 'true',
           |  'delta.universalFormat.enabledFormats' = 'iceberg'
           |)""".stripMargin)
      // Do some writes
      spark.sql(s"INSERT INTO $tableName VALUES (1)")
      spark.sql(s"INSERT INTO $tableName VALUES (2)")
      spark.sql(s"INSERT INTO $tableName VALUES (3)")
      val tableId = TableIdentifier(tableName)
      val deltaLog = DeltaLog.forTable(spark, tableId)
      val snapshot = deltaLog.update()
      val catalogTable = spark.sessionState.catalog.getTableMetadata(tableId)
      assert(snapshot.numOfFiles == 3)

      // Pretend to do an un-commited Txn
      val currSnapshot = deltaLog.update()
      val attemptDeltaVersion = currSnapshot.version + 1
      val txnInfo = constructDummyTxnInfo(
        version = attemptDeltaVersion,
        readSnapshot = currSnapshot,
        newActions = Seq(constructDummyAddFile()),
        catalogTable = catalogTable,
        newMetadata = currSnapshot.metadata
      )
      // Validate on Iceberg conversion
      val converter = new IcebergConverter()
      val (metadataPath, lastConvertedVersion) =
        converter.convertUncommitedTxn(txnInfo, attemptDeltaVersion, deltaLog, catalogTable)
      val icebergTable = new HadoopTables(deltaLog.newDeltaHadoopConf()).load(metadataPath)
      val numFilesInIceberg = icebergTable.currentSnapshot().summary().get("total-data-files").toInt
      assert(numFilesInIceberg === currSnapshot.numOfFiles + 1)
      assert(lastConvertedVersion.isEmpty)
    }
  }

  test("IcebergConverter convertUncommitedTxn - initial conversion with conflict resolution") {
    val tableName = "test_table_2"
    withTable(tableName) {
      spark.sql(
        s"""CREATE TABLE $tableName (id INT) USING DELTA
           |TBLPROPERTIES (
           |  'delta.columnMapping.mode' = 'name',
           |  'delta.enableIcebergCompatV2' = 'true',
           |  'delta.universalFormat.enabledFormats' = 'iceberg'
           |)""".stripMargin)
      // Do some writes
      spark.sql(s"INSERT INTO $tableName VALUES (1)")
      spark.sql(s"INSERT INTO $tableName VALUES (2)")
      spark.sql(s"INSERT INTO $tableName VALUES (3)")
      val tableId = TableIdentifier(tableName)
      val deltaLog = DeltaLog.forTable(spark, tableId)
      val snapshot = deltaLog.update()
      val catalogTable = spark.sessionState.catalog.getTableMetadata(tableId)
      assert(snapshot.numOfFiles == 3)

      // Pretend to do an un-commited Txn with conflict resolution so readSnapshot is stale
      val currSnapshot = deltaLog.update()
      val staleSnapshot = deltaLog.getSnapshotAt(1)
      val attemptDeltaVersion = currSnapshot.version + 1
      val txnInfo = constructDummyTxnInfo(
        version = attemptDeltaVersion,
        readSnapshot = staleSnapshot,
        newActions = Seq(
          constructDummyAddFile(path = "s3://path1/1.parquet"),
          constructDummyAddFile(path = "s3://path1/2.parquet")
        ),
        catalogTable = catalogTable,
        newMetadata = currSnapshot.metadata
      )
      // Validate on Iceberg conversion
      val converter = new IcebergConverter()
      val (metadataPath, lastConvertedVersion) =
        converter.convertUncommitedTxn(txnInfo, attemptDeltaVersion, deltaLog, catalogTable)
      val icebergTable = new HadoopTables(deltaLog.newDeltaHadoopConf()).load(metadataPath)
      val numFilesInIceberg = icebergTable.currentSnapshot().summary().get("total-data-files").toInt
      assert(numFilesInIceberg === currSnapshot.numOfFiles + 2)
      assert(lastConvertedVersion.isEmpty)
    }
  }

  test("IcebergConverter convertUncommitedTxn - incremental conversion") {
    val tableName = "test_incremental_conversion"
    withTable(tableName) {
      spark.sql(
        s"""CREATE TABLE $tableName (id INT) USING DELTA
           |TBLPROPERTIES (
           |  'delta.columnMapping.mode' = 'name',
           |  'delta.enableIcebergCompatV2' = 'true',
           |  'delta.universalFormat.enabledFormats' = 'iceberg'
           |)""".stripMargin)
      spark.sql(s"INSERT INTO $tableName VALUES (1)")
      spark.sql(s"INSERT INTO $tableName VALUES (2)")
      spark.sql(s"INSERT INTO $tableName VALUES (3)")
      val tableId = TableIdentifier(tableName)
      val deltaLog = DeltaLog.forTable(spark, tableId)
      val snapshot = deltaLog.update()
      val catalogTable = spark.sessionState.catalog.getTableMetadata(tableId)
      assert(snapshot.numOfFiles == 3)

      // Do a full conversion to get the initial Iceberg metadata
      val converter = new IcebergConverterForTest()
      val metadataPath = converter.convertSnapshotAndReturnMetadataPath(snapshot, catalogTable)

      // Do incremental conversion
      // Simulate the catalogTable having the deltaUniformIceberg properties set
      val catalogTableWithIcebergInfo =
        catalogTableWithDeltaUniformIceberg(catalogTable, metadataPath, snapshot.version)
      val attemptDeltaVersion = snapshot.version + 1
      val txnInfo = constructDummyTxnInfo(
        version = attemptDeltaVersion,
        readSnapshot = snapshot,
        newActions = Seq(constructDummyAddFile()),
        catalogTable = catalogTableWithIcebergInfo,
        newMetadata = snapshot.metadata
      )

      var newMetadataPath: String = null
      var lastConvertedVersion: Option[Long] = None
      val events = Log4jUsageLogger.track {
        val (path, version) =
          converter.convertUncommitedTxn(
            txnInfo, attemptDeltaVersion, deltaLog, catalogTableWithIcebergInfo)
        newMetadataPath = path
        lastConvertedVersion = version
      }

      // Verify incremental conversion
      assert(lastConvertedVersion === Some(snapshot.version))
      val icebergTable = new HadoopTables(deltaLog.newDeltaHadoopConf()).load(newMetadataPath)
      val numFilesInIceberg =
        icebergTable.currentSnapshot().summary().get("total-data-files").toInt
      assert(numFilesInIceberg === snapshot.numOfFiles + 1)
      assertDeltaCommitRangeEvent(events, snapshot.version + 1, attemptDeltaVersion)
    }
  }

  test("IcebergConverter convertUncommitedTxn - incremental conversion with conflict resolution") {
    val tableName = "test_incremental_conflict"
    withTable(tableName) {
      spark.sql(
        s"""CREATE TABLE $tableName (id INT) USING DELTA
           |TBLPROPERTIES (
           |  'delta.columnMapping.mode' = 'name',
           |  'delta.enableIcebergCompatV2' = 'true',
           |  'delta.universalFormat.enabledFormats' = 'iceberg'
           |)""".stripMargin)
      spark.sql(s"INSERT INTO $tableName VALUES (1)")
      spark.sql(s"INSERT INTO $tableName VALUES (2)")
      spark.sql(s"INSERT INTO $tableName VALUES (3)")
      val tableId = TableIdentifier(tableName)
      val deltaLog = DeltaLog.forTable(spark, tableId)
      val catalogTable = spark.sessionState.catalog.getTableMetadata(tableId)

      // Do Iceberg conversion for stale Snapshot
      val staleSnapshot = deltaLog.getSnapshotAt(1)
      val converter = new IcebergConverterForTest()
      val staleMetadataPath =
        converter.convertSnapshotAndReturnMetadataPath(staleSnapshot, catalogTable)
      val staleIcebergTable =
        new HadoopTables(deltaLog.newDeltaHadoopConf()).load(staleMetadataPath)
      assert(
        staleIcebergTable.currentSnapshot().summary().get("total-data-files").toInt === 1)

      // Do incremental conversion
      // Simulate the catalogTable having the deltaUniformIceberg properties set
      val catalogTableWithIcebergInfo = catalogTableWithDeltaUniformIceberg(
        catalogTable, staleMetadataPath, staleSnapshot.version)
      val currSnapshot = deltaLog.update()
      val attemptDeltaVersion = currSnapshot.version + 1
      val txnInfo = constructDummyTxnInfo(
        version = attemptDeltaVersion,
        readSnapshot = staleSnapshot,
        newActions = Seq(constructDummyAddFile()),
        catalogTable = catalogTableWithIcebergInfo,
        newMetadata = staleSnapshot.metadata
      )
      var newMetadataPath: String = null
      var lastConvertedVersion: Option[Long] = None
      val events = Log4jUsageLogger.track {
        val (path, version) =
          converter.convertUncommitedTxn(
            txnInfo, attemptDeltaVersion, deltaLog, catalogTableWithIcebergInfo)
        newMetadataPath = path
        lastConvertedVersion = version
      }

      // Verify incremental conversion
      assert(lastConvertedVersion === Some(staleSnapshot.version))
      val icebergTable = new HadoopTables(deltaLog.newDeltaHadoopConf()).load(newMetadataPath)
      val numFilesInIceberg =
        icebergTable.currentSnapshot().summary().get("total-data-files").toInt
      assert(numFilesInIceberg === currSnapshot.numOfFiles + 1)
      assertDeltaCommitRangeEvent(events, staleSnapshot.version + 1, attemptDeltaVersion)
    }
  }
}
