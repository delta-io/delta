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

package org.apache.spark.sql.delta.hudi

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.engine.HoodieLocalEngineContext
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodieBaseFile
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.metadata.HoodieMetadataFileSystemView
import org.apache.hudi.storage.StorageConfiguration
import org.apache.hudi.storage.hadoop.{HadoopStorageConfiguration, HoodieHadoopStorage}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{QueryTest, SparkSession}
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.delta.DeltaOperations.Truncate
import org.apache.spark.sql.delta.{DeltaConfigs, DeltaLog, DeltaUnsupportedOperationException, OptimisticTransaction}
import org.apache.spark.sql.delta.actions.{Action, AddFile, Metadata, RemoveFile}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.{ManualClock, Utils}
import org.scalatest.concurrent.Eventually
import org.scalatest.time.SpanSugar._

import java.io.File
import java.time.Instant
import java.util.UUID
import java.util.stream.Collectors
import scala.collection.JavaConverters

class ConvertToHudiSuite extends QueryTest with Eventually {

  private var _sparkSession: SparkSession = null
  private var TMP_DIR: String = ""
  private var testTableName: String = ""
  private var testTablePath: String = ""

  override def spark: SparkSession = _sparkSession

  override def beforeAll(): Unit = {
    super.beforeAll()
    _sparkSession = createSparkSession()
    _sparkSession.conf.set(
      DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.defaultTablePropertyKey, "true")
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    TMP_DIR = Utils.createTempDir().getCanonicalPath
    testTableName = UUID.randomUUID().toString.replace("-", "_")
    testTablePath = s"$TMP_DIR/$testTableName"
  }

  override def afterEach(): Unit = {
    super.afterEach()
    _sparkSession.sql(s"DROP TABLE IF EXISTS $testTableName")
    Utils.deleteRecursively(new File(testTablePath))
  }

  override def afterAll(): Unit = {
    super.afterAll()
    SparkContext.getActive.foreach(_.stop())
  }

  test("basic test - managed table created with SQL") {
    _sparkSession.sql(
      s"""CREATE TABLE `$testTableName` (col1 INT) USING DELTA
         |LOCATION '$testTablePath'
         |TBLPROPERTIES (
         |  'delta.universalFormat.enabledFormats' = 'hudi'
         |)""".stripMargin)
    _sparkSession.sql(s"INSERT INTO `$testTableName` VALUES (123)")
    verifyFilesAndSchemaMatch()
  }

  test("basic test - catalog table created with DataFrame") {
    withDefaultTablePropsInSQLConf(false, {
      _sparkSession.range(10).write.format("delta")
        .option("path", testTablePath)
        .saveAsTable(testTableName)
    })
    verifyFilesAndSchemaMatch()

    withDefaultTablePropsInSQLConf(false, {
      _sparkSession.range(10, 20, 1)
        .write.format("delta").mode("append")
        .save(testTablePath)
    })
    verifyFilesAndSchemaMatch()
  }

  for (isPartitioned <- Seq(true, false)) {
    test(s"validate multiple commits (partitioned = $isPartitioned)") {
      _sparkSession.sql(
        s"""CREATE TABLE `$testTableName` (col1 INT, col2 STRING, col3 STRING) USING DELTA
         |${if (isPartitioned) "PARTITIONED BY (col3)" else ""}
         |LOCATION '$testTablePath'
         |TBLPROPERTIES (
         |  'delta.universalFormat.enabledFormats' = 'hudi',
         |  'delta.enableDeletionVectors' = false
         |)""".stripMargin)
      // perform some inserts
      _sparkSession.sql(
        s"INSERT INTO `$testTableName` VALUES (1, 'instant1', 'a'), (2, 'instant1', 'a')")
      verifyFilesAndSchemaMatch()

      _sparkSession.sql(
        s"INSERT INTO `$testTableName` VALUES (3, 'instant2', 'b'), (4, 'instant2', 'b')")
      verifyFilesAndSchemaMatch()

      _sparkSession.sql(
        s"INSERT INTO `$testTableName` VALUES (5, 'instant3', 'b'), (6, 'instant3', 'a')")
      verifyFilesAndSchemaMatch()

      // update the data from the first instant
      _sparkSession.sql(s"UPDATE `$testTableName` SET col2 = 'instant4' WHERE col2 = 'instant1'")
      verifyFilesAndSchemaMatch()

      // delete a single row
      _sparkSession.sql(s"DELETE FROM `$testTableName` WHERE col1 = 5")
      verifyFilesAndSchemaMatch()
    }
  }

  test("Enabling Delete Vector Throws Exception") {
    intercept[DeltaUnsupportedOperationException] {
      _sparkSession.sql(
        s"""CREATE TABLE `$testTableName` (col1 INT, col2 STRING) USING DELTA
           |LOCATION '$testTablePath'
           |TBLPROPERTIES (
           |  'delta.universalFormat.enabledFormats' = 'hudi',
           |  'delta.enableDeletionVectors' = true
           |)""".stripMargin)
    }
  }

  for (invalidFieldDef <- Seq("col3 ARRAY<STRING>", "col3 MAP<STRING, STRING>")) {
    test(s"Table Throws Exception for Unsupported Type ($invalidFieldDef)") {
      intercept[DeltaUnsupportedOperationException] {
        _sparkSession.sql(
          s"""CREATE TABLE `$testTableName` (col1 INT, col2 STRING, $invalidFieldDef) USING DELTA
             |LOCATION '$testTablePath'
             |TBLPROPERTIES (
             |  'delta.universalFormat.enabledFormats' = 'hudi',
             |  'delta.enableDeletionVectors' = false
             |)""".stripMargin)
      }
    }
  }

  test("validate Hudi timeline archival and cleaning") {
    val testOp = Truncate()
    withDefaultTablePropsInSQLConf(true, {
      val startTime = System.currentTimeMillis() - 12 * 24 * 60 * 60 * 1000
      val clock = new ManualClock(startTime)
      val actualTestStartTime = System.currentTimeMillis()
      val log = DeltaLog.forTable(_sparkSession, new Path(testTablePath), clock)
      (1 to 20).foreach { i =>
        val txn = if (i == 1) startTxnWithManualLogCleanup(log) else log.startTransaction()
        val file = AddFile(i.toString + ".parquet", Map.empty, 1, 1, true) :: Nil
        val delete: Seq[Action] = if (i > 1) {
          val timestamp = startTime + (System.currentTimeMillis() - actualTestStartTime)
          RemoveFile((i - 1).toString + ".parquet", Some(timestamp), true) :: Nil
        } else {
          Nil
        }
        txn.commit(delete ++ file, testOp)
        clock.advance(12.hours.toMillis)
        // wait for each Hudi sync to complete
        verifyNumHudiCommits(i)
      }

      val metaClient: HoodieTableMetaClient = HoodieTableMetaClient.builder
        .setConf(new HadoopStorageConfiguration(log.newDeltaHadoopConf()))
        .setBasePath(log.dataPath.toString)
        .setLoadActiveTimelineOnLoad(true)
        .build
      // Timeline requires a clean commit for proper removal of entries from the Hudi Metadata Table
      assert(metaClient.getActiveTimeline.getCleanerTimeline.countInstants() == 1,
        "Cleaner timeline should have 1 instant")
      // Older commits should move from active to archive timeline
      assert(metaClient.getArchivedTimeline.getCommitsTimeline.filterInflights.countInstants == 2,
        "Archived timeline should have 2 instants")
    })
  }

  test("validate various data types") {
    _sparkSession.sql(
      s"""CREATE TABLE `$testTableName` (col1 BIGINT, col2 BOOLEAN, col3 DATE,
         | col4 DOUBLE, col5 FLOAT, col6 INT, col7 STRING, col8 TIMESTAMP)
         | USING DELTA
         |LOCATION '$testTablePath'
         |TBLPROPERTIES (
         |  'delta.universalFormat.enabledFormats' = 'hudi'
         |)""".stripMargin)
    val nowSeconds = Instant.now().getEpochSecond
    _sparkSession.sql(s"INSERT INTO `$testTableName` VALUES (123, true, "
      + s"date(from_unixtime($nowSeconds)), 32.1, 1.23, 456, 'hello world', "
      + s"timestamp(from_unixtime($nowSeconds)))")
    verifyFilesAndSchemaMatch()
  }

  def buildHudiMetaClient(): HoodieTableMetaClient = {
    val hadoopConf: Configuration = _sparkSession.sparkContext.hadoopConfiguration
    val storageConf : StorageConfiguration[_] = new HadoopStorageConfiguration(hadoopConf)
    HoodieTableMetaClient.builder
      .setConf(storageConf).setBasePath(testTablePath)
      .setLoadActiveTimelineOnLoad(true)
      .build
  }

  def verifyNumHudiCommits(count: Integer): Unit = {
    eventually(timeout(30.seconds)) {
      val metaClient: HoodieTableMetaClient = buildHudiMetaClient()
      val activeCommits = metaClient.getActiveTimeline.getCommitsTimeline
        .filterCompletedInstants.countInstants
      val archivedCommits = metaClient.getArchivedTimeline.getCommitsTimeline
        .filterCompletedInstants.countInstants
      assert(activeCommits + archivedCommits == count)
    }
  }

  def verifyFilesAndSchemaMatch(): Unit = {
    eventually(timeout(30.seconds)) {
      // To avoid requiring Hudi spark dependencies, we first lookup the active base files and then
      // assert by reading those active base files (parquet) directly
      val hadoopConf: Configuration = _sparkSession.sparkContext.hadoopConfiguration
      val storageConf : StorageConfiguration[_] = new HadoopStorageConfiguration(hadoopConf)
      val metaClient: HoodieTableMetaClient = buildHudiMetaClient()
      val engContext: HoodieLocalEngineContext = new HoodieLocalEngineContext(storageConf)
      val fsView: HoodieMetadataFileSystemView = new HoodieMetadataFileSystemView(engContext,
        metaClient, metaClient.getActiveTimeline.getCommitsTimeline.filterCompletedInstants,
        HoodieMetadataConfig.newBuilder.enable(true).build)
      val hoodieStorage = new HoodieHadoopStorage(testTablePath, storageConf)
      val paths = JavaConverters.asScalaBuffer(
        FSUtils.getAllPartitionPaths(engContext, hoodieStorage, testTablePath, true, false))
        .flatMap(partition => JavaConverters.asScalaBuffer(fsView.getLatestBaseFiles(partition)
          .collect(Collectors.toList[HoodieBaseFile])))
        .map(baseFile => baseFile.getPath).sorted
      val avroSchema = new TableSchemaResolver(metaClient).getTableAvroSchema
      val hudiSchemaAsStruct = SchemaConverters.toSqlType(avroSchema).dataType
        .asInstanceOf[StructType]

      val deltaDF = _sparkSession.sql(s"SELECT * FROM $testTableName")
      // Assert file paths are equivalent
      val expectedFiles = deltaDF.inputFiles.map(path => path.substring(5)).toSeq.sorted
      assert(paths.equals(expectedFiles),
        s"Files do not match.\nExpected: $expectedFiles\nActual: $paths")
      // Assert schemas are equal
      val expectedSchema = deltaDF.schema
      assert(hudiSchemaAsStruct.equals(expectedSchema),
        s"Schemas do not match.\nExpected: $expectedSchema\nActual: $hudiSchemaAsStruct")
    }
  }

  def withDefaultTablePropsInSQLConf(enableInCommitTimestamp: Boolean, f: => Unit): Unit = {
    withSQLConf(
      DeltaConfigs.COLUMN_MAPPING_MODE.defaultTablePropertyKey -> "name",
      DeltaConfigs.UNIVERSAL_FORMAT_ENABLED_FORMATS.defaultTablePropertyKey -> "hudi",
      DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.defaultTablePropertyKey ->
        enableInCommitTimestamp.toString
    ) { f }
  }

  protected def startTxnWithManualLogCleanup(log: DeltaLog): OptimisticTransaction = {
    val txn = log.startTransaction()
    // This will pick up `spark.databricks.delta.properties.defaults.enableExpiredLogCleanup` to
    // disable log cleanup.
    txn.updateMetadata(Metadata())
    txn
  }

  def createSparkSession(): SparkSession = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    SparkSession.builder()
      .master("local[*]")
      .appName("UniformSession")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
  }
}
