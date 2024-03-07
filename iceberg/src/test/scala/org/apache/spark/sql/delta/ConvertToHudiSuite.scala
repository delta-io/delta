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

import org.apache.commons.lang3.time.DateUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.engine.HoodieLocalEngineContext
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodieBaseFile
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.metadata.HoodieMetadataFileSystemView
import org.apache.spark.SparkContext
import org.apache.spark.sql.{QueryTest, SparkSession}
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.catalyst.util.IntervalUtils
import org.apache.spark.sql.delta.DeltaOperations.Truncate
import org.apache.spark.sql.delta.actions.{Action, AddFile, Metadata, RemoveFile}
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.{ManualClock, Utils}
import org.scalatest.concurrent.Eventually
import org.scalatest.time.SpanSugar._

import java.io.File
import java.time.Instant
import java.util.{Calendar, TimeZone}
import java.util.stream.Collectors
import scala.collection.JavaConverters

class ConvertToHudiSuite extends QueryTest with Eventually {

  private var _sparkSession: SparkSession = null
  private val TMP_DIR = Utils.createTempDir().getCanonicalPath
  private val testTableName: String = "deltatable"
  private val testTablePath: String = s"$TMP_DIR/$testTableName"

  override def spark: SparkSession = _sparkSession

  override def beforeAll(): Unit = {
    super.beforeAll()
    _sparkSession = createSparkSession()
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
    withDefaultTablePropsInSQLConf {
      _sparkSession.range(10).write.format("delta")
        .option("path", testTablePath)
        .saveAsTable(testTableName)
    }
    verifyFilesAndSchemaMatch()

    withDefaultTablePropsInSQLConf {
      _sparkSession.range(10, 20, 1)
        .write.format("delta").mode("append")
        .save(testTablePath)
    }
    verifyFilesAndSchemaMatch()
  }

  test("validate multiple commits (non-partitioned)") {
    _sparkSession.sql(
      s"""CREATE TABLE `$testTableName` (col1 INT, col2 STRING) USING DELTA
       |LOCATION '$testTablePath'
       |TBLPROPERTIES (
       |  'delta.universalFormat.enabledFormats' = 'hudi',
       |  'delta.enableDeletionVectors' = false
       |)""".stripMargin)
    // perform some inserts
    _sparkSession.sql(s"INSERT INTO `$testTableName` VALUES (1, 'instant1'), (2, 'instant1')")
    verifyFilesAndSchemaMatch()
    
    _sparkSession.sql(s"INSERT INTO `$testTableName` VALUES (3, 'instant2'), (4, 'instant2')")
    verifyFilesAndSchemaMatch()

    _sparkSession.sql(s"INSERT INTO `$testTableName` VALUES (5, 'instant3'), (6, 'instant3')")
    verifyFilesAndSchemaMatch()

    // update the data from the first instant
    _sparkSession.sql(s"UPDATE `$testTableName` SET col2 = 'instant4' WHERE col2 = 'instant1'")
    verifyFilesAndSchemaMatch()

    // delete a single row
    _sparkSession.sql(s"DELETE FROM `$testTableName` WHERE col1 = 5")
    verifyFilesAndSchemaMatch()
  }

  test("validate multiple commits (partitioned)") {
    _sparkSession.sql(
      s"""CREATE TABLE `$testTableName` (col1 INT, col2 STRING, col3 STRING) USING DELTA
         |PARTITIONED BY (col3)
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

  test("Enabling Delete Vector fails") {
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

  test("validate Hudi timeline archival and cleaning") {
    val testOp = Truncate()
    withDefaultTablePropsInSQLConf {
      val startTime = System.currentTimeMillis() - 8 * 24 * 60 * 60 * 1000
      val clock = new ManualClock(startTime)
      val actualTestStartTime = System.currentTimeMillis()
      val log = DeltaLog.forTable(_sparkSession, new Path(testTablePath), clock)
      (1 to 12).foreach { i =>
        val txn = if (i == 1) startTxnWithManualLogCleanup(log) else log.startTransaction()
        val file = AddFile(i.toString, Map.empty, 1, 1, true) :: Nil
        val delete: Seq[Action] = if (i > 1) {
          val timestamp = startTime + (System.currentTimeMillis() - actualTestStartTime)
          RemoveFile(i - 1 toString, Some(timestamp), true) :: Nil
        } else {
          Nil
        }
        txn.commit(delete ++ file, testOp)
        // wait for each Hudi sync to complete
        verifyNumHudiCommits(i)
      }

      val metaClient: HoodieTableMetaClient = HoodieTableMetaClient.builder
        .setConf(log.newDeltaHadoopConf()).setBasePath(log.dataPath.toString)
        .setLoadActiveTimelineOnLoad(true)
        .build
      clock.advance(intervalStringToMillis(DeltaConfigs.LOG_RETENTION.defaultValue) +
        intervalStringToMillis("interval 8 day"))

      val txn = log.startTransaction()
      txn.commit(AddFile("13", Map.empty, 1, 1, true) :: Nil, testOp)
      verifyNumHudiCommits(13)
    }
  }

  // TODO find best ways to reuse helpers
  protected def startTxnWithManualLogCleanup(log: DeltaLog): OptimisticTransaction = {
    val txn = log.startTransaction()
    // This will pick up `spark.databricks.delta.properties.defaults.enableExpiredLogCleanup` to
    // disable log cleanup.
    txn.updateMetadata(Metadata())
    txn
  }

  protected def intervalStringToMillis(str: String): Long = {
    DeltaConfigs.getMilliSeconds(
      IntervalUtils.safeStringToInterval(UTF8String.fromString(str)))
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
    HoodieTableMetaClient.builder
      .setConf(hadoopConf).setBasePath(testTablePath)
      .setLoadActiveTimelineOnLoad(true)
      .build
  }

  def verifyNumHudiCommits(count: Integer): Unit = {
    eventually(timeout(600.seconds)) {
      val metaClient: HoodieTableMetaClient = buildHudiMetaClient()
      val commits = metaClient.getActiveTimeline.filterCompletedInstants
      assert(commits.countInstants() == count)
    }
  }

  def verifyFilesAndSchemaMatch(): Unit = {
    eventually(timeout(10.seconds)) {
      // To avoid requiring Hudi spark dependencies, we first lookup the active base files and then
      // assert by reading those active base files (parquet) directly
      val hadoopConf: Configuration = _sparkSession.sparkContext.hadoopConfiguration
      val metaClient: HoodieTableMetaClient = buildHudiMetaClient()
      val engContext: HoodieLocalEngineContext = new HoodieLocalEngineContext(hadoopConf)
      val fsView: HoodieMetadataFileSystemView = new HoodieMetadataFileSystemView(engContext,
        metaClient, metaClient.getActiveTimeline.getCommitsTimeline.filterCompletedInstants,
        HoodieMetadataConfig.newBuilder.enable(true).build)
      val paths = JavaConverters.asScalaBuffer(
        FSUtils.getAllPartitionPaths(engContext, testTablePath, true, false))
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

  def withDefaultTablePropsInSQLConf(f: => Unit): Unit = {
    withSQLConf(
      DeltaConfigs.COLUMN_MAPPING_MODE.defaultTablePropertyKey -> "name",
      DeltaConfigs.UNIVERSAL_FORMAT_ENABLED_FORMATS.defaultTablePropertyKey -> "hudi"
    ) { f }
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
