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

import java.time.Instant
import java.util.UUID
import java.util.stream.Collectors

import scala.collection.JavaConverters

import org.apache.spark.sql.delta.{DeltaConfigs, DeltaLog, DeltaUnsupportedOperationException, OptimisticTransaction}
import org.apache.spark.sql.delta.DeltaOperations.Truncate
import org.apache.spark.sql.delta.actions.{Action, AddFile, Metadata}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.concurrent.Eventually
import org.scalatest.time.SpanSugar._
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.engine.HoodieLocalEngineContext
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodieBaseFile
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.metadata.HoodieMetadataFileSystemView
import org.apache.hudi.storage.StorageConfiguration
import org.apache.hudi.storage.hadoop.{HadoopStorageConfiguration, HoodieHadoopStorage}

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.ManualClock

trait HudiTestBase extends QueryTest
  with Eventually {

  /**
   * Executes `f` with params (tableId, tempPath).
   *
   * We want to use a temp directory in addition to a unique temp table so that when the async
   * Hudi conversion runs and completes, the parent folder is still removed.
   */
  def withTempTableAndDir(f: (String, String) => Unit): Unit

  protected def spark: SparkSession

  def buildHudiMetaClient(testTablePath: String): HoodieTableMetaClient = {
    // scalastyle:off deltahadoopconfiguration
    val hadoopConf: Configuration = spark.sessionState.newHadoopConf()
    // scalastyle:on deltahadoopconfiguration
    val storageConf : StorageConfiguration[_] = new HadoopStorageConfiguration(hadoopConf)
    HoodieTableMetaClient.builder
      .setConf(storageConf).setBasePath(testTablePath)
      .setLoadActiveTimelineOnLoad(true)
      .build
  }

  def verifyFilesAndSchemaMatch(testTableName: String, testTablePath: String): Unit = {
    eventually(timeout(30.seconds)) {
      // To avoid requiring Hudi spark dependencies, we first lookup the active base files and then
      // assert by reading those active base files (parquet) directly
      // scalastyle:off deltahadoopconfiguration
      val hadoopConf: Configuration = spark.sessionState.newHadoopConf()
      // scalastyle:on deltahadoopconfiguration
      val storageConf : StorageConfiguration[_] = new HadoopStorageConfiguration(hadoopConf)
      val metaClient: HoodieTableMetaClient = buildHudiMetaClient(testTablePath)
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

      val deltaDF = spark.sql(s"SELECT * FROM $testTableName")
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

  def verifyNumHudiCommits(count: Integer, testTablePath: String): Unit = {
    eventually(timeout(30.seconds)) {
      val metaClient: HoodieTableMetaClient = buildHudiMetaClient(testTablePath)
      val activeCommits = metaClient.getActiveTimeline.getCommitsTimeline
        .filterCompletedInstants.countInstants
      val archivedCommits = metaClient.getArchivedTimeline.getCommitsTimeline
        .filterCompletedInstants.countInstants
      assert(activeCommits + archivedCommits == count)
    }
  }
}

trait ConvertToHudiTestBase extends HudiTestBase {
  test("basic test - managed table created with SQL") {
    withTempTableAndDir { case (testTableName, testTablePath) =>
      spark.sql(
        s"""
           |CREATE TABLE $testTableName (ID INT) USING DELTA
           |LOCATION '$testTablePath'
           |TBLPROPERTIES (
           |  'delta.universalFormat.enabledFormats' = 'hudi'
           |)""".stripMargin)
      spark.sql(s"INSERT INTO $testTableName VALUES (123)")
      verifyFilesAndSchemaMatch(testTableName, testTablePath)
    }
  }

  test("basic test - catalog table created with DataFrame") {
    withTempTableAndDir { case (testTableName, testTablePath) =>
      withDefaultTablePropsInSQLConf(false, {
        spark.range(10).write.format("delta")
          .option("path", testTablePath)
          .saveAsTable(testTableName)
      })
      verifyFilesAndSchemaMatch(testTableName, testTablePath)
      withDefaultTablePropsInSQLConf(false, {
        spark.range(10, 20, 1)
          .write.format("delta").mode("append")
          .save(testTablePath)
      })
      verifyFilesAndSchemaMatch(testTableName, testTablePath)
    }
  }

  for (isPartitioned <- Seq(true, false)) {
    test(s"validate multiple commits (partitioned = $isPartitioned)") {
      withTempTableAndDir { case (testTableName, testTablePath) =>
        spark.sql(
          s"""CREATE TABLE $testTableName (col1 INT, col2 STRING, col3 STRING) USING DELTA
             |${if (isPartitioned) "PARTITIONED BY (col3)" else ""}
             |LOCATION '$testTablePath'
             |TBLPROPERTIES (
             |  'delta.universalFormat.enabledFormats' = 'hudi'
             |)""".stripMargin)
        // perform some inserts
        spark.sql(s"INSERT INTO $testTableName VALUES (1, 'instant1', 'a'), (2, 'instant1', 'a')")
        verifyFilesAndSchemaMatch(testTableName, testTablePath)

        spark.sql(s"INSERT INTO `$testTableName` VALUES (3, 'instant2', 'b'), (4, 'instant2', 'b')")
        verifyFilesAndSchemaMatch(testTableName, testTablePath)

        spark.sql(s"INSERT INTO `$testTableName` VALUES (5, 'instant3', 'b'), (6, 'instant3', 'a')")
        verifyFilesAndSchemaMatch(testTableName, testTablePath)

        // update the data from the first instant
        spark.sql(s"UPDATE `$testTableName` SET col2 = 'instant4' WHERE col2 = 'instant1'")
        verifyFilesAndSchemaMatch(testTableName, testTablePath)

        // delete a single row
        spark.sql(s"DELETE FROM `$testTableName` WHERE col1 = 5")
        verifyFilesAndSchemaMatch(testTableName, testTablePath)
      }
    }
  }

  test("Enabling Delete Vector Throws Exception") {
    withTempTableAndDir { case (testTableName, testTablePath) =>
      intercept[DeltaUnsupportedOperationException] {
        spark.sql(
          s"""CREATE TABLE `$testTableName` (col1 INT, col2 STRING) USING DELTA
             |LOCATION '$testTablePath'
             |TBLPROPERTIES (
             |  'delta.universalFormat.enabledFormats' = 'hudi',
             |  'delta.enableDeletionVectors' = true
             |)""".stripMargin)
      }
    }
  }

  test("Enabling Delete Vector After Hudi Enabled Already Throws Exception") {
    withTempTableAndDir { case (testTableName, testTablePath) =>
      spark.sql(
        s"""CREATE TABLE `$testTableName` (col1 INT, col2 STRING) USING DELTA
           |LOCATION '$testTablePath'
           |TBLPROPERTIES (
           |  'delta.universalFormat.enabledFormats' = 'hudi'
           |)""".stripMargin)
      intercept[DeltaUnsupportedOperationException] {
        spark.sql(
          s"""ALTER TABLE `$testTableName` SET TBLPROPERTIES (
             |  'delta.enableDeletionVectors' = true
             |)""".stripMargin)
      }
    }
  }

  test(s"Conversion behavior for lists") {
    withTempTableAndDir { case (testTableName, testTablePath) =>
      spark.sql(
        s"""CREATE TABLE `$testTableName` (col1 ARRAY<INT>) USING DELTA
           |LOCATION '$testTablePath'
           |TBLPROPERTIES (
           |  'delta.universalFormat.enabledFormats' = 'hudi'
           |)""".stripMargin)
      spark.sql(s"INSERT INTO `$testTableName` VALUES (array(1, 2, 3))")
      verifyFilesAndSchemaMatch(testTableName, testTablePath)
    }
  }

  test(s"Conversion behavior for lists of structs") {
    withTempTableAndDir { case (testTableName, testTablePath) =>
      spark.sql(
        s"""CREATE TABLE `$testTableName`
           |(col1 ARRAY<STRUCT<field1: INT, field2: STRING>>) USING DELTA
           |LOCATION '$testTablePath'
           |TBLPROPERTIES (
           |  'delta.universalFormat.enabledFormats' = 'hudi'
           |)""".stripMargin)
      spark.sql(s"INSERT INTO `$testTableName` " +
        s"VALUES (array(named_struct('field1', 1, 'field2', 'hello'), " +
        s"named_struct('field1', 2, 'field2', 'world')))")
      verifyFilesAndSchemaMatch(testTableName, testTablePath)
    }
  }

  test(s"Conversion behavior for lists of lists") {
    withTempTableAndDir { case (testTableName, testTablePath) =>
      spark.sql(
        s"""CREATE TABLE `$testTableName`
           |(col1 ARRAY<ARRAY<INT>>) USING DELTA
           |LOCATION '$testTablePath'
           |TBLPROPERTIES (
           |  'delta.universalFormat.enabledFormats' = 'hudi'
           |)""".stripMargin)
      spark.sql(s"INSERT INTO `$testTableName` " +
        s"VALUES (array(array(1, 2, 3), array(4, 5, 6)))")
      verifyFilesAndSchemaMatch(testTableName, testTablePath)
    }
  }

  test(s"Conversion behavior for maps") {
    withTempTableAndDir { case (testTableName, testTablePath) =>
      spark.sql(
        s"""CREATE TABLE `$testTableName` (col1 MAP<STRING, INT>) USING DELTA
           |LOCATION '$testTablePath'
           |TBLPROPERTIES (
           |  'delta.universalFormat.enabledFormats' = 'hudi'
           |)""".stripMargin)
      spark.sql(
        s"INSERT INTO `$testTableName` VALUES (map('a', 1, 'b', 2, 'c', 3))"
      )
      verifyFilesAndSchemaMatch(testTableName, testTablePath)
    }
  }

  test(s"Conversion behavior for nested structs") {
    withTempTableAndDir { case (testTableName, testTablePath) =>
      spark.sql(
        s"""CREATE TABLE `$testTableName` (col1 STRUCT<field1: INT, field2: STRING,
           |field3: STRUCT<field4: INT, field5: INT, field6: STRING>>)
           |USING DELTA
           |LOCATION '$testTablePath'
           |TBLPROPERTIES (
           |  'delta.universalFormat.enabledFormats' = 'hudi'
           |)""".stripMargin)
      spark.sql(
        s"INSERT INTO `$testTableName` VALUES (named_struct('field1', 1, 'field2', 'hello', " +
          "'field3', named_struct('field4', 2, 'field5', 3, 'field6', 'world')))"
      )
      verifyFilesAndSchemaMatch(testTableName, testTablePath)
    }
  }

  test("validate Hudi timeline archival and cleaning") {
    withTempTableAndDir { case (_, testTablePath) =>
      val testOp = Truncate()
      withDefaultTablePropsInSQLConf(true, {
        val startTime = System.currentTimeMillis() - 12 * 24 * 60 * 60 * 1000
        val clock = new ManualClock(startTime)
        val actualTestStartTime = System.currentTimeMillis()
        val log = DeltaLog.forTable(spark, new Path(testTablePath), clock)
        (1 to 20).foreach { i =>
          val txn = if (i == 1) startTxnWithManualLogCleanup(log) else log.startTransaction()
          val file = AddFile(i.toString + ".parquet", Map.empty, 1, 1, true) :: Nil
          val delete: Seq[Action] = if (i > 1) {
            val timestamp = startTime + (System.currentTimeMillis() - actualTestStartTime)
            val prevFile = AddFile((i - 1).toString + ".parquet", Map.empty, 1, 1, true)
            prevFile.removeWithTimestamp(timestamp) :: Nil
          } else {
            Nil
          }
          txn.commit(delete ++ file, testOp)
          clock.advance(12.hours.toMillis)
          // wait for each Hudi sync to complete
          verifyNumHudiCommits(i, testTablePath)
        }

        val metaClient: HoodieTableMetaClient = HoodieTableMetaClient.builder
          .setConf(new HadoopStorageConfiguration(log.newDeltaHadoopConf()))
          .setBasePath(log.dataPath.toString)
          .setLoadActiveTimelineOnLoad(true)
          .build
        // Timeline requires a clean commit for proper removal of entries from the Hudi
        // Metadata Table
        assert(metaClient.getActiveTimeline.getCleanerTimeline.countInstants() == 1,
          "Cleaner timeline should have 1 instant")
        // Older commits should move from active to archive timeline
        assert(metaClient.getArchivedTimeline.getCommitsTimeline.filterInflights.countInstants == 2,
          "Archived timeline should have 2 instants")
      })
    }
  }

  test("validate various data types") {
    withTempTableAndDir { case (testTableName, testTablePath) =>
      spark.sql(
        s"""CREATE TABLE `$testTableName` (col1 BIGINT, col2 BOOLEAN, col3 DATE,
           | col4 DOUBLE, col5 FLOAT, col6 INT, col7 STRING, col8 TIMESTAMP,
           | col9 BINARY, col10 DECIMAL(5, 2),
           | col11 STRUCT<field1: INT, field2: STRING,
           | field3: STRUCT<field4: INT, field5: INT, field6: STRING>>)
           | USING DELTA
           |LOCATION '$testTablePath'
           |TBLPROPERTIES (
           |  'delta.universalFormat.enabledFormats' = 'hudi'
           |)""".stripMargin)
      val nowSeconds = Instant.now().getEpochSecond
      spark.sql(s"INSERT INTO `$testTableName` VALUES (123, true, "
        + s"date(from_unixtime($nowSeconds)), 32.1, 1.23, 456, 'hello world', "
        + s"timestamp(from_unixtime($nowSeconds)), X'1ABF', -999.99,"
        + s"STRUCT(1, 'hello', STRUCT(2, 3, 'world')))")
      verifyFilesAndSchemaMatch(testTableName, testTablePath)
    }
  }

  for (invalidType <- Seq("SMALLINT", "TINYINT", "TIMESTAMP_NTZ", "VOID")) {
    test(s"Unsupported Type $invalidType Throws Exception") {
      withTempTableAndDir { case (testTableName, testTablePath) =>
        intercept[DeltaUnsupportedOperationException] {
          spark.sql(
            s"""CREATE TABLE `$testTableName` (col1 $invalidType) USING DELTA
               |LOCATION '$testTablePath'
               |TBLPROPERTIES (
               |  'delta.universalFormat.enabledFormats' = 'hudi'
               |)""".stripMargin)
        }
      }
    }
  }


  test("all batches of actions are converted") {
    withTempTableAndDir { case (testTableName, testTablePath) =>
      withSQLConf(
        DeltaSQLConf.HUDI_MAX_COMMITS_TO_CONVERT.key -> "3"
      ) {
        spark.sql(
          s"""CREATE TABLE `$testTableName` (col1 INT)
             | USING DELTA
             |LOCATION '$testTablePath'""".stripMargin)
        for (i <- 1 to 10) {
          spark.sql(s"INSERT INTO `$testTableName` VALUES ($i)")
        }
        spark.sql(
          s"""ALTER TABLE `$testTableName` SET TBLPROPERTIES (
             |  'delta.universalFormat.enabledFormats' = 'hudi'
             |)""".stripMargin)
        verifyFilesAndSchemaMatch(testTableName, testTablePath)
      }
    }
  }
}

class ConvertToHudiSuite
  extends ConvertToHudiTestBase {

  private var _sparkSession: SparkSession = null

  override def spark: SparkSession = _sparkSession

  override def beforeAll(): Unit = {
    super.beforeAll()
    _sparkSession = createSparkSession()
    _sparkSession.conf.set(
      DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.defaultTablePropertyKey, "true")
  }

  def createSparkSession(): SparkSession = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    SparkSession.builder()
      .master("local[*]")
      .appName("UniformSession")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
  }

  override def withTempTableAndDir(f: (String, String) => Unit): Unit = {
    val tableId = s"testTable${UUID.randomUUID()}".replace("-", "_")
    withTempDir { externalLocation =>
      val tablePath = new Path(externalLocation.toString, "table")
      f(tableId, s"$tablePath")
    }
  }
}
