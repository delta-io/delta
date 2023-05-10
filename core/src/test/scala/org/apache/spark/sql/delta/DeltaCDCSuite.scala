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

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.JavaConverters._

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.DeltaTestUtils.BOOLEAN_DOMAIN
import org.apache.spark.sql.delta.commands.cdc.CDCReader._
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaColumnMappingSelectedTestMixin
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.FileNames

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.functions.{col, current_timestamp, floor, lit}
import org.apache.spark.sql.streaming.StreamingQueryException
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructType}

abstract class DeltaCDCSuiteBase
  extends QueryTest
  with SharedSparkSession  with CheckCDCAnswer
  with DeltaSQLCommandTest {

  import testImplicits._

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey, "true")

  /** Represents path or metastore table name */
  abstract case class TblId(id: String)
  class TablePath(path: String) extends TblId(path)
  class TableName(name: String) extends TblId(name)

  /** Indicates either the starting or ending version/timestamp */
  trait Boundary
  case class StartingVersion(value: String) extends Boundary
  case class StartingTimestamp(value: String) extends Boundary
  case class EndingVersion(value: String) extends Boundary
  case class EndingTimestamp(value: String) extends Boundary
  case object Unbounded extends Boundary // used to model situation when a boundary isn't provided
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def createTblWithThreeVersions(
      tblName: Option[String] = None,
      path: Option[String] = None): Unit = {
    // version 0
    if (tblName.isDefined && path.isDefined) {
      spark.range(10).write.format("delta")
        .option("path", path.get)
        .saveAsTable(tblName.get)
    } else if (tblName.isDefined) {
      spark.range(10).write.format("delta")
        .saveAsTable(tblName.get)
    } else if (path.isDefined) {
      spark.range(10).write.format("delta")
        .save(path.get)
    }

    if (tblName.isDefined) {
      // version 1
      spark.range(10, 20).write.format("delta").mode("append").saveAsTable(tblName.get)

      // version 2
      spark.range(20, 30).write.format("delta").mode("append").saveAsTable(tblName.get)
    } else if (path.isDefined) {
      // version 1
      spark.range(10, 20).write.format("delta").mode("append").save(path.get)

      // version 2
      spark.range(20, 30).write.format("delta").mode("append").save(path.get)
    }
  }

  /** Single method to do all kinds of CDC reads */
  // By default, we use the `legacy` batch CDF schema mode, in which either latest schema is used
  // or the time-travelled schema is used.
  def cdcRead(
      tblId: TblId,
      start: Boundary,
      end: Boundary,
      schemaMode: Option[DeltaBatchCDFSchemaMode] = Some(BatchCDFSchemaLegacy),
      readerOptions: Map[String, String] = Map.empty): DataFrame

  /** Modify timestamp for a delta commit, used to test timestamp querying */
  def modifyDeltaTimestamp(deltaLog: DeltaLog, version: Long, time: Long): Unit = {
    val file = new File(FileNames.deltaFile(deltaLog.logPath, version).toUri)
    file.setLastModified(time)
    val crc = new File(FileNames.checksumFile(deltaLog.logPath, version).toUri)
    if (crc.exists()) {
      crc.setLastModified(time)
    }
  }

  /** Create table utility method */
  def ctas(srcTbl: String, dstTbl: String, disableCDC: Boolean = false): Unit = {
    val readDf = cdcRead(new TableName(srcTbl), StartingVersion("0"), EndingVersion("1"))
    if (disableCDC) {
      withSQLConf(DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey -> "false") {
        readDf.write.format("delta")
          .saveAsTable(dstTbl)
      }
    } else {
      readDf.write.format("delta")
        .saveAsTable(dstTbl)
    }
  }

  private val validTimestampFormats =
    Seq("yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss.SSS", "yyyy-MM-dd")
  private val invalidTimestampFormats =
    Seq("yyyyMMddHHmmssSSS")

  (validTimestampFormats ++ invalidTimestampFormats).foreach { formatStr =>
    val isValid = validTimestampFormats.contains(formatStr)
    val isValidStr = if (isValid) "valid" else "invalid"

    test(s"CDF timestamp format - $formatStr is $isValidStr") {
      withTable("src") {
        createTblWithThreeVersions(tblName = Some("src"))

        val timestamp = new SimpleDateFormat(formatStr).format(new Date(1))

        def doRead(): Unit = {
          cdcRead(new TableName("src"), StartingTimestamp(timestamp), EndingVersion("1"))
        }

        if (isValid) {
          doRead()
        } else {
          val e = intercept[AnalysisException] {
            doRead()
          }.getMessage()
          assert(e.contains("The provided timestamp"))
          assert(e.contains("cannot be converted to a valid timestamp"))
        }
      }
    }
  }

  testQuietly("writes with metadata columns") {
    withTable("src", "dst") {

      // populate src table with CDC data
      createTblWithThreeVersions(tblName = Some("src"))

      // writing cdc data to a new table with cdc enabled should fail. the source table has columns
      // that are reserved for CDC only, and shouldn't be allowed into the target table.
      val e = intercept[IllegalStateException] {
        ctas("src", "dst")
      }
      val writeContainsCDCColumnsError = DeltaErrors.cdcColumnsInData(
        cdcReadSchema(new StructType()).fieldNames).getMessage
      val enablingCDCOnTableWithCDCColumns = DeltaErrors.tableAlreadyContainsCDCColumns(
        cdcReadSchema(new StructType()).fieldNames).getMessage

      assert(e.getMessage.contains(writeContainsCDCColumnsError))

      // when cdc is disabled writes should work
      ctas("src", "dst", disableCDC = true)

      // write some more data
      withTable("more_data") {
        spark.range(20, 30)
          .withColumn(CDC_TYPE_COLUMN_NAME, lit("insert"))
          .withColumn("_commit_version", lit(2L))
          .withColumn("_commit_timestamp", current_timestamp)
          .write.saveAsTable("more_data")

        spark.table("more_data").write.format("delta")
          .mode("append")
          .saveAsTable("dst")

        checkAnswer(
          spark.read.format("delta").table("dst"),
          cdcRead(new TableName("src"), StartingVersion("0"), EndingVersion("1"))
            .union(spark.table("more_data"))
        )
      }

      // re-enabling cdc should be disallowed, since the dst table already contains column that are
      // reserved for CDC only.
      val e2 = intercept[IllegalStateException] {
        sql(s"ALTER TABLE dst SET TBLPROPERTIES " +
          s"(${DeltaConfigs.CHANGE_DATA_FEED.key}=true)")
      }
      assert(e2.getMessage.contains(enablingCDCOnTableWithCDCColumns))
    }
  }

  test("changes from table by name") {
    withTable("tbl") {
      createTblWithThreeVersions(tblName = Some("tbl"))

      val readDf = cdcRead(new TableName("tbl"), StartingVersion("0"), EndingVersion("1"))
      checkCDCAnswer(
        DeltaLog.forTable(spark, TableIdentifier("tbl")),
        readDf,
        spark.range(20)
          .withColumn("_change_type", lit("insert"))
          .withColumn("_commit_version", (col("id") / 10).cast(LongType))
      )
    }
  }

  test("changes from table by path") {
    withTempDir { dir =>
      createTblWithThreeVersions(path = Some(dir.getAbsolutePath))

      val readDf = cdcRead(
        new TablePath(dir.getAbsolutePath), StartingVersion("0"), EndingVersion("1"))
      checkCDCAnswer(
        DeltaLog.forTable(spark, dir.getAbsolutePath),
        readDf,
        spark.range(20)
          .withColumn("_change_type", lit("insert"))
          .withColumn("_commit_version", (col("id") / 10).cast(LongType))
      )
    }
  }

  test("changes - start and end are timestamps") {
    withTempDir { tempDir =>
      createTblWithThreeVersions(path = Some(tempDir.getAbsolutePath))
      val deltaLog = DeltaLog.forTable(spark, tempDir.getAbsolutePath)

      // modify timestamps
      // version 0
      modifyDeltaTimestamp(deltaLog, 0, 0)
      val tsAfterV0 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        .format(new Date(1))

      // version 1
      modifyDeltaTimestamp(deltaLog, 1, 1000)
      val tsAfterV1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        .format(new Date(1001))

      modifyDeltaTimestamp(deltaLog, 2, 2000)

      val readDf = cdcRead(
        new TablePath(tempDir.getAbsolutePath),
        StartingTimestamp(tsAfterV0), EndingTimestamp(tsAfterV1))
      checkCDCAnswer(
        DeltaLog.forTable(spark, tempDir),
        readDf,
        spark.range(20)
          .withColumn("_change_type", lit("insert"))
          .withColumn("_commit_version", (col("id") / 10).cast(LongType)))
    }
  }

  test("changes - only start is a timestamp") {
    withTempDir { tempDir =>
      createTblWithThreeVersions(path = Some(tempDir.getAbsolutePath))
      val deltaLog = DeltaLog.forTable(spark, tempDir.getAbsolutePath)

      modifyDeltaTimestamp(deltaLog, 0, 0)
      modifyDeltaTimestamp(deltaLog, 1, 10000)
      modifyDeltaTimestamp(deltaLog, 2, 20000)

      val ts0 = dateFormat.format(new Date(2000))
      val readDf = cdcRead(
        new TablePath(tempDir.getAbsolutePath), StartingTimestamp(ts0), EndingVersion("1"))
      checkCDCAnswer(
        DeltaLog.forTable(spark, tempDir),
        readDf,
        spark.range(10, 20)
          .withColumn("_change_type", lit("insert"))
          .withColumn("_commit_version", (col("id") / 10).cast(LongType)))
    }
  }

  test("changes - only start is a timestamp - inclusive behavior") {
    withTempDir { tempDir =>
      createTblWithThreeVersions(path = Some(tempDir.getAbsolutePath))
      val deltaLog = DeltaLog.forTable(spark, tempDir.getAbsolutePath)

      modifyDeltaTimestamp(deltaLog, 0, 0)
      modifyDeltaTimestamp(deltaLog, 1, 1000)
      modifyDeltaTimestamp(deltaLog, 2, 2000)

      val ts0 = dateFormat.format(new Date(0))
      val readDf = cdcRead(
        new TablePath(tempDir.getAbsolutePath), StartingTimestamp(ts0), EndingVersion("1"))
      checkCDCAnswer(
        DeltaLog.forTable(spark, tempDir),
        readDf,
        spark.range(20)
          .withColumn("_change_type", lit("insert"))
          .withColumn("_commit_version", (col("id") / 10).cast(LongType)))
    }
  }

  test("version from timestamp - before the first version") {
    withTempDir { tempDir =>
      createTblWithThreeVersions(path = Some(tempDir.getAbsolutePath))
      val deltaLog = DeltaLog.forTable(spark, tempDir.getAbsolutePath)

      modifyDeltaTimestamp(deltaLog, 0, 4000)
      modifyDeltaTimestamp(deltaLog, 1, 8000)
      modifyDeltaTimestamp(deltaLog, 2, 12000)

      val ts0 = dateFormat.format(new Date(1000))
      val ts1 = dateFormat.format(new Date(3000))
      intercept[AnalysisException] {
        cdcRead(
          new TablePath(tempDir.getAbsolutePath),
          StartingTimestamp(ts0),
          EndingTimestamp(ts1))
          .collect()
      }.getMessage.contains("before the earliest version")
    }
  }

  test("version from timestamp - between two valid versions") {
    withTempDir { tempDir =>
      createTblWithThreeVersions(path = Some(tempDir.getAbsolutePath))
      val deltaLog = DeltaLog.forTable(spark, tempDir.getAbsolutePath)

      modifyDeltaTimestamp(deltaLog, 0, 0)
      modifyDeltaTimestamp(deltaLog, 1, 4000)
      modifyDeltaTimestamp(deltaLog, 2, 8000)

      val ts0 = dateFormat.format(new Date(1000))
      val ts1 = dateFormat.format(new Date(3000))
      val readDf = cdcRead(
        new TablePath(tempDir.getAbsolutePath), StartingTimestamp(ts0), EndingTimestamp(ts1))
      checkCDCAnswer(
        DeltaLog.forTable(spark, tempDir),
        readDf,
        spark.range(0)
          .withColumn("_change_type", lit("insert"))
          .withColumn("_commit_version", (col("id") / 10).cast(LongType)))
    }
  }

  test("version from timestamp - one version in between") {
    withTempDir { tempDir =>
      createTblWithThreeVersions(path = Some(tempDir.getAbsolutePath))
      val deltaLog = DeltaLog.forTable(spark, tempDir.getAbsolutePath)

      modifyDeltaTimestamp(deltaLog, 0, 0)
      modifyDeltaTimestamp(deltaLog, 1, 4000)
      modifyDeltaTimestamp(deltaLog, 2, 8000)

      val ts0 = dateFormat.format(new Date(3000))
      val ts1 = dateFormat.format(new Date(5000))
      val readDf = cdcRead(
        new TablePath(tempDir.getAbsolutePath), StartingTimestamp(ts0), EndingTimestamp(ts1))
      checkCDCAnswer(
        DeltaLog.forTable(spark, tempDir),
        readDf,
        spark.range(10, 20)
          .withColumn("_change_type", lit("insert"))
          .withColumn("_commit_version", (col("id") / 10).cast(LongType)))
    }
  }

  test("version from timestamp - end before start") {
    withTempDir { tempDir =>
      createTblWithThreeVersions(path = Some(tempDir.getAbsolutePath))
      val deltaLog = DeltaLog.forTable(spark, tempDir.getAbsolutePath)

      modifyDeltaTimestamp(deltaLog, 0, 0)
      modifyDeltaTimestamp(deltaLog, 1, 4000)
      modifyDeltaTimestamp(deltaLog, 2, 8000)

      val ts0 = dateFormat.format(new Date(3000))
      val ts1 = dateFormat.format(new Date(1000))
      intercept[DeltaIllegalArgumentException] {
        cdcRead(
          new TablePath(tempDir.getAbsolutePath),
          StartingTimestamp(ts0),
          EndingTimestamp(ts1))
          .collect()
      }.getErrorClass === "DELTA_INVALID_CDC_RANGE"
    }
  }

  test("version from timestamp - end before start with one version in between") {
    withTempDir { tempDir =>
      createTblWithThreeVersions(path = Some(tempDir.getAbsolutePath))
      val deltaLog = DeltaLog.forTable(spark, tempDir.getAbsolutePath)

      modifyDeltaTimestamp(deltaLog, 0, 0)
      modifyDeltaTimestamp(deltaLog, 1, 4000)
      modifyDeltaTimestamp(deltaLog, 2, 8000)

      val ts0 = dateFormat.format(new Date(5000))
      val ts1 = dateFormat.format(new Date(3000))
      intercept[DeltaIllegalArgumentException] {
        cdcRead(
          new TablePath(tempDir.getAbsolutePath),
          StartingTimestamp(ts0),
          EndingTimestamp(ts1))
          .collect()
      }.getErrorClass === "DELTA_INVALID_CDC_RANGE"
    }
  }

  test("start version and end version are the same") {
    val tblName = "tbl"
    withTable(tblName) {
      createTblWithThreeVersions(tblName = Some(tblName))

      val readDf = cdcRead(
        new TableName(tblName), StartingVersion("0"), EndingVersion("0"))
      checkCDCAnswer(
        DeltaLog.forTable(spark, TableIdentifier("tbl")),
        readDf,
        spark.range(10)
          .withColumn("_change_type", lit("insert"))
          .withColumn("_commit_version", (col("id") / 10).cast(LongType)))
    }
  }

  for (readWithVersionNumber <- BOOLEAN_DOMAIN)
  test(s"CDC read respects timezone and DST - readWithVersionNumber=$readWithVersionNumber") {
    val tblName = "tbl"
    withTable(tblName) {
      createTblWithThreeVersions(tblName = Some(tblName))

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tblName))

      // Set commit time during Daylight savings time change.
      val restoreDate = "2022-11-06 01:42:44"
      val format = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss Z")
      val timestamp = format.parse(s"$restoreDate -0800").getTime
      modifyDeltaTimestamp(deltaLog, 0, timestamp)

      // Verify DST is respected.
      val e = intercept[Exception] {
        cdcRead(new TableName(tblName),
          StartingTimestamp(s"$restoreDate -0700"),
          EndingTimestamp(s"$restoreDate -0700"))
      }
      assert(e.getMessage.contains("is before the earliest version available"))

      val readDf = if (readWithVersionNumber) {
        cdcRead(new TableName(tblName), StartingVersion("0"), EndingVersion("0"))
      } else {
        cdcRead(
          new TableName(tblName),
          StartingTimestamp(s"$restoreDate -0800"),
          EndingTimestamp(s"$restoreDate -0800"))
      }

      checkCDCAnswer(
        DeltaLog.forTable(spark, TableIdentifier(tblName)),
        readDf,
        spark.range(10)
          .withColumn("_change_type", lit("insert"))
          .withColumn("_commit_version", (col("id") / 10).cast(LongType)))
    }
  }

  test("start version is provided and no end version") {
    val tblName = "tbl"
    withTable(tblName) {
      createTblWithThreeVersions(tblName = Some(tblName))

      val readDf = cdcRead(
        new TableName(tblName), StartingVersion("0"), Unbounded)
      checkCDCAnswer(
        DeltaLog.forTable(spark, TableIdentifier("tbl")),
        readDf,
        spark.range(30)
          .withColumn("_change_type", lit("insert"))
          .withColumn("_commit_version", (col("id") / 10).cast(LongType)))
    }
  }

  test("end timestamp < start timestamp") {
    withTempDir { tempDir =>
      createTblWithThreeVersions(path = Some(tempDir.getAbsolutePath))
      val deltaLog = DeltaLog.forTable(spark, tempDir.getAbsolutePath)

      modifyDeltaTimestamp(deltaLog, 0, 0)
      modifyDeltaTimestamp(deltaLog, 1, 1000)
      modifyDeltaTimestamp(deltaLog, 2, 2000)

      val ts0 = dateFormat.format(new Date(2000))
      val ts1 = dateFormat.format(new Date(1))
      val e = intercept[IllegalArgumentException] {
        cdcRead(
          new TablePath(tempDir.getAbsolutePath), StartingTimestamp(ts0), EndingTimestamp(ts1))
      }
      assert(e.getMessage.contains("End cannot be before start"))
    }
  }

  test("end version < start version") {
    val tblName = "tbl"
    withTable(tblName) {
      createTblWithThreeVersions(tblName = Some(tblName))
      val e = intercept[IllegalArgumentException] {
        cdcRead(new TableName(tblName), StartingVersion("1"), EndingVersion("0"))
      }
      assert(e.getMessage.contains("End cannot be before start"))
    }
  }

  test("cdc result dataframe can be transformed further") {
    val tblName = "tbl"
    withTable(tblName) {
      createTblWithThreeVersions(tblName = Some(tblName))

      val cdcResult = cdcRead(new TableName(tblName), StartingVersion("0"), EndingVersion("1"))
      val transformedDf = cdcResult
        .drop(CDC_COMMIT_TIMESTAMP)
        .withColumn("col3", lit(0))
        .withColumn("still_there", col("_change_type"))

      checkAnswer(
        transformedDf,
        spark.range(20)
          .withColumn("_change_type", lit("insert"))
          .withColumn("_commit_version", (col("id") / 10).cast(LongType))
          .withColumn("col3", lit(0))
          .withColumn("still_there", col("_change_type"))
      )
    }
  }

  test("multiple references on same table") {
    val tblName = "tbl"
    withTable(tblName) {
      createTblWithThreeVersions(tblName = Some(tblName))

      val cdcResult0_1 = cdcRead(new TableName(tblName), StartingVersion("0"), EndingVersion("1"))
      val cdcResult0_2 = cdcRead(new TableName(tblName), StartingVersion("0"), EndingVersion("2"))

      val diff = cdcResult0_2.except(cdcResult0_1)

      checkCDCAnswer(
        DeltaLog.forTable(spark, TableIdentifier("tbl")),
        diff,
        spark.range(20, 30)
          .withColumn("_change_type", lit("insert"))
          .withColumn("_commit_version", (col("id") / 10).cast(LongType))
      )
    }
  }

  test("filtering cdc metadata columns") {
    val tblName = "tbl"
    withTable(tblName) {
      createTblWithThreeVersions(tblName = Some(tblName))
      val deltaTable = io.delta.tables.DeltaTable.forName("tbl")
      deltaTable.delete("id > 20")

      val cdcResult = cdcRead(new TableName(tblName), StartingVersion("0"), EndingVersion("3"))

      checkCDCAnswer(
        DeltaLog.forTable(spark, TableIdentifier("tbl")),
        cdcResult.filter("_change_type != 'insert'"),
        spark.range(21, 30)
          .withColumn("_change_type", lit("delete"))
          .withColumn("_commit_version", lit(3))
      )

      checkCDCAnswer(
        DeltaLog.forTable(spark, TableIdentifier("tbl")),
        cdcResult.filter("_commit_version = 1"),
        spark.range(10, 20)
          .withColumn("_change_type", lit("insert"))
          .withColumn("_commit_version", lit(1))
      )
    }
  }

  test("aggregating non-numeric cdc data columns") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      spark.range(10).selectExpr("id", "'text' as text")
          .write.format("delta").save(path)
      val deltaTable = io.delta.tables.DeltaTable.forPath(path)
      deltaTable.delete("id > 5")

      val cdcResult = cdcRead(new TablePath(path), StartingVersion("0"), EndingVersion("3"))

      checkAnswer(
        cdcResult.selectExpr("count(distinct text)"),
        Row(1)
      )

      checkAnswer(
        cdcResult.selectExpr("first(text)"),
        Row("text")
      )
    }
  }

  test("ending version not specified resolves to latest at execution time") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      spark.range(5).selectExpr("id", "'text' as text")
        .write.format("delta").save(path)
      val cdcResult = cdcRead(new TablePath(path), StartingVersion("0"), Unbounded)

      checkAnswer(
        cdcResult.selectExpr("id", "_change_type", "_commit_version"),
        Row(0, "insert", 0) :: Row(1, "insert", 0) :: Row(2, "insert", 0) ::
          Row(3, "insert", 0):: Row(4, "insert", 0) :: Nil
      )

      // The next scan of `cdcResult` should include this delete even though the DF was defined
      // before it.
      val deltaTable = io.delta.tables.DeltaTable.forPath(path)
      deltaTable.delete("id > 2")

      checkAnswer(
        cdcResult.selectExpr("id", "_change_type", "_commit_version"),
        Row(0, "insert", 0) :: Row(1, "insert", 0) :: Row(2, "insert", 0) ::
          Row(3, "insert", 0):: Row(4, "insert", 0) ::
          Row(3, "delete", 1):: Row(4, "delete", 1) :: Nil
      )
    }
  }

  test("table schema changed after dataframe with ending specified") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      spark.range(5).selectExpr("id", "'text' as text")
        .write.format("delta").save(path)
      val cdcResult = cdcRead(new TablePath(path), StartingVersion("0"), EndingVersion("1"))
      sql(s"ALTER TABLE delta.`$path` ADD COLUMN (newCol INT)")

      checkAnswer(
        cdcResult.selectExpr("id", "_change_type", "_commit_version"),
        Row(0, "insert", 0) :: Row(1, "insert", 0) :: Row(2, "insert", 0) ::
          Row(3, "insert", 0) :: Row(4, "insert", 0) :: Nil
      )
    }
  }

  test("table schema changed after dataframe with ending not specified") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      spark.range(5).selectExpr("id", "'text' as text")
        .write.format("delta").save(path)
      val cdcResult = cdcRead(new TablePath(path), StartingVersion("0"), Unbounded)
      sql(s"ALTER TABLE delta.`$path` ADD COLUMN (newCol STRING)")
      sql(s"INSERT INTO delta.`$path` VALUES (5, 'text', 'newColVal')")

      // Just ignoring the new column is pretty weird, but it's what we do for non-CDC dataframes,
      // so we preserve the behavior rather than adding a special case.
      checkAnswer(
        cdcResult.selectExpr("id", "_change_type", "_commit_version"),
        Row(0, "insert", 0) :: Row(1, "insert", 0) :: Row(2, "insert", 0) ::
          Row(3, "insert", 0) :: Row(4, "insert", 0) :: Row(5, "insert", 2) :: Nil
      )
    }
  }

  test("An error should be thrown when CDC is not enabled") {
    val tblName = "tbl"
    withTable(tblName) {
      withSQLConf(DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey -> "false") {
        // create version with cdc disabled - v0
        spark.range(10).write.format("delta").saveAsTable(tblName)
      }
      val deltaTable = io.delta.tables.DeltaTable.forName(tblName)
      // v1
      deltaTable.delete("id > 8")

      // v2
      sql(s"ALTER TABLE ${tblName} SET TBLPROPERTIES " +
        s"(${DeltaConfigs.CHANGE_DATA_FEED.key}=true)")

      // v3
      spark.range(10, 20).write.format("delta").mode("append").saveAsTable(tblName)

      // v4
      deltaTable.delete("id > 18")

      // v5
      sql(s"ALTER TABLE ${tblName} SET TBLPROPERTIES " +
        s"(${DeltaConfigs.CHANGE_DATA_FEED.key}=false)")

      var e = intercept[AnalysisException] {
        cdcRead(new TableName(tblName), StartingVersion("0"), EndingVersion("4")).collect()
      }
      assert(e.getMessage === DeltaErrors.changeDataNotRecordedException(0, 0, 4).getMessage)

      val cdcDf = cdcRead(new TableName(tblName), StartingVersion("2"), EndingVersion("4"))
      assert(cdcDf.count() == 11) // 10 rows inserted, 1 row deleted

      // Check that we correctly detect CDC is disabled and fail the query for multiple types of
      // ranges:
      //  * disabled at the end but not start - (2, 5)
      //  * disabled at the start but not end - (1, 4)
      //  * disabled at both start and end (even though enabled in the middle) - (1, 5)
      for ((start, end, firstDisabledVersion) <- Seq((2, 5, 5), (1, 4, 1), (1, 5, 1))) {
        e = intercept[AnalysisException] {
          cdcRead(
            new TableName(tblName),
            StartingVersion(start.toString), EndingVersion(end.toString)).collect()
        }
        assert(e.getMessage === DeltaErrors.changeDataNotRecordedException(
          firstDisabledVersion, start, end).getMessage)
      }
    }
  }

  test("changes - start timestamp exceeding latest commit timestamp") {
    withTempDir { tempDir =>
      withSQLConf(DeltaSQLConf.DELTA_CDF_ALLOW_OUT_OF_RANGE_TIMESTAMP.key -> "true") {
        val path = tempDir.getAbsolutePath
        createTblWithThreeVersions(path = Some(path))
        val deltaLog = DeltaLog.forTable(spark, path)

        // modify timestamps
        // version 0
        modifyDeltaTimestamp(deltaLog, 0, 0)

        // version 1
        modifyDeltaTimestamp(deltaLog, 1, 1000)

        // version 2
        modifyDeltaTimestamp(deltaLog, 2, 2000)

        val tsStart = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          .format(new Date(3000))
        val tsEnd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          .format(new Date(4000))

        val readDf = cdcRead(
          new TablePath(path),
          StartingTimestamp(tsStart),
          EndingTimestamp(tsEnd))
        checkCDCAnswer(
          DeltaLog.forTable(spark, tempDir),
          readDf,
          sqlContext.emptyDataFrame)
      }
    }
  }

  test("changes - end timestamp exceeding latest commit timestamp") {
    withTempDir { tempDir =>
      withSQLConf(DeltaSQLConf.DELTA_CDF_ALLOW_OUT_OF_RANGE_TIMESTAMP.key -> "true") {
        createTblWithThreeVersions(path = Some(tempDir.getAbsolutePath))
        val deltaLog = DeltaLog.forTable(spark, tempDir.getAbsolutePath)

        // modify timestamps
        // version 0
        modifyDeltaTimestamp(deltaLog, 0, 0)

        // version 1
        modifyDeltaTimestamp(deltaLog, 1, 1000)

        // version 2
        modifyDeltaTimestamp(deltaLog, 2, 2000)

        val tsStart = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          .format(new Date(0))
        val tsEnd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          .format(new Date(4000))

        val readDf = cdcRead(
          new TablePath(tempDir.getAbsolutePath),
          StartingTimestamp(tsStart), EndingTimestamp(tsEnd))
        checkCDCAnswer(
          DeltaLog.forTable(spark, tempDir),
          readDf,
          spark.range(30)
            .withColumn("_change_type", lit("insert"))
            .withColumn("_commit_version", (col("id") / 10).cast(LongType)))
      }
    }
  }

  test("batch write: append, dynamic partition overwrite + CDF") {
    withSQLConf(
      DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey -> "true",
      DeltaSQLConf.DYNAMIC_PARTITION_OVERWRITE_ENABLED.key -> "true") {
      withTempDir { tempDir =>
        def data: DataFrame = spark.read.format("delta").load(tempDir.toString)

        Seq(("a", "x"), ("b", "y"), ("c", "x")).toDF("value", "part")
          .write
          .format("delta")
          .partitionBy("part")
          .mode("append")
          .save(tempDir.getCanonicalPath)
        checkAnswer(
          cdcRead(new TablePath(tempDir.getCanonicalPath), StartingVersion("0"), EndingVersion("0"))
            .drop(CDC_COMMIT_TIMESTAMP),
          Row("a", "x", "insert", 0) :: Row("b", "y", "insert", 0) ::
            Row("c", "x", "insert", 0) :: Nil
        )

        // ovewrite nothing
        Seq(("d", "z")).toDF("value", "part")
          .write
          .format("delta")
          .partitionBy("part")
          .mode("overwrite")
          .option(DeltaOptions.PARTITION_OVERWRITE_MODE_OPTION, "dynamic")
          .save(tempDir.getCanonicalPath)
        checkDatasetUnorderly(data.select("value", "part").as[(String, String)],
          ("a", "x"), ("b", "y"), ("c", "x"), ("d", "z"))
        checkAnswer(
          cdcRead(new TablePath(tempDir.getCanonicalPath), StartingVersion("1"), EndingVersion("1"))
            .drop(CDC_COMMIT_TIMESTAMP),
          Row("d", "z", "insert", 1) :: Nil
        )

        // overwrite partition `part`="x"
        Seq(("a", "x"), ("e", "x")).toDF("value", "part")
          .write
          .format("delta")
          .partitionBy("part")
          .mode("overwrite")
          .option(DeltaOptions.PARTITION_OVERWRITE_MODE_OPTION, "dynamic")
          .save(tempDir.getCanonicalPath)
        checkDatasetUnorderly(data.select("value", "part").as[(String, String)],
          ("a", "x"), ("b", "y"), ("d", "z"), ("e", "x"))
        checkAnswer(
          cdcRead(new TablePath(tempDir.getCanonicalPath), StartingVersion("2"), EndingVersion("2"))
            .drop(CDC_COMMIT_TIMESTAMP),
          Row("a", "x", "delete", 2) :: Row("c", "x", "delete", 2) ::
            Row("a", "x", "insert", 2) :: Row("e", "x", "insert", 2) :: Nil
        )
      }
    }
  }
}

class DeltaCDCScalaSuite extends DeltaCDCSuiteBase {

  /** Single method to do all kinds of CDC reads */
  def cdcRead(
      tblId: TblId,
      start: Boundary,
      end: Boundary,
      schemaMode: Option[DeltaBatchCDFSchemaMode] = Some(BatchCDFSchemaLegacy),
      readerOptions: Map[String, String] = Map.empty): DataFrame = {

    // Set the batch CDF schema mode using SQL conf if we specified it
    if (schemaMode.isDefined) {
      var result: DataFrame = null
      withSQLConf(DeltaSQLConf.DELTA_CDF_DEFAULT_SCHEMA_MODE_FOR_COLUMN_MAPPING_TABLE.key ->
        schemaMode.get.name) {
        result = cdcRead(tblId, start, end, None, readerOptions)
      }
      return result
    }

    val startPrefix: (String, String) = start match {
      case startingVersion: StartingVersion =>
        ("startingVersion", startingVersion.value)

      case startingTimestamp: StartingTimestamp =>
        ("startingTimestamp", startingTimestamp.value)

      case Unbounded =>
        ("", "")
    }
    val endPrefix: (String, String) = end match {
      case endingVersion: EndingVersion =>
        ("endingVersion", endingVersion.value)

      case endingTimestamp: EndingTimestamp =>
        ("endingTimestamp", endingTimestamp.value)

      case Unbounded =>
        ("", "")
    }

    var dfr = spark.read.format("delta")
      .option(DeltaOptions.CDC_READ_OPTION, "true")
      .option(startPrefix._1, startPrefix._2)
      .option(endPrefix._1, endPrefix._2)

    readerOptions.foreach { case (k, v) =>
      dfr = dfr.option(k, v)
    }

    tblId match {
      case path: TablePath =>
        dfr.load(path.id)

      case tblName: TableName =>
        dfr.table(tblName.id)

      case _ =>
        throw new IllegalArgumentException("No table name or path provided")
    }
  }


  test("start version or timestamp is not provided") {
    val tblName = "tbl"
    withTable(tblName) {
      createTblWithThreeVersions(tblName = Some(tblName))

      val e = intercept[AnalysisException] {
        spark.read.format("delta")
          .option(DeltaOptions.CDC_READ_OPTION, "true")
          .option("endingVersion", 1)
          .table(tblName)
          .show()
      }
      assert(e.getMessage.contains(DeltaErrors.noStartVersionForCDC().getMessage))
    }
  }

  test("Not having readChangeFeed will not output cdc columns") {
    val tblName = "tbl2"
    withTable(tblName) {
      spark.range(0, 10).write.format("delta").saveAsTable(tblName)
      checkAnswer(spark.read.format("delta").table(tblName), spark.range(0, 10).toDF("id"))

      checkAnswer(
        spark.read.format("delta")
          .option("startingVersion", "0")
          .option("endingVersion", "0")
          .table(tblName),
        spark.range(0, 10).toDF("id"))
    }
  }

  test("non-monotonic timestamps") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      val deltaLog = DeltaLog.forTable(spark, path)
      (0 to 3).foreach { i =>
        spark.range(i * 10, (i + 1) * 10).write.format("delta").mode("append").save(path)
        val file = new File(FileNames.deltaFile(deltaLog.logPath, i).toUri)
        file.setLastModified(300 - i)
      }

      checkCDCAnswer(
        deltaLog,
        cdcRead(new TablePath(path), StartingVersion("0"), EndingVersion("3")),
        spark.range(0, 40)
          .withColumn("_change_type", lit("insert"))
          .withColumn("_commit_version", floor(col("id") / 10)))
    }
  }

  test("Repeated delete") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      val deltaLog = DeltaLog.forTable(spark, path)
      spark.range(0, 5, 1, numPartitions = 1).write.format("delta").save(path)
      sql(s"DELETE FROM delta.`$path` WHERE id = 3") // Version 1
      sql(s"DELETE FROM delta.`$path` WHERE id = 4") // Version 2
      sql(s"DELETE FROM delta.`$path` WHERE id IN (0, 1, 2)") // Version 3, remove the whole file

      val allChanges: Map[Int, Seq[Row]] = Map(
        1 -> (Row(3, "delete", 1) :: Nil),
        2 -> (Row(4, "delete", 2) :: Nil),
        3 -> (Row(0, "delete", 3) :: Row(1, "delete", 3) :: Row(2, "delete", 3) :: Nil)
      )

      for(start <- 1 to 3; end <- start to 3) {
        checkCDCAnswer(
          deltaLog,
          cdcRead(
            new TablePath(path),
            StartingVersion(start.toString),
            EndingVersion(end.toString)),
         (start to end).flatMap(v => allChanges(v)))
      }
    }
  }
}

class DeltaCDCScalaWithDeletionVectorsSuite extends DeltaCDCScalaSuite
  with DeletionVectorsTestUtils {
  override def beforeAll(): Unit = {
    super.beforeAll()
    enableDeletionVectorsForDeletes(spark)
  }
}
