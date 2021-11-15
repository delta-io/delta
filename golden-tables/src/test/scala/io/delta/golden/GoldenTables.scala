/*
 * Copyright (2020-present) The Delta Lake Project Authors.
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

package io.delta.golden

import java.io.File
import java.math.{BigDecimal => JBigDecimal}
import java.sql.Timestamp
import java.util.{Locale, TimeZone}

import scala.concurrent.duration._
import scala.language.implicitConversions

import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.delta.{DeltaLog, OptimisticTransaction}
import org.apache.spark.sql.delta.DeltaOperations.ManualUpdate
import org.apache.spark.sql.delta.actions.{Action, AddCDCFile, AddFile, CommitInfo, JobInfo, Metadata, NotebookInfo, Protocol, RemoveFile, SetTransaction, SingleAction}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/**
 * This is a special class to generate golden tables for other projects. Run the following commands
 * to re-generate all golden tables:
 * ```
 * GENERATE_GOLDEN_TABLES=1 build/sbt 'goldenTables/test'
 * ```
 *
 * To generate a single table (that is specified below) run:
 * ```
 * GENERATE_GOLDEN_TABLES=1 build/sbt 'goldenTables/test-only *GoldenTables -- -z tbl_name'
 * ```
 *
 * After generating golden tables, be sure to package or test project standalone, otherwise the
 * test resources won't be available when running tests with IntelliJ.
 */
class GoldenTables extends QueryTest with SharedSparkSession {
  import testImplicits._

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

  // Timezone is fixed to America/Los_Angeles for timezone-sensitive tests
  TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"))
  // Add Locale setting
  Locale.setDefault(Locale.US)

  private val shouldGenerateGoldenTables = sys.env.contains("GENERATE_GOLDEN_TABLES")

  private lazy val goldenTablePath = {
    val dir = new File("src/test/resources/golden").getCanonicalFile
    require(dir.exists(),
      s"Cannot find $dir. Please run `GENERATE_GOLDEN_TABLES=1 build/sbt 'goldenTables/test'`.")
    dir
  }

  private def copyDir(src: String, dest: String): Unit = {
    FileUtils.copyDirectory(createGoldenTableFile(src), createGoldenTableFile(dest))
  }

  private def createGoldenTableFile(name: String): File = new File(goldenTablePath, name)

  private def createHiveGoldenTableFile(name: String): File =
    new File(createGoldenTableFile("hive"), name)

  private def generateGoldenTable(name: String,
      createTableFile: String => File = createGoldenTableFile) (generator: String => Unit): Unit = {
    if (shouldGenerateGoldenTables) {
      test(name) {
        val tablePath = createTableFile(name)
        JavaUtils.deleteRecursively(tablePath)
        generator(tablePath.getCanonicalPath)
      }
    }
  }

  /**
   * Helper class for to ensure initial commits contain a Metadata action.
   */
  private implicit class OptimisticTxnTestHelper(txn: OptimisticTransaction) {
    def commitManually(actions: Action*): Long = {
      if (txn.readVersion == -1 && !actions.exists(_.isInstanceOf[Metadata])) {
        txn.commit(Metadata() +: actions, ManualUpdate)
      } else {
        txn.commit(actions, ManualUpdate)
      }
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // io.delta.standalone.internal.DeltaLogSuite
  ///////////////////////////////////////////////////////////////////////////

  /** TEST: DeltaLogSuite > checkpoint */
  generateGoldenTable("checkpoint") { tablePath =>
    val log = DeltaLog.forTable(spark, new Path(tablePath))
    (1 to 15).foreach { i =>
      val txn = log.startTransaction()
      val file = AddFile(i.toString, Map.empty, 1, 1, true) :: Nil
      val delete: Seq[Action] = if (i > 1) {
        RemoveFile(i - 1 toString, Some(System.currentTimeMillis()), true) :: Nil
      } else {
        Nil
      }
      txn.commitManually(delete ++ file: _*)
    }
  }

  /** TEST: DeltaLogSuite > snapshot */
  private def writeData(data: Seq[(Int, String)], mode: String, tablePath: String): Unit = {
    data.toDS
      .toDF("col1", "col2")
      .write
      .mode(mode)
      .format("delta")
      .save(tablePath)
  }

  generateGoldenTable("snapshot-data0") { tablePath =>
    writeData((0 until 10).map(x => (x, s"data-0-$x")), "append", tablePath)
  }

  generateGoldenTable("snapshot-data1") { tablePath =>
    copyDir("snapshot-data0", "snapshot-data1")
    writeData((0 until 10).map(x => (x, s"data-1-$x")), "append", tablePath)
  }

  generateGoldenTable("snapshot-data2") { tablePath =>
    copyDir("snapshot-data1", "snapshot-data2")
    writeData((0 until 10).map(x => (x, s"data-2-$x")), "overwrite", tablePath)
  }

  generateGoldenTable("snapshot-data3") { tablePath =>
    copyDir("snapshot-data2", "snapshot-data3")
    writeData((0 until 20).map(x => (x, s"data-3-$x")), "append", tablePath)
  }

  generateGoldenTable("snapshot-data2-deleted") { tablePath =>
    copyDir("snapshot-data3", "snapshot-data2-deleted")
    DeltaTable.forPath(spark, tablePath).delete("col2 like 'data-2-%'")
  }

  generateGoldenTable("snapshot-repartitioned") { tablePath =>
    copyDir("snapshot-data2-deleted", "snapshot-repartitioned")
    spark.read
      .format("delta")
      .load(tablePath)
      .repartition(2)
      .write
      .option("dataChange", "false")
      .format("delta")
      .mode("overwrite")
      .save(tablePath)
  }

  generateGoldenTable("snapshot-vacuumed") { tablePath =>
    copyDir("snapshot-repartitioned", "snapshot-vacuumed")
    withSQLConf(DeltaSQLConf.DELTA_VACUUM_RETENTION_CHECK_ENABLED.key -> "false") {
      DeltaTable.forPath(spark, tablePath).vacuum(0.0)
    }
  }

  /** TEST: DeltaLogSuite > SC-8078: update deleted directory */
  generateGoldenTable("update-deleted-directory") { tablePath =>
    val log = DeltaLog.forTable(spark, new Path(tablePath))
    val txn = log.startTransaction()
    val files = (1 to 10).map(f => AddFile(f.toString, Map.empty, 1, 1, true))
    txn.commitManually(files: _*)
    log.checkpoint()
  }

  /** TEST: DeltaLogSuite > handle corrupted '_last_checkpoint' file */
  generateGoldenTable("corrupted-last-checkpoint") { tablePath =>
    val log = DeltaLog.forTable(spark, new Path(tablePath))
    val checkpointInterval = log.checkpointInterval
    for (f <- 0 to checkpointInterval) {
      val txn = log.startTransaction()
      txn.commitManually(AddFile(f.toString, Map.empty, 1, 1, true))
    }
  }

  /** TEST: DeltaLogSuite > paths should be canonicalized */
  {
    def helper(scheme: String, path: String, tableSuffix: String): Unit = {
      generateGoldenTable(s"canonicalized-paths-$tableSuffix") { tablePath =>
        val log = DeltaLog.forTable(spark, new Path(tablePath))
        new File(log.logPath.toUri).mkdirs()

        val add = AddFile(path, Map.empty, 100L, 10L, dataChange = true)
        val rm = RemoveFile(s"$scheme$path", Some(200L), dataChange = false)

        log.store.write(
          FileNames.deltaFile(log.logPath, 0L),
          Iterator(Protocol(), Metadata(), add).map(a => JsonUtils.toJson(a.wrap)))
        log.store.write(
          FileNames.deltaFile(log.logPath, 1L),
          Iterator(JsonUtils.toJson(rm.wrap)))
      }
    }

    // normal characters
    helper("file:", "/some/unqualified/absolute/path", "normal-a")
    helper("file://", "/some/unqualified/absolute/path", "normal-b")

    // special characters
    helper("file:", new Path("/some/unqualified/with space/p@#h").toUri.toString, "special-a")
    helper("file://", new Path("/some/unqualified/with space/p@#h").toUri.toString, "special-b")
  }

  /** TEST: DeltaLogSuite > delete and re-add the same file in different transactions */
  generateGoldenTable(s"delete-re-add-same-file-different-transactions") { tablePath =>
    val log = DeltaLog.forTable(spark, new Path(tablePath))
    assert(new File(log.logPath.toUri).mkdirs())

    val add1 = AddFile("foo", Map.empty, 1L, 1600000000000L, dataChange = true)
    log.startTransaction().commitManually(add1)

    val rm = add1.remove
    log.startTransaction().commit(rm :: Nil, ManualUpdate)

    val add2 = AddFile("foo", Map.empty, 1L, 1700000000000L, dataChange = true)
    log.startTransaction().commit(add2 :: Nil, ManualUpdate)

    // Add a new transaction to replay logs using the previous snapshot. If it contained
    // AddFile("foo") and RemoveFile("foo"), "foo" would get removed and fail this test.
    val otherAdd = AddFile("bar", Map.empty, 1L, System.currentTimeMillis(), dataChange = true)
    log.startTransaction().commit(otherAdd :: Nil, ManualUpdate)
  }

  /** TEST: DeltaLogSuite > error - versions not contiguous */
  generateGoldenTable("versions-not-contiguous") { tablePath =>
    val log = DeltaLog.forTable(spark, new Path(tablePath))
    assert(new File(log.logPath.toUri).mkdirs())

    val add1 = AddFile("foo", Map.empty, 1L, System.currentTimeMillis(), dataChange = true)
    log.startTransaction().commitManually(add1)

    val add2 = AddFile("foo", Map.empty, 1L, System.currentTimeMillis(), dataChange = true)
    log.startTransaction().commit(add2 :: Nil, ManualUpdate)

    val add3 = AddFile("foo", Map.empty, 1L, System.currentTimeMillis(), dataChange = true)
    log.startTransaction().commit(add3 :: Nil, ManualUpdate)

    new File(new Path(log.logPath, "00000000000000000001.json").toUri).delete()
  }

  /** TEST: DeltaLogSuite > state reconstruction without Protocol/Metadata should fail */
  Seq("protocol", "metadata").foreach { action =>
    generateGoldenTable(s"deltalog-state-reconstruction-without-$action") { tablePath =>
      val log = DeltaLog.forTable(spark, new Path(tablePath))
      assert(new File(log.logPath.toUri).mkdirs())

      val selectedAction = if (action == "metadata") {
        Protocol()
      } else {
        Metadata()
      }

      val file = AddFile("abc", Map.empty, 1, 1, true)
      log.store.write(
        FileNames.deltaFile(log.logPath, 0L),
        Iterator(selectedAction, file).map(a => JsonUtils.toJson(a.wrap)))
    }
  }

  /**
   * TEST: DeltaLogSuite > state reconstruction from checkpoint with missing Protocol/Metadata
   * should fail
   */
  Seq("protocol", "metadata").foreach { action =>
    generateGoldenTable(s"deltalog-state-reconstruction-from-checkpoint-missing-$action") {
      tablePath =>
        val log = DeltaLog.forTable(spark, tablePath)
        val checkpointInterval = log.checkpointInterval
        // Create a checkpoint regularly
        for (f <- 0 to checkpointInterval) {
          val txn = log.startTransaction()
          if (f == 0) {
            txn.commitManually(AddFile(f.toString, Map.empty, 1, 1, true))
          } else {
            txn.commit(Seq(AddFile(f.toString, Map.empty, 1, 1, true)), ManualUpdate)
          }
        }

        // Create an incomplete checkpoint without the action and overwrite the
        // original checkpoint
        val checkpointPath = FileNames.checkpointFileSingular(log.logPath, log.snapshot.version)
        withTempDir { tmpCheckpoint =>
          val takeAction = if (action == "metadata") {
            "protocol"
          } else {
            "metadata"
          }
          val corruptedCheckpointData = spark.read.parquet(checkpointPath.toString)
            .where(s"add is not null or $takeAction is not null")
            .as[SingleAction].collect()

          // Keep the add files and also filter by the additional condition
          corruptedCheckpointData.toSeq.toDS().coalesce(1).write
            .mode("overwrite").parquet(tmpCheckpoint.toString)
          val writtenCheckpoint =
            tmpCheckpoint.listFiles().toSeq.filter(_.getName.startsWith("part")).head
          val checkpointFile = new File(checkpointPath.toUri)
          new File(log.logPath.toUri).listFiles().toSeq.foreach { file =>
            if (file.getName.startsWith(".0")) {
              // we need to delete checksum files, otherwise trying to replace our incomplete
              // checkpoint file fails due to the LocalFileSystem's checksum checks.
              require(file.delete(), "Failed to delete checksum file")
            }
          }
          require(checkpointFile.delete(), "Failed to delete old checkpoint")
          require(writtenCheckpoint.renameTo(checkpointFile),
            "Failed to rename corrupt checkpoint")
        }
    }
  }

  /** TEST: DeltaLogSuite > table protocol version greater than client reader protocol version */
  generateGoldenTable("deltalog-invalid-protocol-version") { tablePath =>
    val log = DeltaLog.forTable(spark, new Path(tablePath))
    assert(new File(log.logPath.toUri).mkdirs())

    val file = AddFile("abc", Map.empty, 1, 1, true)
    log.store.write(
      FileNames.deltaFile(log.logPath, 0L),

      // Protocol reader version explicitly set too high
      // Also include a Metadata
      Iterator(Protocol(99), Metadata(), file).map(a => JsonUtils.toJson(a.wrap)))
  }

  /** TEST: DeltaLogSuite > get commit info */
  generateGoldenTable("deltalog-commit-info") { tablePath =>
    val log = DeltaLog.forTable(spark, new Path(tablePath))
    assert(new File(log.logPath.toUri).mkdirs())

    val commitInfoFile = CommitInfo(
      Some(0),
      new Timestamp(1540415658000L),
      Some("user_0"),
      Some("username_0"),
      "WRITE",
      Map("test" -> "\"test\""),
      Some(JobInfo("job_id_0", "job_name_0", "run_id_0", "job_owner_0", "trigger_type_0")),
      Some(NotebookInfo("notebook_id_0")),
      Some("cluster_id_0"),
      Some(-1),
      Some("default"),
      Some(true),
      Some(Map("test" -> "test")),
      Some("foo")
    )

    val addFile = AddFile("abc", Map.empty, 1, 1, true)
    log.store.write(
      FileNames.deltaFile(log.logPath, 0L),
      Iterator(Metadata(), Protocol(), commitInfoFile, addFile).map(a => JsonUtils.toJson(a.wrap)))
  }

  /** TEST: DeltaLogSuite > getChanges - no data loss */
  generateGoldenTable("deltalog-getChanges") { tablePath =>
    val log = DeltaLog.forTable(spark, new Path(tablePath))

    val add1 = AddFile("fake/path/1", Map.empty, 1, 1, dataChange = true)
    val txn1 = log.startTransaction()
    txn1.commitManually(Metadata() :: add1 :: Nil: _*)

    val addCDC2 = AddCDCFile("fake/path/2", Map("partition_foo" -> "partition_bar"), 1,
      Map("tag_foo" -> "tag_bar"))
    val remove2 = RemoveFile("fake/path/1", Some(100), dataChange = true)
    val txn2 = log.startTransaction()
    txn2.commitManually(addCDC2 :: remove2 :: Nil: _*)

    val setTransaction3 = SetTransaction("fakeAppId", 3L, Some(200))
    val txn3 = log.startTransaction()
    txn3.commitManually(Protocol() :: setTransaction3 :: Nil: _*)
  }

  ///////////////////////////////////////////////////////////////////////////
  // io.delta.standalone.internal.ReadOnlyLogStoreSuite
  ///////////////////////////////////////////////////////////////////////////

  /** TEST: ReadOnlyLogStoreSuite > read */
  generateGoldenTable("log-store-read") { tablePath =>
    val log = DeltaLog.forTable(spark, new Path(tablePath))
    assert(new File(log.logPath.toUri).mkdirs())

    val deltas = Seq(0, 1).map(i => new File(tablePath, i.toString)).map(_.getCanonicalPath)
    log.store.write(deltas.head, Iterator("zero", "none"))
    log.store.write(deltas(1), Iterator("one"))
  }

  /** TEST: ReadOnlyLogStoreSuite > listFrom */
  generateGoldenTable("log-store-listFrom") { tablePath =>
    val log = DeltaLog.forTable(spark, new Path(tablePath))
    assert(new File(log.logPath.toUri).mkdirs())

    val deltas = Seq(0, 1, 2, 3, 4)
      .map(i => new File(tablePath, i.toString))
      .map(_.getCanonicalPath)

    log.store.write(deltas(1), Iterator("zero"))
    log.store.write(deltas(2), Iterator("one"))
    log.store.write(deltas(3), Iterator("two"))
  }

  ///////////////////////////////////////////////////////////////////////////
  // io.delta.standalone.internal.DeltaTimeTravelSuite
  ///////////////////////////////////////////////////////////////////////////

  private implicit def durationToLong(duration: FiniteDuration): Long = {
    duration.toMillis
  }

  /** Generate commits with the given timestamp in millis. */
  private def generateCommits(location: String, commits: Long*): Unit = {
    val deltaLog = DeltaLog.forTable(spark, location)
    var startVersion = deltaLog.snapshot.version + 1
    commits.foreach { ts =>
      val rangeStart = startVersion * 10
      val rangeEnd = rangeStart + 10
      spark.range(rangeStart, rangeEnd).write.format("delta").mode("append").save(location)
      val file = new File(FileNames.deltaFile(deltaLog.logPath, startVersion).toUri)
      file.setLastModified(ts)
      startVersion += 1
    }
  }

  val start = 1540415658000L

  generateGoldenTable("time-travel-start") { tablePath =>
    generateCommits(tablePath, start)
  }

  generateGoldenTable("time-travel-start-start20") { tablePath =>
    copyDir("time-travel-start", "time-travel-start-start20")
    generateCommits(tablePath, start + 20.minutes)
  }

  generateGoldenTable("time-travel-start-start20-start40") { tablePath =>
    copyDir("time-travel-start-start20", "time-travel-start-start20-start40")
    generateCommits(tablePath, start + 40.minutes)
  }

  /**
   * TEST: DeltaTimeTravelSuite > time travel with schema changes - should instantiate old schema
   */
  generateGoldenTable("time-travel-schema-changes-a") { tablePath =>
    spark.range(10).write.format("delta").mode("append").save(tablePath)
  }

  generateGoldenTable("time-travel-schema-changes-b") { tablePath =>
    copyDir("time-travel-schema-changes-a", "time-travel-schema-changes-b")
    spark.range(10, 20).withColumn("part", 'id)
      .write.format("delta").mode("append").option("mergeSchema", true).save(tablePath)
  }

  /**
   * TEST: DeltaTimeTravelSuite > time travel with partition changes - should instantiate old schema
   */
  generateGoldenTable("time-travel-partition-changes-a") { tablePath =>
    spark.range(10).withColumn("part5", 'id % 5).write.format("delta")
      .partitionBy("part5").mode("append").save(tablePath)
  }

  generateGoldenTable("time-travel-partition-changes-b") { tablePath =>
    copyDir("time-travel-partition-changes-a", "time-travel-partition-changes-b")
    spark.range(10, 20).withColumn("part2", 'id % 2)
      .write
      .format("delta")
      .partitionBy("part2")
      .mode("overwrite")
      .option("overwriteSchema", true)
      .save(tablePath)
  }

  ///////////////////////////////////////////////////////////////////////////
  // io.delta.standalone.internal.DeltaDataReaderSuite
  ///////////////////////////////////////////////////////////////////////////

  private def writeDataWithSchema(tblLoc: String, data: Seq[Row], schema: StructType): Unit = {
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    df.write.format("delta").save(tblLoc)
  }

  /** TEST: DeltaDataReaderSuite > read - primitives */
  generateGoldenTable("data-reader-primitives") { tablePath =>
    def createRow(i: Int): Row = {
      Row(i, i.longValue, i.toByte, i.shortValue, i % 2 == 0, i.floatValue, i.doubleValue,
        i.toString, Array[Byte](i.toByte, i.toByte), new JBigDecimal(i))
    }

    def createRowWithNullValues(): Row = {
      Row(null, null, null, null, null, null, null, null, null, null)
    }

    val schema = new StructType()
      .add("as_int", IntegerType)
      .add("as_long", LongType)
      .add("as_byte", ByteType)
      .add("as_short", ShortType)
      .add("as_boolean", BooleanType)
      .add("as_float", FloatType)
      .add("as_double", DoubleType)
      .add("as_string", StringType)
      .add("as_binary", BinaryType)
      .add("as_big_decimal", DecimalType(1, 0))

    val data = createRowWithNullValues() +: (0 until 10).map(createRow)
    writeDataWithSchema(tablePath, data, schema)
  }

  /** TEST: DeltaDataReaderSuite > data reader can read partition values */
  generateGoldenTable("data-reader-partition-values") { tablePath =>
    def createRow(i: Int): Row = {
      Row(i, i.longValue, i.toByte, i.shortValue, i % 2 == 0, i.floatValue, i.doubleValue,
        i.toString, "null", java.sql.Date.valueOf("2021-09-08"),
        java.sql.Timestamp.valueOf("2021-09-08 11:11:11"), new JBigDecimal(i),
        Array(Row(i), Row(i), Row(i)),
        Row(i.toString, i.toString, Row(i, i.toLong)),
        i.toString)
    }

    def createRowWithNullPartitionValues(): Row = {
      Row(
        // partition values
        null, null, null, null, null, null, null, null, null, null, null, null,
        // data values
        Array(Row(2), Row(2), Row(2)),
        Row("2", "2", Row(2, 2L)),
        "2")
    }

    val schema = new StructType()
      // partition fields
      .add("as_int", IntegerType)
      .add("as_long", LongType)
      .add("as_byte", ByteType)
      .add("as_short", ShortType)
      .add("as_boolean", BooleanType)
      .add("as_float", FloatType)
      .add("as_double", DoubleType)
      .add("as_string", StringType)
      .add("as_string_lit_null", StringType)
      .add("as_date", DateType)
      .add("as_timestamp", TimestampType)
      .add("as_big_decimal", DecimalType(1, 0))
      // data fields
      .add("as_list_of_records", ArrayType(new StructType().add("val", IntegerType)))
      .add("as_nested_struct", new StructType()
        .add("aa", StringType)
        .add("ab", StringType)
        .add("ac", new StructType()
          .add("aca", IntegerType)
          .add("acb", LongType)
        )
      )
      .add("value", StringType)

    val data = (0 until 2).map(createRow) :+ createRowWithNullPartitionValues()

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    df.write
      .format("delta")
      .partitionBy("as_int", "as_long", "as_byte", "as_short", "as_boolean", "as_float",
        "as_double", "as_string", "as_string_lit_null", "as_date", "as_timestamp", "as_big_decimal")
      .save(tablePath)
  }

  /** TEST: DeltaDataReaderSuite > read - date types */
  Seq("UTC", "Iceland", "PST", "America/Los_Angeles", "Etc/GMT+9", "Asia/Beirut",
    "JST").foreach { timeZoneId =>
    generateGoldenTable(s"data-reader-date-types-$timeZoneId") { tablePath =>
      val timeZone = TimeZone.getTimeZone(timeZoneId)
      TimeZone.setDefault(timeZone)

      val timestamp = Timestamp.valueOf("2020-01-01 08:09:10")
      val date = java.sql.Date.valueOf("2020-01-01")

      val data = Row(timestamp, date) :: Nil
      val schema = new StructType()
        .add("timestamp", TimestampType)
        .add("date", DateType)

      writeDataWithSchema(tablePath, data, schema)
    }
  }

  /** TEST: DeltaDataReaderSuite > read - array of primitives */
  generateGoldenTable("data-reader-array-primitives") { tablePath =>
    def createRow(i: Int): Row = {
      Row(Array(i), Array(i.longValue), Array(i.toByte), Array(i.shortValue),
        Array(i % 2 == 0), Array(i.floatValue), Array(i.doubleValue), Array(i.toString),
        Array(Array(i.toByte, i.toByte)),
        Array(new JBigDecimal(i))
      )
    }

    val schema = new StructType()
      .add("as_array_int", ArrayType(IntegerType))
      .add("as_array_long", ArrayType(LongType))
      .add("as_array_byte", ArrayType(ByteType))
      .add("as_array_short", ArrayType(ShortType))
      .add("as_array_boolean", ArrayType(BooleanType))
      .add("as_array_float", ArrayType(FloatType))
      .add("as_array_double", ArrayType(DoubleType))
      .add("as_array_string", ArrayType(StringType))
      .add("as_array_binary", ArrayType(BinaryType))
      .add("as_array_big_decimal", ArrayType(DecimalType(1, 0)))

    val data = (0 until 10).map(createRow)
    writeDataWithSchema(tablePath, data, schema)
  }

  /** TEST: DeltaDataReaderSuite > read - array of complex objects */
  generateGoldenTable("data-reader-array-complex-objects") { tablePath =>
    def createRow(i: Int): Row = {
      Row(
        i,
        Array(Array(Array(i, i, i), Array(i, i, i)), Array(Array(i, i, i), Array(i, i, i))),
        Array(
          Array(Array(Array(i, i, i), Array(i, i, i)), Array(Array(i, i, i), Array(i, i, i))),
          Array(Array(Array(i, i, i), Array(i, i, i)), Array(Array(i, i, i), Array(i, i, i)))
        ),
        Array(
          Map[String, Long](i.toString -> i.toLong),
          Map[String, Long](i.toString -> i.toLong)
        ),
        Array(Row(i), Row(i), Row(i))
      )
    }

    val schema = new StructType()
      .add("i", IntegerType)
      .add("3d_int_list", ArrayType(ArrayType(ArrayType(IntegerType))))
      .add("4d_int_list", ArrayType(ArrayType(ArrayType(ArrayType(IntegerType)))))
      .add("list_of_maps", ArrayType(MapType(StringType, LongType)))
      .add("list_of_records", ArrayType(new StructType().add("val", IntegerType)))

    val data = (0 until 10).map(createRow)
    writeDataWithSchema(tablePath, data, schema)
  }

  /** TEST: DeltaDataReaderSuite > read - map */
  generateGoldenTable("data-reader-map") { tablePath =>
    def createRow(i: Int): Row = {
      Row(
        i,
        Map(i -> i),
        Map(i.toLong -> i.toByte),
        Map(i.toShort -> (i % 2 == 0)),
        Map(i.toFloat -> i.toDouble),
        Map(i.toString -> new JBigDecimal(i)),
        Map(i -> Array(Row(i), Row(i), Row(i)))
      )
    }

    val schema = new StructType()
      .add("i", IntegerType)
      .add("a", MapType(IntegerType, IntegerType))
      .add("b", MapType(LongType, ByteType))
      .add("c", MapType(ShortType, BooleanType))
      .add("d", MapType(FloatType, DoubleType))
      .add("e", MapType(StringType, DecimalType(1, 0)))
      .add("f", MapType(IntegerType, ArrayType(new StructType().add("val", IntegerType))))

    val data = (0 until 10).map(createRow)
    writeDataWithSchema(tablePath, data, schema)
  }

  /** TEST: DeltaDataReaderSuite > read - nested struct */
  generateGoldenTable("data-reader-nested-struct") { tablePath =>
    def createRow(i: Int): Row = Row(Row(i.toString, i.toString, Row(i, i.toLong)), i)

    val schema = new StructType()
      .add("a", new StructType()
        .add("aa", StringType)
        .add("ab", StringType)
        .add("ac", new StructType()
          .add("aca", IntegerType)
          .add("acb", LongType)
        )
      )
      .add("b", IntegerType)

    val data = (0 until 10).map(createRow)
    writeDataWithSchema(tablePath, data, schema)
  }

  /** TEST: DeltaDataReaderSuite > read - nullable field, invalid schema column key */
  generateGoldenTable("data-reader-nullable-field-invalid-schema-key") { tablePath =>
    val data = Row(Seq(null, null, null)) :: Nil
    val schema = new StructType()
      .add("array_can_contain_null", ArrayType(StringType, containsNull = true))
    writeDataWithSchema(tablePath, data, schema)
  }

  /** TEST: DeltaDataReaderSuite > test escaped char sequences in path */
  generateGoldenTable("data-reader-escaped-chars") { tablePath =>
    val data = Seq("foo1" -> "bar+%21", "foo2" -> "bar+%22", "foo3" -> "bar+%23")

    data.foreach { row =>
      Seq(row).toDF().write.format("delta").mode("append").partitionBy("_2").save(tablePath)
    }
  }

  /** TEST: DeltaDataReaderSuite > #124: decimal decode bug */
  generateGoldenTable("124-decimal-decode-bug") { tablePath =>
    val data = Seq(Row(new JBigDecimal(1000000)))
    val schema = new StructType().add("large_decimal", DecimalType(10, 0))
    writeDataWithSchema(tablePath, data, schema)
  }

  /** TEST: DeltaDataReaderSuite > #125: iterator bug */
  generateGoldenTable("125-iterator-bug") { tablePath =>
    val datas = Seq(
      Seq(),
      Seq(1),
      Seq(2), Seq(),
      Seq(3), Seq(), Seq(),
      Seq(4), Seq(), Seq(), Seq(),
      Seq(5)
    )
    datas.foreach { data =>
      data.toDF("col1").write.format("delta").mode("append").save(tablePath)
    }
  }

  generateGoldenTable("deltatbl-not-allow-write", createHiveGoldenTableFile) { tablePath =>
    val data = (0 until 10).map(x => (x, s"foo${x % 2}"))
    data.toDF("a", "b").write.format("delta").save(tablePath)
  }

  generateGoldenTable("deltatbl-schema-match", createHiveGoldenTableFile) { tablePath =>
    val data = (0 until 10).map(x => (x, s"foo${x % 2}", s"test${x % 3}"))
    data.toDF("a", "b", "c").write.format("delta").partitionBy("b").save(tablePath)
  }

  generateGoldenTable("deltatbl-non-partitioned", createHiveGoldenTableFile) { tablePath =>
    val data = (0 until 10).map(x => (x, s"foo${x % 2}"))
    data.toDF("c1", "c2").write.format("delta").save(tablePath)
  }

  generateGoldenTable("deltatbl-partitioned", createHiveGoldenTableFile) { tablePath =>
    val data = (0 until 10).map(x => (x, s"foo${x % 2}"))
    data.toDF("c1", "c2").write.format("delta").partitionBy("c2").save(tablePath)
  }

  generateGoldenTable("deltatbl-partition-prune", createHiveGoldenTableFile) { tablePath =>
    val data = Seq(
      ("hz", "20180520", "Jim", 3),
      ("hz", "20180718", "Jone", 7),
      ("bj", "20180520", "Trump", 1),
      ("sh", "20180512", "Jay", 4),
      ("sz", "20181212", "Linda", 8)
    )
    data.toDF("city", "date", "name", "cnt")
    .write.format("delta").partitionBy("date", "city").save(tablePath)
  }

  generateGoldenTable("deltatbl-touch-files-needed-for-partitioned", createHiveGoldenTableFile) {
    tablePath =>
      val data = (0 until 10).map(x => (x, s"foo${x % 2}"))
      data.toDF("c1", "c2").write.format("delta").partitionBy("c2").save(tablePath)
  }

  generateGoldenTable("deltatbl-special-chars-in-partition-column", createHiveGoldenTableFile) {
    tablePath =>
      val data = (0 until 10).map(x => (x, s"+ =%${x % 2}"))
      data.toDF("c1", "c2").write.format("delta").partitionBy("c2").save(tablePath)
  }

  generateGoldenTable("deltatbl-map-types-correctly", createHiveGoldenTableFile) { tablePath =>
    val data = Seq(
      TestClass(
        97.toByte,
        Array(98.toByte, 99.toByte),
        true,
        4,
        5L,
        "foo",
        6.0f,
        7.0,
        8.toShort,
        new java.sql.Date(60000000L),
        new java.sql.Timestamp(60000000L),
        new java.math.BigDecimal(12345.6789),
        Array("foo", "bar"),
        Map("foo" -> 123L),
        TestStruct("foo", 456L)
      )
    )
    data.toDF.write.format("delta").save(tablePath)
  }

  generateGoldenTable("deltatbl-column-names-case-insensitive", createHiveGoldenTableFile) {
    tablePath =>
      val data = (0 until 10).map(x => (x, s"foo${x % 2}"))
      data.toDF("FooBar", "BarFoo").write.format("delta").partitionBy("BarFoo").save(tablePath)
  }

  generateGoldenTable("deltatbl-deleted-path", createHiveGoldenTableFile) {
    tablePath =>
      val data = (0 until 10).map(x => (x, s"foo${x % 2}"))
      data.toDF("c1", "c2").write.format("delta").save(tablePath)
  }

  generateGoldenTable("deltatbl-incorrect-format-config", createHiveGoldenTableFile) { tablePath =>
    val data = (0 until 10).map(x => (x, s"foo${x % 2}"))
    data.toDF("a", "b").write.format("delta").save(tablePath)
  }
}

case class TestStruct(f1: String, f2: Long)

/** A special test class that covers all Spark types we support in the Hive connector. */
case class TestClass(
  c1: Byte,
  c2: Array[Byte],
  c3: Boolean,
  c4: Int,
  c5: Long,
  c6: String,
  c7: Float,
  c8: Double,
  c9: Short,
  c10: java.sql.Date,
  c11: java.sql.Timestamp,
  c12: BigDecimal,
  c13: Array[String],
  c14: Map[String, Long],
  c15: TestStruct
)

case class OneItem[T](t: T)
