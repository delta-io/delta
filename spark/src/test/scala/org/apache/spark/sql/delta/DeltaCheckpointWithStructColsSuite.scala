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

import org.apache.spark.sql.delta.schema.SchemaMergingUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.FileNames

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.functions.{col, lit, struct}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class DeltaCheckpointWithStructColsSuite
  extends QueryTest
  with SharedSparkSession
  with DeltaColumnMappingTestUtils
  with DeltaSQLCommandTest {

  import testImplicits._

  protected val checkpointFnsWithStructAndJsonStats: Seq[DeltaLog => Long] = Seq(
    checkpointWithProperty(writeStatsAsJson = Some(true)),
    checkpointWithProperty(writeStatsAsJson = None))

  protected val checkpointFnsWithStructWithoutJsonStats: Seq[DeltaLog => Long] = Seq(
    checkpointWithProperty(writeStatsAsJson = Some(false)))

  protected val checkpointFnsWithoutStructWithJsonStats: Seq[DeltaLog => Long] = Seq(
    checkpointWithProperty(writeStatsAsJson = Some(true), writeStatsAsStruct = false),
    checkpointWithProperty(writeStatsAsJson = None, writeStatsAsStruct = false))

  /**
   * Creates a table from the given DataFrame and partitioning. Then for each checkpointing
   * function, it runs the given validation function.
   */
  protected def checkpointSchemaForTable(df: DataFrame, partitionBy: String*)(
      checkpointingFns: Seq[DeltaLog => Long],
      expectedCols: Seq[(String, DataType)],
      additionalValidationFn: Set[String] => Unit = _ => ()): Unit = {
    checkpointingFns.foreach { checkpointingFn =>
      withTempDir { dir =>
        val deltaLog = DeltaLog.forTable(spark, dir)
        df.write.format("delta").partitionBy(partitionBy: _*).save(dir.getCanonicalPath)
        val version = checkpointingFn(deltaLog)

        val f = spark.read.parquet(
          FileNames.checkpointFileSingular(deltaLog.logPath, version).toString)
        assert(f.schema.getFieldIndex("commitInfo").isEmpty,
          "commitInfo should not be written to the checkpoint")
        val baseCols = Set("add", "metaData", "protocol", "remove", "txn")
        baseCols.foreach { name =>
          assert(f.schema.getFieldIndex(name).nonEmpty, s"Couldn't find required field $name " +
            s"among: ${f.schema.fieldNames.mkString("[", ", ", " ]")}")
        }

        val addSchema = f.schema("add").dataType.asInstanceOf[StructType]
        val addColumns = SchemaMergingUtils.explodeNestedFieldNames(addSchema).toSet

        val requiredCols = Seq(
          "path" -> StringType,
          "partitionValues" -> MapType(StringType, StringType),
          "size" -> LongType,
          "modificationTime" -> LongType,
          "dataChange" -> BooleanType,
          "tags" -> MapType(StringType, StringType)
        )

        val schema = deltaLog.update().schema
        (requiredCols ++ expectedCols).foreach { case (expectedField, dataType) =>
          // use physical name if possible
          val expectedPhysicalField =
            convertColumnNameToAttributeWithPhysicalName(expectedField, schema).name
          assert(addColumns.contains(expectedPhysicalField))
          // Check data type
          assert(f.select(col(s"add.$expectedPhysicalField")).schema.head.dataType === dataType)
        }

        additionalValidationFn(addColumns)

        DeltaLog.clearCache()
        checkAnswer(
          spark.read.format("delta").load(dir.getCanonicalPath),
          df
        )
      }
    }
  }

  test("unpartitioned table") {
    val df = spark.range(10).withColumn("part", ('id / 2).cast("int"))
    checkpointSchemaForTable(df)(
      checkpointingFns = checkpointFnsWithStructAndJsonStats,
      expectedCols = Nil,
      additionalValidationFn = addColumns => {
        checkFields(
          addColumns,
          statsAsJsonExists = true,
          unexpected = Seq(Checkpoints.STRUCT_PARTITIONS_COL_NAME))
      }
    )

    checkpointSchemaForTable(df)(
      checkpointingFns = checkpointFnsWithStructWithoutJsonStats,
      expectedCols = Nil,
      additionalValidationFn = addColumns => {
        checkFields(
          addColumns,
          statsAsJsonExists = false,
          unexpected = Seq(Checkpoints.STRUCT_PARTITIONS_COL_NAME))
      }
    )

    checkpointSchemaForTable(df)(
      checkpointingFns = checkpointFnsWithoutStructWithJsonStats,
      expectedCols = Nil,
      additionalValidationFn = addColumns => {
        checkFields(
          addColumns,
          statsAsJsonExists = true,
          unexpected = Seq(Checkpoints.STRUCT_PARTITIONS_COL_NAME))
      }
    )

    checkpointSchemaForTable(df)(
      checkpointingFns = Seq(checkpointWithoutStats),
      expectedCols = Nil,
      additionalValidationFn = addColumns => {
        checkFields(
          addColumns, statsAsJsonExists = false, Seq(Checkpoints.STRUCT_PARTITIONS_COL_NAME))
      }
    )
  }

  test("partitioned table") {
    val df = spark.range(10).withColumn("part", ('id / 2).cast("int"))
    // partitioned by "part"
    checkpointSchemaForTable(df, "part")(
      checkpointingFns = checkpointFnsWithStructAndJsonStats,
      expectedCols = Seq("partitionValues_parsed.part" -> IntegerType),
      additionalValidationFn = addColumns => {
        checkFields(
          addColumns,
          statsAsJsonExists = true,
          Nil)
      }
    )

    checkpointSchemaForTable(df, "part")(
      checkpointingFns = checkpointFnsWithStructWithoutJsonStats,
      expectedCols = Seq("partitionValues_parsed.part" -> IntegerType),
      additionalValidationFn = addColumns => {
        checkFields(
          addColumns,
          statsAsJsonExists = false,
          Nil)
      }
    )

    checkpointSchemaForTable(df, "part")(
      checkpointingFns = checkpointFnsWithoutStructWithJsonStats,
      expectedCols = Nil,
      additionalValidationFn = addColumns => {
        checkFields(
          addColumns,
          statsAsJsonExists = true,
          Seq(Checkpoints.STRUCT_PARTITIONS_COL_NAME))
      }
    )

    checkpointSchemaForTable(df, "part")(
      checkpointingFns = Seq(checkpointWithoutStats),
      expectedCols = Nil,
      additionalValidationFn = addColumns => {
        checkFields(
          addColumns, statsAsJsonExists = false, Seq(Checkpoints.STRUCT_PARTITIONS_COL_NAME))
      }
    )
  }

  test("special characters") {
    val weirdName1 = "part%!@#_$%^&*-"
    val weirdName2 = "part?_.+<>|/"
    val df = spark.range(10)
      .withColumn(weirdName1, ('id / 2).cast("int"))
      .withColumn(weirdName2, ('id / 3).cast("int"))
      .withColumn("struct", struct($"id", col(weirdName1), $"id".as(weirdName2)))

    val structColumns = Seq(
      s"partitionValues_parsed.$weirdName1" -> IntegerType,
      s"partitionValues_parsed.`$weirdName2`" -> IntegerType)

    // partitioned by weirdName1, weirdName2
    checkpointSchemaForTable(df, weirdName1, weirdName2)(
      checkpointingFns = checkpointFnsWithStructAndJsonStats,
      expectedCols = structColumns,
      additionalValidationFn = addColumns => {
        checkFields(
          addColumns,
          statsAsJsonExists = true,
          Nil)
      }
    )

    checkpointSchemaForTable(df, weirdName1, weirdName2)(
      checkpointingFns = checkpointFnsWithStructWithoutJsonStats,
      expectedCols = Nil,
      additionalValidationFn = addColumns => {
        checkFields(
          addColumns,
          statsAsJsonExists = false,
          Nil)
      }
    )

    checkpointSchemaForTable(df, weirdName1, weirdName2)(
      checkpointingFns = checkpointFnsWithoutStructWithJsonStats,
      expectedCols = Nil,
      additionalValidationFn = addColumns => {
        checkFields(
          addColumns,
          statsAsJsonExists = true,
          structColumns.map(_._1))
      }
    )
  }

  test("timestamps as partition values") {
    withTempDir { dir =>
      val df = Seq(
        (java.sql.Timestamp.valueOf("2012-12-31 16:00:10.011"), 2),
        (java.sql.Timestamp.valueOf("2099-12-31 16:00:10.011"), 4)).toDF("key", "value")

      df.write.format("delta").partitionBy("key").save(dir.getCanonicalPath)
      val deltaLog = DeltaLog.forTable(spark, dir)
      val version = checkpointWithProperty(
        writeStatsAsJson = Some(true), writeStatsAsStruct = true)(deltaLog)
      val f = spark.read.parquet(
        FileNames.checkpointFileSingular(deltaLog.logPath, version).toString)

      // use physical name
      val key = getPhysicalName("key", deltaLog.snapshot.schema)
      checkAnswer(
        f.select(s"add.partitionValues_parsed.`$key`"),
        Seq(Row(null), Row(null)) ++ df.select("key").collect()
      )

      sql(s"DELETE FROM delta.`${dir.getCanonicalPath}` WHERE CURRENT_TIMESTAMP > key")
      checkAnswer(
        spark.read.format("delta").load(dir.getCanonicalPath),
        Row(java.sql.Timestamp.valueOf("2099-12-31 16:00:10.011"), 4)
      )

      sql(s"DELETE FROM delta.`${dir.getCanonicalPath}` WHERE CURRENT_TIMESTAMP < key")
    }
  }


  /**
   * Creates a checkpoint by based on `writeStatsAsJson`/`writeStatsAsStruct` properties.
   */
  protected def checkpointWithProperty(
      writeStatsAsJson: Option[Boolean],
      writeStatsAsStruct: Boolean = true)(deltaLog: DeltaLog): Long = {
    val asJson = writeStatsAsJson.map { v =>
      s", delta.checkpoint.writeStatsAsJson = $v"
    }.getOrElse("")
    sql(s"ALTER TABLE delta.`${deltaLog.dataPath}` " +
      s"SET TBLPROPERTIES (delta.checkpoint.writeStatsAsStruct = ${writeStatsAsStruct}${asJson})")
    deltaLog.checkpoint()
    deltaLog.readLastCheckpointFile().get.version
  }

  /** A checkpoint that doesn't have any stats columns, i.e. `stats` and `stats_parsed`. */
  protected def checkpointWithoutStats(deltaLog: DeltaLog): Long = {
    sql(s"ALTER TABLE delta.`${deltaLog.dataPath}` " +
      s"SET TBLPROPERTIES (delta.checkpoint.writeStatsAsStruct = false, " +
      "delta.checkpoint.writeStatsAsJson = false)")
    deltaLog.checkpoint()
    deltaLog.readLastCheckpointFile().get.version
  }

  /**
   * Check the existence of the stats field and also not existence of the `unexpected` fields. The
   * `addColumns` is a Set of column names that contain the entire tree of columns in the `add`
   * field of the schema.
   */
  protected def checkFields(
      addColumns: Set[String],
      statsAsJsonExists: Boolean,
      unexpected: Seq[String]): Unit = {
    if (statsAsJsonExists) {
      assert(addColumns.contains("stats"))
    } else {
      assert(!addColumns.contains("stats"))
    }
    unexpected.foreach { colName =>
      assert(!addColumns.contains(colName), s"$colName shouldn't be part of the " +
        "schema because it is of null type.")
    }
  }

  test("unpartitioned table - check stats") {
    val df = spark.range(10).withColumn("part", ('id / 2).cast("int"))

    val structStatsColumns = Seq(
      "stats_parsed.numRecords" -> LongType,
      "stats_parsed.minValues.id" -> LongType,
      "stats_parsed.maxValues.id" -> LongType,
      "stats_parsed.nullCount.id" -> LongType,
      "stats_parsed.minValues.part" -> IntegerType,
      "stats_parsed.maxValues.part" -> IntegerType,
      "stats_parsed.nullCount.part" -> LongType)

    checkpointSchemaForTable(df)(
      checkpointingFns = checkpointFnsWithStructAndJsonStats,
      expectedCols = structStatsColumns,
      additionalValidationFn = addColumns => {
        checkFields(addColumns, statsAsJsonExists = true, Nil)
      }
    )

    checkpointSchemaForTable(df)(
      checkpointingFns = checkpointFnsWithStructWithoutJsonStats,
      expectedCols = structStatsColumns,
      additionalValidationFn = addColumns => {
        checkFields(addColumns, statsAsJsonExists = false, Nil)
      }
    )

    checkpointSchemaForTable(df)(
      checkpointingFns = checkpointFnsWithoutStructWithJsonStats,
      expectedCols = Nil,
      additionalValidationFn = addColumns => {
        checkFields(
          addColumns,
          statsAsJsonExists = true,
          unexpected = Seq(Checkpoints.STRUCT_PARTITIONS_COL_NAME) ++ structStatsColumns.map(_._1))
      }
    )

    checkpointSchemaForTable(df)(
      checkpointingFns = Seq(checkpointWithoutStats),
      expectedCols = Nil,
      additionalValidationFn = addColumns => {
        checkFields(addColumns, statsAsJsonExists = false, structStatsColumns.map(_._1))
      }
    )
  }

  test("use kill switch to disable stats as struct in checkpoint") {
    withSQLConf(DeltaSQLConf.STATS_AS_STRUCT_IN_CHECKPOINT_FORCE_DISABLED.key -> "true") {
      val df = spark.range(10).withColumn("part", ('id / 2).cast("int"))

      val structStatsColumns = Seq(
        "stats_parsed.numRecords",
        "stats_parsed.minValues.id",
        "stats_parsed.maxValues.id",
        "stats_parsed.nullCount.id",
        "stats_parsed.minValues.part",
        "stats_parsed.maxValues.part",
        "stats_parsed.nullCount.part")

      checkpointSchemaForTable(df)(
        checkpointingFns = checkpointFnsWithStructAndJsonStats,
        expectedCols = Nil,
        additionalValidationFn = addColumns => {
          checkFields(
            addColumns,
            statsAsJsonExists = true,
            unexpected = structStatsColumns
          )
        }
      )
    }
  }

  test("partitioned table - check stats") {
    val df = spark.range(10).withColumn("part", ('id / 2).cast("int"))

    val structStatsColumns = Seq(
      "stats_parsed.numRecords" -> LongType,
      "stats_parsed.minValues.id" -> LongType,
      "stats_parsed.maxValues.id" -> LongType,
      "stats_parsed.nullCount.id" -> LongType)

    // partitioned by "part"
    checkpointSchemaForTable(df, "part")(
      checkpointingFns = checkpointFnsWithStructAndJsonStats,
      expectedCols = structStatsColumns,
      additionalValidationFn = addColumns => {
        checkFields(addColumns, statsAsJsonExists = true, Nil)
      }
    )

    checkpointSchemaForTable(df, "part")(
      checkpointingFns = checkpointFnsWithStructWithoutJsonStats,
      expectedCols = structStatsColumns,
      additionalValidationFn = addColumns => {
        checkFields(addColumns, statsAsJsonExists = false, Nil)
      }
    )

    checkpointSchemaForTable(df, "part")(
      checkpointingFns = checkpointFnsWithoutStructWithJsonStats,
      expectedCols = Nil,
      additionalValidationFn = addColumns => {
        checkFields(
          addColumns,
          statsAsJsonExists = true,
          structStatsColumns.map(_._1))
      }
    )

    checkpointSchemaForTable(df, "part")(
      checkpointingFns = Seq(checkpointWithoutStats),
      expectedCols = Nil,
      additionalValidationFn = addColumns => {
        checkFields(addColumns, statsAsJsonExists = false, structStatsColumns.map(_._1))
      }
    )
  }

  test("nested fields, dots, and boolean types") {
    val df = spark.range(10).withColumn("part", ('id / 2).cast("int"))
      .withColumn("struct", struct($"id", $"part", $"id".as("with.dot")))
      .withColumn("bool", lit(true))

    val structColumns = Seq(
      "partitionValues_parsed.part" -> IntegerType,
      "stats_parsed.numRecords" -> LongType,
      "stats_parsed.minValues.id" -> LongType,
      "stats_parsed.maxValues.id" -> LongType,
      "stats_parsed.nullCount.id" -> LongType,
      "stats_parsed.minValues.struct.id" -> LongType,
      "stats_parsed.maxValues.struct.id" -> LongType,
      "stats_parsed.nullCount.struct.id" -> LongType,
      "stats_parsed.minValues.struct.part" -> IntegerType,
      "stats_parsed.maxValues.struct.part" -> IntegerType,
      "stats_parsed.nullCount.struct.part" -> LongType,
      "stats_parsed.minValues.struct.`with.dot`" -> LongType,
      "stats_parsed.maxValues.struct.`with.dot`" -> LongType,
      "stats_parsed.nullCount.struct.`with.dot`" -> LongType,
      "stats_parsed.nullCount.bool" -> LongType)

    val unexpectedCols = Seq(
      "stats_parsed.minValues.bool",
      "stats_parsed.maxValues.bool"
    )

    // partitioned by "part"
    checkpointSchemaForTable(df, "part")(
      checkpointingFns = checkpointFnsWithStructAndJsonStats,
      expectedCols = structColumns,
      additionalValidationFn = addColumns => {
        checkFields(addColumns, statsAsJsonExists = true, unexpectedCols)
      }
    )

    checkpointSchemaForTable(df, "part")(
      checkpointingFns = checkpointFnsWithStructWithoutJsonStats,
      expectedCols = structColumns,
      additionalValidationFn = addColumns => {
        checkFields(addColumns, statsAsJsonExists = false, unexpectedCols)
      }
    )

    checkpointSchemaForTable(df, "part")(
      checkpointingFns = checkpointFnsWithoutStructWithJsonStats,
      expectedCols = Nil,
      additionalValidationFn = addColumns => {
        checkFields(
          addColumns,
          statsAsJsonExists = true,
          unexpectedCols ++ structColumns.map(_._1))
      }
    )

    checkpointSchemaForTable(df, "part")(
      checkpointingFns = Seq(checkpointWithoutStats),
      expectedCols = Nil,
      additionalValidationFn = addColumns => {
        checkFields(
          addColumns,
          statsAsJsonExists = false,
          unexpectedCols ++ structColumns.map(_._1))
      }
    )
  }

  test("special characters - check stats") {
    val weirdName1 = "part%!@#_$%^&*-"
    val weirdName2 = "part?_.+<>|/"
    val df = spark.range(10)
      .withColumn(weirdName1, ('id / 2).cast("int"))
      .withColumn(weirdName2, ('id / 3).cast("int"))
      .withColumn("struct", struct($"id", col(weirdName1), $"id".as(weirdName2)))

    val structColumns = Seq(
      "stats_parsed.numRecords" -> LongType,
      "stats_parsed.minValues.id" -> LongType,
      "stats_parsed.maxValues.id" -> LongType,
      "stats_parsed.nullCount.id" -> LongType,
      "stats_parsed.minValues.struct.id" -> LongType,
      "stats_parsed.maxValues.struct.id" -> LongType,
      "stats_parsed.nullCount.struct.id" -> LongType,
      s"stats_parsed.minValues.struct.$weirdName1" -> IntegerType,
      s"stats_parsed.maxValues.struct.$weirdName1" -> IntegerType,
      s"stats_parsed.nullCount.struct.$weirdName1" -> LongType,
      s"stats_parsed.minValues.struct.`$weirdName2`" -> LongType,
      s"stats_parsed.maxValues.struct.`$weirdName2`" -> LongType,
      s"stats_parsed.nullCount.struct.`$weirdName2`" -> LongType,
      s"partitionValues_parsed.$weirdName1" -> IntegerType,
      s"partitionValues_parsed.`$weirdName2`" -> IntegerType)

    // partitioned by weirdName1, weirdName2
    checkpointSchemaForTable(df, weirdName1, weirdName2)(
      checkpointingFns = checkpointFnsWithStructAndJsonStats,
      expectedCols = structColumns,
      additionalValidationFn = addColumns => {
        checkFields(addColumns, statsAsJsonExists = true, Nil)
      }
    )

    checkpointSchemaForTable(df, weirdName1, weirdName2)(
      checkpointingFns = checkpointFnsWithStructWithoutJsonStats,
      expectedCols = structColumns,
      additionalValidationFn = addColumns => {
        checkFields(addColumns, statsAsJsonExists = false, Nil)
      }
    )

    checkpointSchemaForTable(df, weirdName1, weirdName2)(
      checkpointingFns = checkpointFnsWithoutStructWithJsonStats,
      expectedCols = Nil,
      additionalValidationFn = addColumns => {
        checkFields(
          addColumns,
          statsAsJsonExists = true,
          structColumns.map(_._1))
      }
    )

    checkpointSchemaForTable(df, weirdName1, weirdName2)(
      checkpointingFns = Seq(checkpointWithoutStats),
      expectedCols = Nil,
      additionalValidationFn = addColumns => {
        checkFields(
          addColumns,
          statsAsJsonExists = false,
          structColumns.map(_._1))
      }
    )
  }

  test("no data column stats collected + unpartitioned") {
    val df = spark.range(10).withColumn("part", ('id / 2).cast("int"))
      .withColumn("struct", struct($"id", $"part", $"id".as("with.dot")))
      .withColumn("bool", lit(true))

    val expectedColumns = Seq("stats_parsed.numRecords" -> LongType)

    val unexpected = Seq(
      "stats_parsed.minValues",
      "stats_parsed.maxValues",
      "stats_parsed.nullCount",
      "stats_parsed.minValues.id",
      "stats_parsed.maxValues.id",
      "stats_parsed.nullCount.id",
      "stats_parsed.minValues.struct.id",
      "stats_parsed.maxValues.struct.id",
      "stats_parsed.nullCount.struct.id",
      "stats_parsed.minValues.struct.part",
      "stats_parsed.maxValues.struct.part",
      "stats_parsed.nullCount.struct.part",
      "stats_parsed.minValues.struct.`with.dot`",
      "stats_parsed.maxValues.struct.`with.dot`",
      "stats_parsed.nullCount.struct.`with.dot`",
      "stats_parsed.nullCount.bool",
      "stats_parsed.minValues.bool",
      "stats_parsed.maxValues.bool",
      "partitionValues_parsed")

    withSQLConf(s"${DeltaConfigs.sqlConfPrefix}dataSkippingNumIndexedCols" -> "0") {
      checkpointSchemaForTable(df)(
        checkpointingFns = checkpointFnsWithStructAndJsonStats,
        expectedCols = expectedColumns,
        additionalValidationFn = addColumns => {
          // None of the stats column should exist instead of numRecords
          checkFields(addColumns, statsAsJsonExists = true, unexpected)
        }
      )

      checkpointSchemaForTable(df)(
        checkpointingFns = checkpointFnsWithStructWithoutJsonStats,
        expectedCols = expectedColumns,
        additionalValidationFn = addColumns => {
          // None of the stats column should exist instead of numRecords
          checkFields(addColumns, statsAsJsonExists = false, unexpected)
        }
      )

      checkpointSchemaForTable(df)(
        checkpointingFns = checkpointFnsWithoutStructWithJsonStats,
        expectedCols = Nil,
        additionalValidationFn = addColumns => {
          checkFields(
            addColumns,
            statsAsJsonExists = true,
            unexpected :+ "stats_parsed.numRecords")
        }
      )

      checkpointSchemaForTable(df)(
        checkpointingFns = Seq(checkpointWithoutStats),
        expectedCols = Nil,
        additionalValidationFn = addColumns => {
          checkFields(
            addColumns,
            statsAsJsonExists = false,
            unexpected :+ "stats_parsed.numRecords")
        }
      )
    }
  }

  test("checkpoint read succeeds with column pruning disabled") {
    withTempDir { dir =>
      // Populate the table with three commits, take a checkpoint at v1, and delete v0. Otherwise,
      // if the bug we test for caused snapshot construction to fail, Delta would silently retry
      // without the checkpoint, and the test would always appear to succeed.
      spark.range(1000).write.format("delta").save(dir.getCanonicalPath)
      spark.range(1000).write.format("delta").mode("append").save(dir.getCanonicalPath)
      val deltaLog = DeltaLog.forTable(spark, dir)
      deltaLog.checkpoint()
      spark.range(1000).write.format("delta").mode("append").save(dir.getCanonicalPath)
      val firstCommit = deltaLog.store
        .listFrom(FileNames.listingPrefix(deltaLog.logPath, 0), deltaLog.newDeltaHadoopConf())
        .find(f => FileNames.isDeltaFile(f.getPath) && FileNames.deltaVersion(f.getPath) == 0)
      assert(new File(firstCommit.get.getPath.toUri).delete())
      DeltaLog.clearCache()

      // Trigger both metadata reconstruction and state reconstruction queries with column pruning
      // disabled. We must also disable reading metadata from .crc file, for the former case.
      withSQLConf(
        SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
          org.apache.spark.sql.catalyst.optimizer.ColumnPruning.ruleName,
        DeltaSQLConf.USE_PROTOCOL_AND_METADATA_FROM_CHECKSUM_ENABLED.key -> "false") {

        // NOTE: Just creating the snapshot should already trigger metadata reconstruction, but we
        // still access it directly just to be extra sure.
        logInfo("About to create a new snapshot")
        val snapshot = DeltaLog.forTable(spark, dir).snapshot
        logInfo("About to access metadata")
        snapshot.metadata
        logInfo("About to access withStats")
        snapshot.withStats.count()
        logInfo("About to trigger state reconstrution")
        snapshot.stateDF
      }
    }
  }

  Seq(Seq("part"), Nil).foreach { partitionBy =>
    test("do not lose file stats after a checkpoint when writeStatsAsJson=false - isPartitioned: " +
        partitionBy.nonEmpty) {
      withTempDir { dir =>
        var start = 0
        def writeNewData(mode: String): Unit = {
          spark.range(start, start + 10).withColumn("part", 'id % 4)
            .write
            .format("delta")
            .mode(mode)
            .partitionBy(partitionBy: _*)
            .save(dir.getCanonicalPath)
          start += 10
        }
        writeNewData("append")
        val deltaLog = DeltaLog.forTable(spark, dir)
        checkpointWithProperty(writeStatsAsJson = Some(false))(deltaLog)

        def checkpointAndCheck(): Unit = {
          deltaLog.checkpoint()
          val checkpoint = spark.read.parquet(
            FileNames.checkpointFileSingular(deltaLog.logPath, deltaLog.snapshot.version).toString)

          // use physical name if possible
          val id = getPhysicalName("id", deltaLog.snapshot.schema)
          val adds = checkpoint.where("add is not null").selectExpr("add.*")
          assert(adds.selectExpr(s"stats_parsed.minValues.`$id`")
            .collect().forall(r => !r.isNullAt(0)),
            "minValues was null for some values.\n" + adds.collect().mkString("\n"))
          assert(adds.selectExpr(s"stats_parsed.maxValues.`$id`")
            .collect().forall(r => !r.isNullAt(0)),
            "maxValues was null for some values.\n" + adds.collect().mkString("\n"))
          assert(adds.selectExpr(s"stats_parsed.nullCount.`$id`")
            .collect().forall(r => !r.isNullAt(0)),
            "nullCount was null for some values.\n" + adds.collect().mkString("\n"))

          checkAnswer(
            adds.select("path"),
            deltaLog.snapshot.allFiles.select("path")
          )
        }

        writeNewData("append")
        checkpointAndCheck()
        writeNewData("overwrite")
        checkpointAndCheck()
      }
    }
  }

  test("switching between v1 and v2 checkpoints") {
    withTempDir { dir =>
      spark.range(0, 10).withColumn("part", 'id % 4).write.format("delta").partitionBy("part")
        .save(dir.getCanonicalPath)
      val deltaLog = DeltaLog.forTable(spark, dir)
      sql(s"ALTER TABLE delta.`${dir.getCanonicalPath}` SET TBLPROPERTIES " +
        s"('delta.checkpoint.writeStatsAsStruct'='true')")
      deltaLog.checkpoint()
      def getLatestCheckpoint: DataFrame = spark.read.parquet(
        FileNames.checkpointFileSingular(deltaLog.logPath, deltaLog.snapshot.version).toString)

      // statsAsStruct=true, statsAsJson=true
      val withStructAndJson = getLatestCheckpoint

      sql(s"ALTER TABLE delta.`${dir.getCanonicalPath}` SET TBLPROPERTIES " +
        s"('delta.checkpoint.writeStatsAsStruct'='false')")
      deltaLog.checkpoint()
      // statsAsStruct=false, statsAsJson=true
      val noStructWithJson = getLatestCheckpoint
      // The columns should be the same, without the stats_parsed column in noStructWithJson
      checkAnswer(
        noStructWithJson.select("add.*")
          .select("path", "partitionValues", "modificationTime", "tags", "stats"),
        withStructAndJson.select("add.*")
          .select("path", "partitionValues", "modificationTime", "tags", "stats")
      )

      sql(s"ALTER TABLE delta.`${dir.getCanonicalPath}` SET TBLPROPERTIES " +
        s"('delta.checkpoint.writeStatsAsStruct'='true'," +
        s"'delta.checkpoint.writeStatsAsJson'='false')")
      deltaLog.checkpoint()
      // statsAsStruct=true, statsAsJson=false
      val withStructNoJson = getLatestCheckpoint
      checkAnswer(
        withStructNoJson.select("add.*"),
        withStructAndJson.select("add.*").drop("stats")
      )

      sql(s"ALTER TABLE delta.`${dir.getCanonicalPath}` SET TBLPROPERTIES " +
        s"('delta.checkpoint.writeStatsAsStruct'='false')")
      deltaLog.checkpoint()
      // statsAsStruct=false, statsAsJson=false
      val noStructNoJson = getLatestCheckpoint
      // should not have the stats column anymore
      checkAnswer(
        noStructNoJson.select("add.*"),
        noStructWithJson.select("add.*").drop("stats"))

      // going to a v2 checkpoint with the json stats
      sql(s"ALTER TABLE delta.`${dir.getCanonicalPath}` SET TBLPROPERTIES " +
        s"('delta.checkpoint.writeStatsAsStruct'='true'," +
        s"'delta.checkpoint.writeStatsAsJson'='true')")
      deltaLog.checkpoint()
      // statsAsStruct=true, statsAsJson=true
      val lostAllStats = getLatestCheckpoint
      // should be identical to withStructAndJson
      checkAnswer(
        lostAllStats.select("add.*"),
        withStructAndJson.select("add.*")
          .withColumn("stats", lit(null))
          .withColumn("stats_parsed", lit(null))
      )
    }
  }
}


class DeltaCheckpointWithStructColsNameColumnMappingSuite extends DeltaCheckpointWithStructColsSuite
  with DeltaColumnMappingEnableNameMode {

  override protected def runOnlyTests = Seq(
    "unpartitioned table",
    "partitioned table"
  )
}
