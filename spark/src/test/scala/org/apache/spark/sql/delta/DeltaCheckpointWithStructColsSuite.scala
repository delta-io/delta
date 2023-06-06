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

import org.apache.spark.sql.delta.schema.SchemaMergingUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.FileNames

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class DeltaCheckpointWithStructColsSuite
  extends QueryTest
  with SharedSparkSession  with DeltaColumnMappingTestUtils
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
}


class DeltaCheckpointWithStructColsNameColumnMappingSuite extends DeltaCheckpointWithStructColsSuite
  with DeltaColumnMappingEnableNameMode {

  override protected def runOnlyTests = Seq(
    "unpartitioned table",
    "partitioned table"
  )
}
