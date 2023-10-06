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

// scalastyle:off import.ordering.noEmptyLine
import java.io.File
import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.delta.commands.ConvertToDeltaCommand
import org.apache.spark.sql.delta.schema.SchemaMergingUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.iceberg.Table

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.types._
// scalastyle:on import.ordering.noEmptyLine

abstract class ConvertIcebergToDeltaPartitioningUtils
    extends QueryTest
    with ConvertIcebergToDeltaUtils {

  override protected val schemaDDL = "id bigint, data string, size int, ts timestamp, dt date"

  protected lazy val schemaColumnNames: Seq[String] = schema.map(_.name)

  /** Original iceberg data used to check the correctness of conversion. */
  protected def initRows: Seq[String] = Seq(
    "1L, 'abc', 100, cast('2021-06-01 18:00:00' as timestamp), cast('2021-06-01' as date)",
    "2L, 'ace', 200, cast('2022-07-01 20:00:00' as timestamp), cast('2022-07-01' as date)"
  )

  /** Data added into both iceberg and converted delta to check post-conversion consistency. */
  protected def incrRows: Seq[String] = Seq(
    "3L, 'acf', 300, cast('2023-07-01 03:00:00' as timestamp), cast('2023-07-01' as date)"
  )

  protected override def test(testName: String, testTags: org.scalatest.Tag*)
    (testFun: => Any)
    (implicit pos: org.scalactic.source.Position): Unit = {
    Seq("true", "false").foreach { flag =>
      val msg = if (flag == "true") "- with native partition values"
      else "- with inferred partition values"
      super.test(testName + msg, testTags : _*) {
        withSQLConf(DeltaSQLConf.DELTA_CONVERT_ICEBERG_USE_NATIVE_PARTITION_VALUES.key -> flag) {
          testFun
        }
      }(pos)
    }
  }

  /**
   * Creates an iceberg table with the default schema and the provided partition columns, writes
   * some original rows into the iceberg table for conversion.
   */
  protected def createIcebergTable(
      tableName: String,
      partitionColumns: Seq[String],
      withRows: Seq[String] = initRows): Unit = {
    val partitionClause =
      if (partitionColumns.nonEmpty) s"PARTITIONED BY (${partitionColumns.mkString(",")})" else ""
    spark.sql(s"CREATE TABLE $tableName ($schemaDDL) USING iceberg $partitionClause")

    withRows.foreach{ row => spark.sql(s"INSERT INTO $tableName VALUES ($row)") }
  }

  /**
   * Tests ConvertToDelta on the provided iceberg table, and checks both schema and data of the
   * converted delta table.
   *
   * @param tableName: the iceberg table name.
   * @param tablePath: the iceberg table path.
   * @param partitionSchemaDDL: the expected partition schema DDL.
   * @param deltaPath: the location for the converted delta table.
   */
  protected def testConvertToDelta(
      tableName: String,
      tablePath: String,
      partitionSchemaDDL: String,
      deltaPath: String): Unit = {
    // Convert at an external location to ease testing.
    ConvertToDeltaCommand(
      tableIdentifier = TableIdentifier(tablePath, Some("iceberg")),
      partitionSchema = None,
      collectStats = true,
      Some(deltaPath)).run(spark)

    // Check the converted table schema.
    validateConvertedSchema(
      readIcebergHadoopTable(tablePath),
      DeltaLog.forTable(spark, new Path(deltaPath)),
      StructType.fromDDL(partitionSchemaDDL))

    // Check converted data.
    checkAnswer(
      // The converted delta table will have partition columns.
      spark.sql(s"select ${schemaColumnNames.mkString(",")} from delta.`$deltaPath`"),
      spark.sql(s"select * from $tableName"))
  }

  /**
   * Checks partition-based file skipping on the iceberg table (as parquet) and the converted delta
   * table to verify post-conversion partition consistency.
   *
   * @param icebergTableName: the iceberg table name.
   * @param icebergTablePath: the iceberg table path.
   * @param deltaTablePath: the converted delta table path.
   * @param filterAndFiles: a map from filter expression to the expected number of scanned files.
   */
  protected def checkSkipping(
      icebergTableName: String,
      icebergTablePath: String,
      deltaTablePath: String,
      filterAndFiles: Map[String, Int] = Map.empty[String, Int]): Unit = {
    // Add the same data into both iceberg table and converted delta table.
    writeRows(icebergTableName, deltaTablePath, incrRows)

    // Disable file stats to check file skipping solely based on partition, please note this only
    // works for optimizable partition expressions, check 'optimizablePartitionExpressions.scala'
    // for the whole list of supported partition expressions.
    sql(
      s"""
         |ALTER TABLE delta.`$deltaTablePath`
         |SET TBLPROPERTIES (
         |  '${DeltaConfigs.DATA_SKIPPING_NUM_INDEXED_COLS.key}' = '0')""".stripMargin)

    // Always check full scan.
    (filterAndFiles ++ Map("" -> 3)).foreach { case (filter, numFilesScanned) =>
      val filterExpr = if (filter == "") "" else s"where $filter"
      checkAnswer(
        // The converted delta table will have partition columns.
        spark.sql(
          s"""SELECT ${schemaColumnNames.mkString(",")} FROM delta.`$deltaTablePath`
             | WHERE $filter""".stripMargin),
        spark.sql(s"SELECT * FROM $icebergTableName $filterExpr"))

      // Check the raw parquet partition directories written out by Iceberg
      checkAnswer(
        spark.sql(s"select * from parquet.`$icebergTablePath/data` $filterExpr"),
        spark.sql(s"select * from delta.`$deltaTablePath` $filterExpr"))

      assert(
        spark.sql(s"select * from delta.`$deltaTablePath` $filterExpr").inputFiles.length ==
          numFilesScanned)
    }
  }

  /**
   * Validates the table schema and partition schema of the iceberg table and the converted delta
   * table.
   */
  private def validateConvertedSchema(
      icebergTable: Table,
      convertedDeltaLog: DeltaLog,
      expectedPartitionSchema: StructType): Unit = {

    def mergeSchema(dataSchema: StructType, partitionSchema: StructType): StructType = {
      StructType(dataSchema.fields ++
        partitionSchema.fields.filter { partField =>
          !dataSchema.fields.exists(f => spark.sessionState.conf.resolver(partField.name, f.name))})
    }

    val columnIds = mutable.Set[Long]()
    val schemaWithoutMetadata =
      SchemaMergingUtils.transformColumns(convertedDeltaLog.update().schema) { (_, field, _) =>
        // all columns should have the columnID metadata
        assert(DeltaColumnMapping.hasColumnId(field))
        // all columns should have physical name metadata
        assert(DeltaColumnMapping.hasPhysicalName(field))
        // nest column ids should be distinct
        val id = DeltaColumnMapping.getColumnId(field)
        assert(!columnIds.contains(id))
        columnIds.add(id)
        // the id can either be a data schema id or a identity transform partition field
        // or it is generated because it's a non-identity transform partition field
        assert(
          Option(icebergTable.schema().findField(id)).map(_.name()).contains(field.name) ||
            icebergTable.spec().fields().asScala.map(_.name()).contains(field.name)
        )
        field.copy(metadata = Metadata.empty)
      }

    assert(schemaWithoutMetadata == mergeSchema(schema, expectedPartitionSchema))

    // check partition columns
    assert(
      expectedPartitionSchema.map(_.name) == convertedDeltaLog.update().metadata.partitionColumns)
  }

  /**
   * Writes the same rows into both the iceberg table and the converted delta table using the
   * default schema.
   */
  protected def writeRows(
      icebergTableName: String,
      deltaTablePath: String,
      rows: Seq[String]): Unit = {

    // Write Iceberg
    rows.foreach { row => spark.sql(s"INSERT INTO $icebergTableName VALUES ($row)") }

    // Write Delta
    rows.foreach { row =>
      val values = row.split(",")
      assert(values.length == schemaColumnNames.length)
      val valueAsColumns =
        values.zip(schemaColumnNames).map { case (value, column) => s"$value AS $column" }

      val df = spark.sql(valueAsColumns.mkString("SELECT ", ",", ""))
      df.write.format("delta").mode("append").save(deltaTablePath)
    }
  }
}

class ConvertIcebergToDeltaPartitioningSuite extends ConvertIcebergToDeltaPartitioningUtils {

  import testImplicits._

  test("partition by timestamp year") {
    withTable(table) {
      createIcebergTable(table, Seq("years(ts)"))

      withTempDir { dir =>
        testConvertToDelta(table, tablePath, "ts_year int", dir.getCanonicalPath)
        checkSkipping(
          table, tablePath, dir.getCanonicalPath,
          Map(
            "ts < cast('2021-06-01 00:00:00' as timestamp)" -> 1,
            "ts <= cast('2021-06-01 00:00:00' as timestamp)" -> 1,
            "ts > cast('2021-06-01 00:00:00' as timestamp)" -> 3,
            "ts > cast('2022-01-01 00:00:00' as timestamp)" -> 2)
        )
      }
    }
  }

  test("partition by date year") {
    withTable(table) {
      createIcebergTable(table, Seq("years(dt)"))

      withTempDir { dir =>
        testConvertToDelta(table, tablePath, "dt_year int", dir.getCanonicalPath)
        checkSkipping(
          table, tablePath, dir.getCanonicalPath,
          Map(
            "dt < cast('2021-06-01' as date)" -> 1,
            "dt <= cast('2021-06-01' as date)" -> 1,
            "dt > cast('2021-06-01' as date)" -> 3,
            "dt = cast('2022-08-01' as date)" -> 1)
        )
      }
    }
  }

  test("partition by timestamp day") {
    withTable(table) {
      createIcebergTable(table, Seq("days(ts)"))

      withTempDir { dir =>
        testConvertToDelta(table, tablePath, "ts_day date", dir.getCanonicalPath)
        checkSkipping(
          table, tablePath, dir.getCanonicalPath,
          Map("ts < cast('2021-07-01 00:00:00' as timestamp)" -> 1))
      }
    }
  }

  test("partition by date day") {
    withTable(table) {
      createIcebergTable(table, Seq("days(dt)"))

      withTempDir { dir =>
        testConvertToDelta(table, tablePath, "dt_day date", dir.getCanonicalPath)
        checkSkipping(
          table, tablePath, dir.getCanonicalPath,
          Map(
            "dt < cast('2021-06-01' as date)" -> 1,
            "dt <= cast('2021-06-01' as date)" -> 1,
            "dt > cast('2021-06-01' as date)" -> 3,
            "dt = cast('2022-07-01' as date)" -> 1)
        )
      }
    }
  }

  test("partition by truncate string") {
    withTable(table) {
      createIcebergTable(table, Seq("truncate(data, 2)"))

      withTempDir { dir =>
        testConvertToDelta(table, tablePath, "data_trunc string", dir.getCanonicalPath)
        checkSkipping(
          table, tablePath, dir.getCanonicalPath,
          Map(
            "data >= 'ac'" -> 2,
            "data >= 'ad'" -> 0
          )
        )
      }
    }
  }

  test("partition by truncate long and int") {
    withTable(table) {
      // Include both positive and negative long values in the rows: positive will be rounded up
      // while negative will be rounded down.
      val sampleRows = Seq(
        "111L, 'abc', 100, cast('2021-06-01 18:00:00' as timestamp), cast('2021-06-01' as date)",
        "-11L, 'ace', -10, cast('2022-07-01 20:00:00' as timestamp), cast('2022-07-01' as date)")
      createIcebergTable(table, Seq("truncate(id, 10)", "truncate(size, 8)"), sampleRows)

      withTempDir { dir =>
        val deltaPath = dir.getCanonicalPath
        testConvertToDelta(table, tablePath, "id_trunc long, size_trunc int", deltaPath)
        // TODO: make iceberg truncate partition expression optimizable and check file skipping.

        // Write the same rows again into the converted delta table and make sure the partition
        // value computed by delta are the same with iceberg.
        writeRows(table, deltaPath, sampleRows)
        checkAnswer(
          spark.sql(s"SELECT id_trunc, size_trunc FROM delta.`$deltaPath`"),
          Row(110L, 96) :: Row(-20L, -16) :: Row(110L, 96) :: Row(-20L, -16) :: Nil)
      }
    }
  }

  test("partition by identity") {
    withTable(table) {
      createIcebergTable(table, Seq("data"))

      withTempDir { dir =>
        val deltaPath = new File(dir, "delta-table").getCanonicalPath
        testConvertToDelta(table, tablePath, "data string", deltaPath)
        checkSkipping(table, tablePath, deltaPath)

        spark.read.format("delta").load(deltaPath).inputFiles.foreach { fileName =>
          val sourceFile = new File(fileName.stripPrefix("file:"))
          val targetFile = new File(dir, sourceFile.getName)
          FileUtils.copyFile(sourceFile, targetFile)
          val parquetFileSchema =
            spark.read.format("parquet").load(targetFile.getCanonicalPath).schema
          if (fileName.contains("acf")) { // new file written by delta
            SchemaMergingUtils.equalsIgnoreCaseAndCompatibleNullability(
              parquetFileSchema, StructType(schema.fields.filter(_.name != "data")))
          } else {
            SchemaMergingUtils.equalsIgnoreCaseAndCompatibleNullability(parquetFileSchema, schema)
          }
        }
      }
    }
  }

  test("df writes and Insert Into with composite partitioning") {
    withTable(table) {
      createIcebergTable(table, Seq("years(dt), truncate(data, 3), id"))

      withTempDir { dir =>
        val deltaPath = new File(dir, "/delta").getCanonicalPath
        testConvertToDelta(
          table,
          tablePath,
          "dt_year int, data_trunc string, id bigint",
          deltaPath)

        checkSkipping(
          table, tablePath, deltaPath,
          Map(
            "data >= 'ac'" -> 2,
            "data >= 'acg'" -> 0,
            "dt = cast('2022-07-01' as date) and data >= 'ac'" -> 1
          )
        )

        // for Dataframe, we don't need to explicitly mention partition columns
        Seq((4L, "bcddddd", 400,
          new Timestamp(TimeUnit.DAYS.toMillis(10)),
          new java.sql.Date(TimeUnit.DAYS.toMillis(10))))
          .toDF(schemaColumnNames: _*)
          .write.format("delta").mode("append").save(deltaPath)

        checkAnswer(
          spark.read.format("delta").load(deltaPath).where("id = 4")
            .select("id", "data", "dt_year", "data_trunc"),
          Row(
            4,
            "bcddddd",
            // generated partition columns
            1970, "bcd") :: Nil)

        val tempTablePath = dir.getCanonicalPath + "/temp"
        Seq((5, "c", 500,
          new Timestamp(TimeUnit.DAYS.toMillis(20)),
          new java.sql.Date(TimeUnit.DAYS.toMillis(20)))
        ).toDF(schemaColumnNames: _*)
          .write.format("delta").save(tempTablePath)

        val e = intercept[AnalysisException] {
          spark.sql(
            s"""
               | INSERT INTO delta.`$deltaPath`
               | SELECT * from delta.`$tempTablePath`
               |""".stripMargin)
        }
        assert(e.getMessage.contains("not enough data columns"))
      }
    }
  }

  test("partition by timestamp month") {
    withTable(table) {
      createIcebergTable(table, Seq("months(ts)"))

      withTempDir { dir =>
        testConvertToDelta(table, tablePath, "ts_month string", dir.getCanonicalPath)
        // Do NOT infer partition column type for ts_month and dt_month since: 2020-01 will be
        // inferred as a date and cast it to 2020-01-01.
        withSQLConf("spark.sql.sources.partitionColumnTypeInference.enabled" -> "false") {
          checkSkipping(
            table,
            tablePath,
            dir.getCanonicalPath,
            Map(
              "ts < cast('2021-06-01 00:00:00' as timestamp)" -> 1,
              "ts <= cast('2021-06-01 00:00:00' as timestamp)" -> 1,
              "ts > cast('2021-06-01 00:00:00' as timestamp)" -> 3,
              "ts >= cast('2021-06-01 00:00:00' as timestamp)" -> 3,
              "ts < cast('2021-05-01 00:00:00' as timestamp)" -> 0,
              "ts > cast('2021-07-01 00:00:00' as timestamp)" -> 2,
              "ts = cast('2023-07-30 00:00:00' as timestamp)" -> 1,
              "ts > cast('2023-08-01 00:00:00' as timestamp)" -> 0))
        }
      }
    }
  }

  test("partition by date month") {
    withTable(table) {
      createIcebergTable(table, Seq("months(dt)"))

      withTempDir { dir =>
        testConvertToDelta(table, tablePath, "dt_month string", dir.getCanonicalPath)
        // Do NOT infer partition column type for ts_month and dt_month since: 2020-01 will be
        // inferred as a date and cast it to 2020-01-01.
        withSQLConf("spark.sql.sources.partitionColumnTypeInference.enabled" -> "false") {
          checkSkipping(
            table, tablePath, dir.getCanonicalPath,
            Map(
              "dt < cast('2021-06-01' as date)" -> 1,
              "dt <= cast('2021-06-01' as date)" -> 1,
              "dt > cast('2021-06-01' as date)" -> 3,
              "dt >= cast('2021-06-01' as date)" -> 3,
              "dt < cast('2021-05-01' as date)" -> 0,
              "dt > cast('2021-07-01' as date)" -> 2,
              "dt = cast('2023-07-30' as date)" -> 1,
              "dt > cast('2023-08-01' as date)" -> 0))
        }
      }
    }
  }

  test("partition by timestamp hour") {
    withTable(table) {
      createIcebergTable(table, Seq("hours(ts)"))

      withTempDir { dir =>
        testConvertToDelta(table, tablePath, "ts_hour string", dir.getCanonicalPath)
        checkSkipping(table, tablePath, dir.getCanonicalPath,
          Map(
            "ts < cast('2021-06-01 18:00:00' as timestamp)" -> 1,
            "ts <= cast('2021-06-01 18:00:00' as timestamp)" -> 1,
            "ts > cast('2021-06-01 18:00:00' as timestamp)" -> 3,
            "ts >= cast('2021-06-01 18:30:00' as timestamp)" -> 3,
            "ts < cast('2021-06-01 17:59:59' as timestamp)" -> 0,
            "ts = cast('2021-06-01 18:30:10' as timestamp)" -> 1,
            "ts > cast('2022-07-01 20:00:00' as timestamp)" -> 2,
            "ts > cast('2023-07-01 02:00:00' as timestamp)" -> 1,
            "ts > cast('2023-07-01 04:00:00' as timestamp)" -> 0))
      }
    }
  }
}

/////////////////////////////////
// 5-DIGIT-YEAR TIMESTAMP TEST //
/////////////////////////////////
class ConvertIcebergToDeltaPartitioningFiveDigitYearSuite
  extends ConvertIcebergToDeltaPartitioningUtils {

  override protected def initRows: Seq[String] = Seq(
    "1, 'abc', 100, cast('13168-11-15 18:00:00' as timestamp), cast('13168-11-15' as date)",
    "2, 'abc', 200, cast('2021-08-24 18:00:00' as timestamp), cast('2021-08-24' as date)"
  )

  override protected def incrRows: Seq[String] = Seq(
    "3, 'acf', 300, cast('11267-07-15 18:00:00' as timestamp), cast('11267-07-15' as date)",
    "4, 'acf', 400, cast('2008-07-15 18:00:00' as timestamp), cast('2008-07-15' as date)"
  )

  /**
   * Checks filtering on 5-digit year based on different policies.
   *
   * @param icebergTableName: the iceberg table name.
   * @param deltaTablePath: the converted delta table path.
   * @param partitionSchemaDDL: the partition schema DDL.
   * @param policy: time parser policy to determine 5-digit year handling.
   * @param filters: a list of filter expressions to check.
   */
  private def checkFiltering(
      icebergTableName: String,
      deltaTablePath: String,
      partitionSchemaDDL: String,
      policy: String,
      filters: Seq[String]): Unit = {
    filters.foreach { filter =>
      val filterExpr = if (filter == "") "" else s"where $filter"
      if (policy == "EXCEPTION" && filterExpr != "" &&
        partitionSchemaDDL != "ts_year int" && partitionSchemaDDL != "ts_day date") {
        var thrownError = false
        val msg = try {
          spark.sql(s"select * from delta.`$deltaTablePath` $filterExpr").collect()
        } catch {
          case e: Throwable if e.isInstanceOf[org.apache.spark.SparkThrowable] &&
            e.getMessage.contains("spark.sql.legacy.timeParserPolicy") =>
            thrownError = true
          case other => throw other
        }
        assert(thrownError, s"Error message $msg is incorrect.")
      } else {
        // check results of iceberg == delta
        checkAnswer(
          // the converted delta table will have partition columns
          spark.sql(
            s"select ${schema.fields.map(_.name).mkString(",")} from delta.`$deltaTablePath`"),
          spark.sql(s"select * from $icebergTableName"))
      }
    }
  }

  Seq("EXCEPTION", "CORRECTED", "LEGACY").foreach { policy =>
    test(s"future timestamp: partition by month when timeParserPolicy is: $policy") {
      withSQLConf("spark.sql.legacy.timeParserPolicy" -> policy) {
        withTable(table) {
          createIcebergTable(table, Seq("months(ts)"))

          withTempDir { dir =>
            val partitionSchemaDDL = "ts_month string"
            testConvertToDelta(table, tablePath, partitionSchemaDDL, dir.getCanonicalPath)
            checkFiltering(
              table, dir.getCanonicalPath, partitionSchemaDDL, policy,
              Seq("",
                "ts > cast('2021-06-01 00:00:00' as timestamp)",
                "ts < cast('12000-06-01 00:00:00' as timestamp)",
                "ts >= cast('13000-06-01 00:00:00' as timestamp)",
                "ts <= cast('2009-06-01 00:00:00' as timestamp)",
                "ts = cast('11267-07-15 00:00:00' as timestamp)"
              )
            )
          }
        }
      }
    }

    test(s"future timestamp: partition by hour when timeParserPolicy is: $policy") {
      withSQLConf("spark.sql.legacy.timeParserPolicy" -> policy) {
        withTable(table) {
          createIcebergTable(table, Seq("hours(ts)"))

          withTempDir { dir =>
            val partitionSchemaDDL = "ts_hour string"
            testConvertToDelta(table, tablePath, partitionSchemaDDL, dir.getCanonicalPath)
            checkFiltering(
              table, dir.getCanonicalPath, partitionSchemaDDL, policy,
              Seq("",
                "ts > cast('2021-06-01 18:00:00' as timestamp)",
                "ts < cast('12000-06-01 18:00:00' as timestamp)",
                "ts >= cast('13000-06-01 19:00:00' as timestamp)",
                "ts <= cast('2009-06-01 16:00:00' as timestamp)",
                "ts = cast('11267-07-15 18:30:00' as timestamp)"
              )
            )
          }
        }
      }
    }

    test(s"future timestamp: partition by year when timeParserPolicy is: $policy") {
      withSQLConf("spark.sql.legacy.timeParserPolicy" -> policy) {
        withTable(table) {
          createIcebergTable(table, Seq("years(ts)"))

          withTempDir { dir =>
            val partitionSchemaDDL = "ts_year int"
            testConvertToDelta(table, tablePath, partitionSchemaDDL, dir.getCanonicalPath)
            checkFiltering(
              table, dir.getCanonicalPath, partitionSchemaDDL, policy,
              Seq("",
                "ts > cast('2021-06-01 18:00:00' as timestamp)",
                "ts < cast('12000-06-01 18:00:00' as timestamp)",
                "ts >= cast('13000-06-01 19:00:00' as timestamp)",
                "ts <= cast('2009-06-01 16:00:00' as timestamp)",
                "ts = cast('11267-07-15 18:30:00' as timestamp)"
              )
            )
          }
        }
      }
    }

    test(s"future timestamp: partition by day when timeParserPolicy is: $policy") {
      withSQLConf("spark.sql.legacy.timeParserPolicy" -> policy) {
        withTable(table) {
          createIcebergTable(table, Seq("days(ts)"))

          withTempDir { dir =>
            val partitionSchemaDDL = "ts_day date"
            testConvertToDelta(table, tablePath, partitionSchemaDDL, dir.getCanonicalPath)
            checkFiltering(
              table, dir.getCanonicalPath, partitionSchemaDDL, policy,
              Seq("",
                "ts > cast('2021-06-01 18:00:00' as timestamp)",
                "ts < cast('12000-06-01 18:00:00' as timestamp)",
                "ts >= cast('13000-06-01 19:00:00' as timestamp)",
                "ts <= cast('2009-06-01 16:00:00' as timestamp)",
                "ts = cast('11267-07-15 18:30:00' as timestamp)"
              )
            )
          }
        }
      }
    }
  }
}
