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
import java.sql.Date
import java.time.LocalDate
import java.time.LocalTime

import scala.collection.JavaConverters._
import scala.util.Try

import org.apache.spark.sql.delta.commands.convert.ConvertUtils
import org.apache.spark.sql.delta.schema.SchemaMergingUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.{DataSkippingDeltaTestsUtils, StatisticsCollection}
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.iceberg.Schema
import org.apache.iceberg.hadoop.HadoopTables
import org.apache.iceberg.spark.{SparkSchemaUtil => IcebergSparkSchemaUtil}
import org.apache.iceberg.types.Types
import org.apache.iceberg.types.Types.NestedField

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.util.DateTimeUtils.{stringToDate, toJavaDate}
import org.apache.spark.sql.functions.{col, expr, from_json, lit, struct, substring}
import org.apache.spark.sql.types.{Decimal, DecimalType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String
// scalastyle:on import.ordering.noEmptyLine

case class DeltaStatsClass(
    numRecords: Int,
    maxValues: Map[String, String],
    minValues: Map[String, String],
    nullCount: Map[String, Int])

trait CloneIcebergSuiteBase extends QueryTest
  with DataSkippingDeltaTestsUtils
  with ConvertIcebergToDeltaUtils {

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(DeltaSQLConf.DELTA_CONVERT_ICEBERG_PARTITION_EVOLUTION_ENABLED.key, "true")
  }

  protected val cloneTable = "clone"

  // The identifier of clone source, can be either path-based or name-based.
  protected def sourceIdentifier: String
  protected def supportedModes: Seq[String] = Seq("SHALLOW")

  protected def toDate(date: String): Date = {
    toJavaDate(stringToDate(UTF8String.fromString(date)).get)
  }

  protected def physicalNamesAreEqual(
    sourceSchema: StructType, targetSchema: StructType): Boolean = {

    val sourcePathToPhysicalName = SchemaMergingUtils.explode(sourceSchema).map {
      case (path, field) => path -> DeltaColumnMapping.getPhysicalName(field)
    }.toMap

    val targetPathToPhysicalName = SchemaMergingUtils.explode(targetSchema).map {
      case (path, field) => path -> DeltaColumnMapping.getPhysicalName(field)
    }.toMap

    targetPathToPhysicalName.foreach {
      case (path, physicalName) =>
        if (!sourcePathToPhysicalName.contains(path) ||
            physicalName != sourcePathToPhysicalName(path)) {
          return false
        }
    }

    sourcePathToPhysicalName.size == targetPathToPhysicalName.size
  }

  protected def testClone(testName: String)(f: String => Unit): Unit =
    supportedModes.foreach { mode => test(s"$testName - $mode") { f(mode) } }

  testClone("table with deleted files") { mode =>
    withTable(table, cloneTable) {
      spark.sql(
        s"""CREATE TABLE $table (id bigint, data string)
           |USING iceberg PARTITIONED BY (data)
           |TBLPROPERTIES ('write.format.default' = 'PARQUET')""".stripMargin)
      spark.sql(s"INSERT INTO $table VALUES (1, 'a'), (2, 'b'), (3, 'c')")
      spark.sql(s"DELETE FROM $table WHERE data > 'a'")
      checkAnswer(spark.sql(s"SELECT * from $table"), Row(1, "a") :: Nil)

      spark.sql(s"CREATE TABLE $cloneTable $mode CLONE $sourceIdentifier")

      assert(SchemaMergingUtils.equalsIgnoreCaseAndCompatibleNullability(
        DeltaLog.forTable(spark, TableIdentifier(cloneTable)).snapshot.schema,
        new StructType().add("id", LongType).add("data", StringType)))

      checkAnswer(spark.table(cloneTable), Row(1, "a") :: Nil)
    }
  }

  protected def runCreateOrReplace(mode: String, source: String): DataFrame = {
    Try(spark.sql(s"DELETE FROM $cloneTable"))
    spark.sql(s"CREATE OR REPLACE TABLE $cloneTable $mode CLONE $source")
  }

  testClone("table with renamed columns") { mode =>
    withTable(table, cloneTable) {
      spark.sql(
        s"""CREATE TABLE $table (id bigint, data string)
           |USING iceberg PARTITIONED BY (data)""".stripMargin)
      spark.sql(s"INSERT INTO $table VALUES (1, 'a'), (2, 'b')")
      spark.sql("ALTER TABLE local.db.table RENAME COLUMN id TO id2")
      spark.sql(s"INSERT INTO $table VALUES (3, 'c')")

      // Parquet files still have the old schema
        assert(
          SchemaMergingUtils.equalsIgnoreCaseAndCompatibleNullability(
            spark.read.format("parquet").load(tablePath + "/data").schema,
            new StructType().add("id", LongType).add("data", StringType)))

      runCreateOrReplace(mode, sourceIdentifier)
      // The converted delta table will get the updated schema
      assert(
        SchemaMergingUtils.equalsIgnoreCaseAndCompatibleNullability(
          DeltaLog.forTable(spark, TableIdentifier(cloneTable)).snapshot.schema,
          new StructType().add("id2", LongType).add("data", StringType)))

      checkAnswer(spark.table(cloneTable), Row(1, "a") :: Row(2, "b") :: Row(3, "c") :: Nil)
    }
  }

  testClone("create or replace table - same schema") { mode =>
    withTable(table, cloneTable) {
      spark.sql(
        s"""CREATE TABLE $table (id bigint, data string)
           |USING iceberg PARTITIONED BY (data)""".stripMargin)

      // Add some rows to check the initial CLONE.
      spark.sql(s"INSERT INTO $table VALUES (1, 'a'), (2, 'b')")
      runCreateOrReplace(mode, sourceIdentifier)
      checkAnswer(spark.table(cloneTable), Row(1, "a") :: Row(2, "b") :: Nil)

      // Add more rows to check incremental update with REPLACE.
      spark.sql(s"INSERT INTO $table VALUES (3, 'c')")
      runCreateOrReplace(mode, sourceIdentifier)
      checkAnswer(spark.table(cloneTable), Row(1, "a") :: Row(2, "b") :: Row(3, "c") :: Nil)
    }
  }

  testClone("create or replace table - renamed column") { mode =>
    withTable(table, cloneTable) {
      spark.sql(
        s"""CREATE TABLE $table (id bigint, data string)
           |USING iceberg PARTITIONED BY (data)""".stripMargin)

      spark.sql(s"INSERT INTO $table VALUES (1, 'a'), (2, 'b')")
      runCreateOrReplace(mode, sourceIdentifier)
      assert(
        SchemaMergingUtils.equalsIgnoreCaseAndCompatibleNullability(
          DeltaLog.forTable(spark, TableIdentifier(cloneTable)).snapshot.schema,
          new StructType().add("id", LongType).add("data", StringType)))

      checkAnswer(spark.table(cloneTable), Row(1, "a") :: Row(2, "b") :: Nil)

      // Rename column 'id' into column 'id2'.
      spark.sql("ALTER TABLE local.db.table RENAME COLUMN id TO id2")
      spark.sql(s"INSERT INTO $table VALUES (3, 'c')")

      // Update the cloned delta table with REPLACE.
      runCreateOrReplace(mode, sourceIdentifier)
      assert(
        SchemaMergingUtils.equalsIgnoreCaseAndCompatibleNullability(
          DeltaLog.forTable(spark, TableIdentifier(cloneTable)).snapshot.schema,
          new StructType().add("id2", LongType).add("data", StringType)))

      checkAnswer(spark.table(cloneTable), Row(1, "a") :: Row(2, "b") :: Row(3, "c") :: Nil)
    }
  }

  testClone("create or replace table - deleted rows") { mode =>
    withTable(table, cloneTable) {
      spark.sql(
        s"""CREATE TABLE $table (id bigint, data string)
           |USING iceberg PARTITIONED BY (data)""".stripMargin)

      spark.sql(s"INSERT INTO $table VALUES (1, 'a'), (2, 'b'), (3, 'c')")
      runCreateOrReplace(mode, sourceIdentifier)
      checkAnswer(spark.table(cloneTable), Row(1, "a") :: Row(2, "b") :: Row(3, "c") :: Nil)

      // Delete some rows from the iceberg table.
      spark.sql(s"DELETE FROM $table WHERE data > 'a'")
      checkAnswer(
        spark.sql(s"SELECT * from $table"), Row(1, "a") :: Nil)

      runCreateOrReplace(mode, sourceIdentifier)
      checkAnswer(spark.table(cloneTable), Row(1, "a") :: Nil)
    }
  }

  testClone("create or replace table - schema with nested column") { mode =>
    withTable(table, cloneTable) {
      spark.sql(
        s"""CREATE TABLE $table (id bigint, person struct<name:string,phone:int>)
           |USING iceberg PARTITIONED BY (truncate(person.name, 2))""".stripMargin)

      spark.sql(s"INSERT INTO $table VALUES (1, ('AaAaAa', 10)), (2, ('BbBbBb', 20))")
      runCreateOrReplace(mode, sourceIdentifier)
      checkAnswer(
        spark.table(cloneTable),
        Row(1, Row("AaAaAa", 10), "Aa") :: Row(2, Row("BbBbBb", 20), "Bb") :: Nil)

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier(cloneTable))
      val schemaBefore = deltaLog.update().schema

      spark.sql(s"INSERT INTO $table VALUES (3, ('AaZzZz', 30)), (4, ('CcCcCc', 40))")
      runCreateOrReplace(mode, sourceIdentifier)
      checkAnswer(
        spark.table(cloneTable),
        Row(1, Row("AaAaAa", 10), "Aa") :: Row(2, Row("BbBbBb", 20), "Bb") ::
          Row(3, Row("AaZzZz", 30), "Aa") :: Row(4, Row("CcCcCc", 40), "Cc") :: Nil)

      assert(physicalNamesAreEqual(schemaBefore, deltaLog.update().schema))
    }
  }

  testClone("create or replace table - add partition field") { mode =>
    withTable(table, cloneTable) {
      spark.sql(
        s"""CREATE TABLE $table (date date, id bigint, category string, price double)
           | USING iceberg PARTITIONED BY (date)""".stripMargin)

      // scalastyle:off deltahadoopconfiguration
      val hadoopTables = new HadoopTables(spark.sessionState.newHadoopConf())
      // scalastyle:on deltahadoopconfiguration
      val icebergTable = hadoopTables.load(tablePath)
      val icebergTableSchema =
        IcebergSparkSchemaUtil.convert(icebergTable.schema())

      val df1 = spark.createDataFrame(
        Seq(
          Row(toDate("2022-01-01"), 1L, "toy", 2.5D),
          Row(toDate("2022-01-01"), 2L, "food", 0.6D),
          Row(toDate("2022-02-05"), 3L, "food", 1.4D),
          Row(toDate("2022-02-05"), 4L, "toy", 10.2D)).asJava,
        icebergTableSchema)

      df1.writeTo(table).append()

      runCreateOrReplace(mode, sourceIdentifier)
      val deltaLog = DeltaLog.forTable(spark, TableIdentifier(cloneTable))
      assert(deltaLog.snapshot.metadata.partitionColumns == Seq("date"))
      checkAnswer(spark.table(cloneTable), df1)

      // Add a new partition field from the existing column "category"
      icebergTable.refresh()
      icebergTable.updateSpec().addField("category").commit()

      // Invalidate cache and load the updated partition spec
      spark.sql(s"REFRESH TABLE $table")
      val df2 = spark.createDataFrame(
        Seq(
          Row(toDate("2022-02-05"), 5L, "toy", 5.8D),
          Row(toDate("2022-06-04"), 6L, "toy", 20.1D)).asJava,
        icebergTableSchema)

      df2.writeTo(table).append()

      runCreateOrReplace(mode, sourceIdentifier)
      assert(deltaLog.update().metadata.partitionColumns == Seq("date", "category"))
      // Old data of cloned Delta table has null on the new partition field.
      checkAnswer(spark.table(cloneTable), df1.withColumn("category", lit(null)).union(df2))
      // Iceberg table projects existing value of old data to the new partition field though.
      checkAnswer(spark.sql(s"SELECT * FROM $table"), df1.union(df2))
    }
  }

  testClone("create or replace table - remove partition field") { mode =>
    withTable(table, cloneTable) {
      spark.sql(
        s"""CREATE TABLE $table (date date, id bigint, category string, price double)
           | USING iceberg PARTITIONED BY (date)""".stripMargin)

      // scalastyle:off deltahadoopconfiguration
      val hadoopTables = new HadoopTables(spark.sessionState.newHadoopConf())
      // scalastyle:on deltahadoopconfiguration
      val icebergTable = hadoopTables.load(tablePath)
      val icebergTableSchema =
        IcebergSparkSchemaUtil.convert(icebergTable.schema())

      val df1 = spark.createDataFrame(
        Seq(
          Row(toDate("2022-01-01"), 1L, "toy", 2.5D),
          Row(toDate("2022-01-01"), 2L, "food", 0.6D),
          Row(toDate("2022-02-05"), 3L, "food", 1.4D),
          Row(toDate("2022-02-05"), 4L, "toy", 10.2D)).asJava,
        icebergTableSchema)

      df1.writeTo(table).append()

      runCreateOrReplace(mode, sourceIdentifier)
      val deltaLog = DeltaLog.forTable(spark, TableIdentifier(cloneTable))
      assert(deltaLog.snapshot.metadata.partitionColumns == Seq("date"))
      checkAnswer(spark.table(cloneTable), df1)

      // Remove the partition field "date"
      icebergTable.refresh()
      icebergTable.updateSpec().removeField("date").commit()

      // Invalidate cache and load the updated partition spec
      spark.sql(s"REFRESH TABLE $table")
      val df2 = spark.createDataFrame(
        Seq(
          Row(toDate("2022-02-05"), 5L, "toy", 5.8D),
          Row(toDate("2022-06-04"), 6L, "toy", 20.1D)).asJava,
        icebergTableSchema)

      df2.writeTo(table).append()

      runCreateOrReplace(mode, sourceIdentifier)
      assert(deltaLog.update().metadata.partitionColumns.isEmpty)
      // Both cloned Delta table and Iceberg table has data for the removed partition field.
      checkAnswer(spark.table(cloneTable), df1.union(df2))
      checkAnswer(spark.table(cloneTable), spark.sql(s"SELECT * FROM $table"))
    }
  }

  testClone("create or replace table - replace partition field") { mode =>
    withTable(table, cloneTable) {
      spark.sql(
        s"""CREATE TABLE $table (date date, id bigint, category string, price double)
           | USING iceberg PARTITIONED BY (date)""".stripMargin)

      // scalastyle:off deltahadoopconfiguration
      val hadoopTables = new HadoopTables(spark.sessionState.newHadoopConf())
      // scalastyle:on deltahadoopconfiguration
      val icebergTable = hadoopTables.load(tablePath)
      val icebergTableSchema =
        IcebergSparkSchemaUtil.convert(icebergTable.schema())

      val df1 = spark.createDataFrame(
        Seq(
          Row(toDate("2022-01-01"), 1L, "toy", 2.5D),
          Row(toDate("2022-01-01"), 2L, "food", 0.6D),
          Row(toDate("2022-02-05"), 3L, "food", 1.4D),
          Row(toDate("2022-02-05"), 4L, "toy", 10.2D)).asJava,
        icebergTableSchema)

      df1.writeTo(table).append()

      runCreateOrReplace(mode, sourceIdentifier)
      val deltaLog = DeltaLog.forTable(spark, TableIdentifier(cloneTable))
      assert(deltaLog.snapshot.metadata.partitionColumns == Seq("date"))
      checkAnswer(spark.table(cloneTable), df1)

      // Replace the partition field "date" with a transformed field "month(date)"
      icebergTable.refresh()
      icebergTable.updateSpec().removeField("date")
        .addField(org.apache.iceberg.expressions.Expressions.month("date"))
        .commit()

      // Invalidate cache and load the updated partition spec
      spark.sql(s"REFRESH TABLE $table")
      val df2 = spark.createDataFrame(
        Seq(
          Row(toDate("2022-02-05"), 5L, "toy", 5.8D),
          Row(toDate("2022-06-04"), 6L, "toy", 20.1D)).asJava,
        icebergTableSchema)

      df2.writeTo(table).append()

      runCreateOrReplace(mode, sourceIdentifier)
      assert(deltaLog.update().metadata.partitionColumns == Seq("date_month"))
      // Old data of cloned Delta table has null on the new partition field.
      checkAnswer(spark.table(cloneTable),
        df1.withColumn("date_month", lit(null))
          .union(df2.withColumn("date_month", substring(col("date") cast "String", 1, 7))))
      // The new partition field is a hidden metadata column in Iceberg.
      checkAnswer(
        spark.table(cloneTable).drop("date_month"),
        spark.sql(s"SELECT * FROM $table"))
    }
  }

  testClone("Enables column mapping table feature") { mode =>
    withTable(table, cloneTable) {
      spark.sql(
        s"""CREATE TABLE $table (id bigint, data string)
           |USING iceberg PARTITIONED BY (data)""".stripMargin)

      spark.sql(s"CREATE TABLE $cloneTable $mode CLONE $sourceIdentifier")
      val log = DeltaLog.forTable(spark, TableIdentifier(cloneTable))
      val protocol = log.update().protocol
      assert(protocol.isFeatureSupported(ColumnMappingTableFeature))
    }
  }

  testClone("Iceberg bucket partition should be converted to unpartitioned delta table") { mode =>
    withTable(table, cloneTable) {
      spark.sql(
        s"""CREATE TABLE $table (date date, id bigint, category string, price double)
           | USING iceberg PARTITIONED BY (bucket(2, id))""".stripMargin)

      // scalastyle:off deltahadoopconfiguration
      val hadoopTables = new HadoopTables(spark.sessionState.newHadoopConf())
      // scalastyle:on deltahadoopconfiguration
      val icebergTable = hadoopTables.load(tablePath)
      val icebergTableSchema =
        IcebergSparkSchemaUtil.convert(icebergTable.schema())

      val df1 = spark.createDataFrame(
        Seq(
          Row(toDate("2022-01-01"), 1L, "toy", 2.5D),
          Row(toDate("2022-01-01"), 2L, "food", 0.6D),
          Row(toDate("2022-02-05"), 3L, "food", 1.4D),
          Row(toDate("2022-02-05"), 4L, "toy", 10.2D)).asJava,
        icebergTableSchema)

      df1.writeTo(table).append()

      runCreateOrReplace(mode, sourceIdentifier)
      val deltaLog = DeltaLog.forTable(spark, TableIdentifier(cloneTable))
      assert(deltaLog.snapshot.metadata.partitionColumns.isEmpty)
      checkAnswer(spark.table(cloneTable), df1)
      checkAnswer(spark.sql(s"select * from $cloneTable where id = 1"), df1.where("id = 1"))

      // clone should fail with flag off
      withSQLConf(DeltaSQLConf.DELTA_CONVERT_ICEBERG_BUCKET_PARTITION_ENABLED.key -> "false") {
        df1.writeTo(table).append()
        val ae = intercept[UnsupportedOperationException] {
          runCreateOrReplace(mode, sourceIdentifier)
        }
        assert(ae.getMessage.contains("bucket partition"))
      }
    }
  }

  testClone("Convert Iceberg date stats from int to date string") { mode =>
    withSQLConf(DeltaSQLConf.DELTA_CONVERT_ICEBERG_STATS.key-> "true") {
      withTable(table, cloneTable) {
        // Create Iceberg table with date type
        spark.sql(
          s"""CREATE TABLE $table (col1 int, col2 date)
             | USING iceberg""".stripMargin)
        // Write into Iceberg table
        // scalastyle:off deltahadoopconfiguration
        val hadoopTables = new HadoopTables(spark.sessionState.newHadoopConf())
        // scalastyle:on deltahadoopconfiguration
        val icebergTable = hadoopTables.load(tablePath)
        val icebergTableSchema =
          IcebergSparkSchemaUtil.convert(icebergTable.schema())
        val df = spark.createDataFrame(
          Seq(
            Row(1, toDate("2015-01-25"))
          ).asJava,
          icebergTableSchema)
        df.writeTo(table).append()

        // Create cloned table
        runCreateOrReplace(mode, sourceIdentifier)
        checkAnswer(spark.table(cloneTable), df)
        // Check stats of cloned table
        val deltaLog = DeltaLog.forTable(
          spark,
          spark.sessionState.catalog.getTableMetadata(TableIdentifier(cloneTable))
        )
        val snapshot = deltaLog.unsafeVolatileSnapshot
        snapshot.allFiles.collectAsList().iterator().asScala.foreach { f =>
          val parsedStats = JsonUtils.fromJson[DeltaStatsClass](
            f.stats
          )
          assert(parsedStats.numRecords == 1)
          assert(parsedStats.minValues("col2") == "2015-01-25")
          assert(parsedStats.maxValues("col2") == "2015-01-25")
        }
        // Check state-reconstruction stats
        val analyzedDf = deltaLog.update().withStatsDeduplicated.queryExecution.analyzed.toString
        val statsCol = if (analyzedDf.contains("stats_parsed")) "stats_parsed" else "stats"
        val stats = snapshot.withStats.select(statsCol)
        assert(stats.select(s"$statsCol.minValues.col2").collect().head
          .getDate(0).toString == "2015-01-25")
        assert(stats.select(s"$statsCol.maxValues.col2").collect().head
          .getDate(0).toString == "2015-01-25")
      }
    }
  }

  testClone("DataSkipping on date type") { mode =>
    withSQLConf(DeltaSQLConf.DELTA_CONVERT_ICEBERG_STATS.key-> "true") {
      withTable(table, cloneTable) {
        // Create Iceberg table with date type
        spark.sql(
          s"""CREATE TABLE $table (col1 int, col2 date)
             | USING iceberg PARTITIONED BY (col2)""".stripMargin)
        // Write into Iceberg table
        // scalastyle:off deltahadoopconfiguration
        val hadoopTables = new HadoopTables(spark.sessionState.newHadoopConf())
        // scalastyle:on deltahadoopconfiguration
        val icebergTable = hadoopTables.load(tablePath)
        val icebergTableSchema =
          IcebergSparkSchemaUtil.convert(icebergTable.schema())

        val df = spark.createDataFrame(
          Seq(
            Row(1, toDate("2015-01-25")),
            Row(2, toDate("1917-02-10")),
            Row(3, toDate("2050-06-23"))
          ).asJava,
          icebergTableSchema)
        df.writeTo(table).append()
        runCreateOrReplace(mode, sourceIdentifier)
        // Check read results
        checkAnswer(spark.table(cloneTable), df)
        // Check data skipping - 1
        val deltaLog = DeltaLog.forTable(
          spark,
          spark.sessionState.catalog.getTableMetadata(TableIdentifier(cloneTable))
        )
        val predicate1 = "col2 > '2030-01-25'"
        val filesRead1 =
          getFilesRead(spark, deltaLog, predicate1, checkEmptyUnusedFilters = false)
        assert(filesRead1.size == 1)
        assert(filesRead1.head.partitionValues.head._2 == "2050-06-23")
        checkAnswer(spark.sql(s"select * from $cloneTable where $predicate1"), df.where(predicate1))
        // Check data skipping - 2
        val predicate2 = "col2 < '1917-02-11'"
        val filesRead2 =
          getFilesRead(spark, deltaLog, predicate2, checkEmptyUnusedFilters = false)
        assert(filesRead2.head.partitionValues.head._2 == "1917-02-10")
        checkAnswer(spark.sql(s"select * from $cloneTable where $predicate2"), df.where(predicate2))
      }
    }
  }
}

class CloneIcebergByPathSuite extends CloneIcebergSuiteBase
{
  override def sourceIdentifier: String = s"iceberg.`$tablePath`"

  test("negative case: select from iceberg table using path") {
    withTable(table) {
      val ae = intercept[AnalysisException] {
        sql(s"SELECT * FROM $sourceIdentifier")
      }
      assert(ae.getMessage.contains("does not support batch scan"))
    }
  }
}

/**
 * This suite test features in Iceberg that is not directly supported by Spark.
 * See also [[NonSparkIcebergTestUtils]].
 * We do not put these tests in or extend from [[CloneIcebergSuiteBase]] because they
 * use non-Spark way to create test data.
 */
class CloneNonSparkIcebergByPathSuite extends QueryTest
  with ConvertIcebergToDeltaUtils {

  protected val cloneTable = "clone"

  private def sourceIdentifier: String = s"iceberg.`$tablePath`"

  private def runCreateOrReplace(mode: String, source: String): DataFrame = {
    Try(spark.sql(s"DELETE FROM $cloneTable"))
    spark.sql(s"CREATE OR REPLACE TABLE $cloneTable $mode CLONE $source")
  }

  private val mode = "SHALLOW"

  test("cast Iceberg TIME to Spark long") {
    withTable(table, cloneTable) {
      val schema = new Schema(
        Seq[NestedField](
          NestedField.required(1, "id", Types.IntegerType.get),
          NestedField.required(2, "event_time", Types.TimeType.get)
        ).asJava
      )
      val rows = Seq(
        Map(
          "id" -> 1,
          "event_time" -> LocalTime.of(14, 30, 11)
        )
      )
      NonSparkIcebergTestUtils.createIcebergTable(spark, tablePath, schema, rows)
      intercept[UnsupportedOperationException] {
        runCreateOrReplace(mode, sourceIdentifier)
      }
      withSQLConf(DeltaSQLConf.DELTA_CONVERT_ICEBERG_CAST_TIME_TYPE.key -> "true") {
        runCreateOrReplace(mode, sourceIdentifier)
        val expectedMicrosec = (14 * 3600 + 30 * 60 + 11) * 1000000L
        checkAnswer(spark.table(cloneTable), Row(1, expectedMicrosec) :: Nil)
        val clonedDeltaTable = DeltaLog.forTable(
          spark,
          spark.sessionState.catalog.getTableMetadata(TableIdentifier(cloneTable))
        )
        assert(DeltaConfigs.CAST_ICEBERG_TIME_TYPE.fromMetaData(clonedDeltaTable.update().metadata))
      }
    }
  }
}

class CloneIcebergByNameSuite extends CloneIcebergSuiteBase
{
  override def sourceIdentifier: String = table

  test("missing iceberg library should throw a sensical error") {
    val validIcebergSparkTableClassPath = ConvertUtils.icebergSparkTableClassPath
    val validIcebergLibTableClassPath = ConvertUtils.icebergLibTableClassPath

    Seq(
      () => {
        ConvertUtils.icebergSparkTableClassPath = validIcebergSparkTableClassPath + "2"
      },
      () => {
        ConvertUtils.icebergLibTableClassPath = validIcebergLibTableClassPath + "2"
      }
    ).foreach { makeInvalid =>
      try {
        makeInvalid()
        withTable(table, cloneTable) {
          spark.sql(
            s"""CREATE TABLE $table (`1 id` bigint, 2data string)
               |USING iceberg PARTITIONED BY (2data)""".stripMargin)
          spark.sql(s"INSERT INTO $table VALUES (1, 'a'), (2, 'b'), (3, 'c')")
          val e = intercept[DeltaIllegalStateException] {
            runCreateOrReplace("SHALLOW", sourceIdentifier)
          }
          assert(e.getErrorClass == "DELTA_MISSING_ICEBERG_CLASS")
        }
      } finally {
        ConvertUtils.icebergSparkTableClassPath = validIcebergSparkTableClassPath
        ConvertUtils.icebergLibTableClassPath = validIcebergLibTableClassPath
      }
    }
  }
}

trait DisablingConvertIcebergStats extends CloneIcebergSuiteBase {
  override def sparkConf: SparkConf =
    super.sparkConf.set(DeltaSQLConf.DELTA_CONVERT_ICEBERG_STATS.key, "false")
}

class CloneIcebergByPathNoConvertStatsSuite
  extends CloneIcebergByPathSuite
    with DisablingConvertIcebergStats

class CloneIcebergByNameNoConvertStatsSuite
  extends CloneIcebergByNameSuite
    with DisablingConvertIcebergStats

