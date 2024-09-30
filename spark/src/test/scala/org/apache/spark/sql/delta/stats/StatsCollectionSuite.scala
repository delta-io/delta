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

package org.apache.spark.sql.delta.stats

import java.math.BigDecimal
import java.sql.Date
import java.time.LocalDateTime

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.Protocol
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.StatisticsCollection.{ASCII_MAX_CHARACTER, UTF8_MAX_CHARACTER}
import org.apache.spark.sql.delta.test.{DeltaExceptionTestUtils, DeltaSQLCommandTest, DeltaSQLTestUtils, TestsStatistics}
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.hadoop.fs.Path
import org.scalatest.exceptions.TestFailedException

import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{GenericRow, GenericRowWithSchema}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

class StatsCollectionSuite
    extends QueryTest
    with SharedSparkSession
    with DeltaColumnMappingTestUtils
    with TestsStatistics
    with DeltaSQLCommandTest
    with DeltaSQLTestUtils
    with DeletionVectorsTestUtils
    with DeltaExceptionTestUtils {

  import testImplicits._


  test("on write") {
    withTempDir { dir =>
      val deltaLog = DeltaLog.forTable(spark, dir)

      val data = Seq(1, 2, 3).toDF().coalesce(1)
      data.write.format("delta").save(dir.getAbsolutePath)
      val snapshot = deltaLog.update()
      val statsJson = deltaLog.update().allFiles.head().stats

      // convert data schema to physical name if possible
      val dataRenamed = data.toDF(
        data.columns.map(name => getPhysicalName(name, deltaLog.snapshot.schema)): _*)

      val skipping = new StatisticsCollection {
        override val spark = StatsCollectionSuite.this.spark
        override def tableSchema: StructType = dataRenamed.schema
        override def outputTableStatsSchema: StructType = dataRenamed.schema
        override def outputAttributeSchema: StructType = dataRenamed.schema
        override val statsColumnSpec = DeltaStatsColumnSpec(
          None,
          Some(
            DeltaConfigs.DATA_SKIPPING_NUM_INDEXED_COLS.fromString(
              DeltaConfigs.DATA_SKIPPING_NUM_INDEXED_COLS.defaultValue)
          )
        )
        override def columnMappingMode: DeltaColumnMappingMode = deltaLog.snapshot.columnMappingMode
        override val protocol: Protocol = snapshot.protocol
      }

      val correctAnswer = dataRenamed
        .select(skipping.statsCollector)
        .select(to_json($"stats").as[String])
        .collect()
        .head

      assert(statsJson === correctAnswer)
    }
  }

  test("gather stats") {
    withTempDir { dir =>
      val deltaLog = DeltaLog.forTable(spark, dir)

      val data = spark.range(1, 10, 1, 10).withColumn("odd", $"id" % 2 === 1)
      data.write.partitionBy("odd").format("delta").save(dir.getAbsolutePath)

      val df = spark.read.format("delta").load(dir.getAbsolutePath)
      withSQLConf("spark.sql.parquet.filterPushdown" -> "false") {
        assert(recordsScanned(df) == 9)
        assert(recordsScanned(df.where("id = 1")) == 1)
      }
    }
  }

  test("statistics re-computation throws error on Delta tables with DVs") {
    withDeletionVectorsEnabled() {
      withTempDir { dir =>
        val df = spark.range(start = 0, end = 20).toDF().repartition(numPartitions = 4)
        df.write.format("delta").save(dir.toString())

        spark.sql(s"DELETE FROM delta.`${dir.toString}` WHERE id in (2, 15)")
        val e = intercept[DeltaCommandUnsupportedWithDeletionVectorsException] {
          val deltaLog = DeltaLog.forTable(spark, dir)
          StatisticsCollection.recompute(spark, deltaLog)
        }
        assert(e.getErrorClass == "DELTA_UNSUPPORTED_STATS_RECOMPUTE_WITH_DELETION_VECTORS")
        assert(e.getSqlState == "0AKDD")
        assert(e.getMessage ==
          "[DELTA_UNSUPPORTED_STATS_RECOMPUTE_WITH_DELETION_VECTORS] " +
            "Statistics re-computation on a Delta table with deletion " +
            "vectors is not yet supported.")
      }
    }
  }

  statsTest("recompute stats basic") {
    withTempDir { tempDir =>
      withSQLConf(DeltaSQLConf.DELTA_COLLECT_STATS.key -> "false") {
        val df = spark.range(2).coalesce(1).toDF()
        df.write.format("delta").save(tempDir.toString())
        val deltaLog = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
        assert(statsDF(deltaLog).where('numRecords.isNotNull).count() == 0)

        {
          StatisticsCollection.recompute(spark, deltaLog)
        }
        checkAnswer(
          spark.read.format("delta").load(tempDir.getCanonicalPath),
          df
        )
        val statsDf = statsDF(deltaLog)
        assert(statsDf.where('numRecords.isNotNull).count() > 0)
        // Make sure stats indicate 2 rows, min [0], max [1]
        checkAnswer(statsDf, Row(2, Row(0), Row(1)))
      }
    }
  }

  statsTestSparkMasterOnly("recompute variant stats") {
    withTempDir { tempDir =>
      withSQLConf(DeltaSQLConf.DELTA_COLLECT_STATS.key -> "false") {
        val df = spark.range(2)
          .selectExpr(
            "case when id % 2 = 0 then parse_json(cast(id as string)) else null end as v"
          )
          .coalesce(1)
          .toDF()
        df.write.format("delta").save(tempDir.toString())
        val deltaLog = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))

        assert(getStatsDf(deltaLog, Seq($"numRecords")).where('numRecords.isNotNull).count() == 0)

        {
          StatisticsCollection.recompute(spark, deltaLog)
        }
        checkAnswer(
          spark.read.format("delta").load(tempDir.getCanonicalPath),
          df
        )
        val statsDf = getStatsDf(deltaLog, Seq($"numRecords", $"nullCount"))
        assert(statsDf.where('numRecords.isNotNull).count() > 0)
        // Make sure stats indicate 2 rows, nullCount [1]
        checkAnswer(statsDf, Row(2, Row(1)))
      }
    }
  }

  statsTest("recompute stats multiple columns and files") {
    withTempDir { tempDir =>
      withSQLConf(DeltaSQLConf.DELTA_COLLECT_STATS.key -> "false") {
        val df = spark.range(10, 20).withColumn("x", 'id + 10).repartition(3)

        df.write.format("delta").save(tempDir.toString())
        val deltaLog = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
        assert(statsDF(deltaLog).where('numRecords.isNotNull).count() == 0)

        {
          StatisticsCollection.recompute(spark, deltaLog)
        }

        checkAnswer(
          spark.read.format("delta").load(tempDir.getCanonicalPath),
          df
        )
        val statsDf = statsDF(deltaLog)
        assert(statsDf.where('numRecords.isNotNull).count() > 0)
        // scalastyle:off line.size.limit
        val expectedStats = Seq(Row(3, Row(10, 20), Row(19, 29)), Row(4, Row(12, 22), Row(17, 27)), Row(3, Row(11, 21), Row(18, 28)))
        // scalastyle:on line.size.limit
        checkAnswer(statsDf, expectedStats)
      }
    }
  }

  statsTest("recompute stats on partitioned table") {
    withTempDir { tempDir =>
      withSQLConf(DeltaSQLConf.DELTA_COLLECT_STATS.key -> "false") {
        val df = spark.range(15).toDF("a")
          .withColumn("b", 'a % 3)
          .withColumn("c", 'a % 2)
          .repartition(3, 'b)

        df.write.format("delta").partitionBy("b").save(tempDir.toString())
        val deltaLog = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
        assert(statsDF(deltaLog).where('numRecords.isNotNull).count() == 0)

        {
          StatisticsCollection.recompute(spark, deltaLog)
        }
        checkAnswer(
          spark.read.format("delta").load(tempDir.getCanonicalPath),
          df
        )
        val statsDf = statsDF(deltaLog)
        assert(statsDf.where('numRecords.isNotNull).count() > 0)
        checkAnswer(statsDf, Seq(
          Row(5, Row(1, 0), Row(13, 1)),
          Row(5, Row(0, 0), Row(12, 1)),
          Row(5, Row(2, 0), Row(14, 1))))
      }
    }
  }

  statsTest("recompute stats with partition predicates") {
    withTempDir { tempDir =>
      withSQLConf(DeltaSQLConf.DELTA_COLLECT_STATS.key -> "false") {
        val df = Seq(
          (1, 0, 10), (1, 2, 20), (1, 4, 30), (2, 6, 40), (2, 8, 50), (3, 10, 60), (4, 12, 70))
          .toDF("a", "b", "c")

        df.write.format("delta").partitionBy("a").save(tempDir.toString())
        val deltaLog = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
        assert(statsDF(deltaLog).where('numRecords.isNotNull).count() == 0)

        {
          StatisticsCollection.recompute(spark, deltaLog, Seq(('a > 1).expr, ('a < 4).expr))
        }
        checkAnswer(
          spark.read.format("delta").load(tempDir.getCanonicalPath),
          df
        )
        val statsDf = statsDF(deltaLog)
        assert(statsDf.where('numRecords.isNotNull).count() == 2)
        checkAnswer(statsDf, Seq(
          Row(null, Row(null, null), Row(null, null)),
          Row(2, Row(6, 40), Row(8, 50)),
          Row(1, Row(10, 60), Row(10, 60)),
          Row(null, Row(null, null), Row(null, null))))
      }
    }
  }

  statsTest("recompute stats with invalid partition predicates") {
    withTempDir { tempDir =>
      withSQLConf(DeltaSQLConf.DELTA_COLLECT_STATS.key -> "false") {
        Seq((1, 0, 10), (1, 2, 20), (1, 4, 30), (2, 6, 40), (2, 8, 50), (3, 10, 60), (4, 12, 70))
          .toDF("a", "b", "c")
          .write.format("delta").partitionBy("a").save(tempDir.toString())
        val deltaLog = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
        assert(statsDF(deltaLog).where('numRecords.isNotNull).count() == 0)

        {
          intercept[AnalysisException] {
            StatisticsCollection.recompute(spark, deltaLog, Seq(('b > 1).expr))
          }
          intercept[AnalysisException] {
            StatisticsCollection.recompute(spark, deltaLog, Seq(('a > 1).expr, ('c > 1).expr))
          }
        }
        assert(statsDF(deltaLog).where('numRecords.isNotNull).count() == 0)
      }
    }
  }

  statsTest("recompute stats on a table with corrupted stats") {
    withTempDir { tempDir =>
      val df = Seq(
        (1, 0, 10), (1, 2, 20), (1, 4, 30), (2, 6, 40), (2, 8, 50), (3, 10, 60), (4, 12, 70))
        .toDF("a", "b", "c")

      df.write.format("delta").partitionBy("a").save(tempDir.toString())
      val deltaLog = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
      val correctStats = statsDF(deltaLog)
      assert(correctStats.where('numRecords.isNotNull).count() == 4)

      // use physical names if possible
      val (a, b, c) = (
        getPhysicalName("a", deltaLog.snapshot.schema),
        getPhysicalName("b", deltaLog.snapshot.schema),
        getPhysicalName("c", deltaLog.snapshot.schema)
      )

      {
        // Corrupt stats on one of the files
        val txn = deltaLog.startTransaction()
        val f = deltaLog.snapshot.allFiles.filter(_.partitionValues(a) == "1").first()
        val corrupted = f.copy(stats = f.stats.replace(
          s"""maxValues":{"$b":4,"$c":30}""",
          s"""maxValues":{"$b":-100,"$c":100}"""))
        txn.commit(Seq(corrupted), DeltaOperations.ComputeStats(Nil))
        intercept[TestFailedException] {
          checkAnswer(statsDF(deltaLog), correctStats)
        }

        // Recompute stats and verify they match the original ones
        StatisticsCollection.recompute(spark, deltaLog)
        checkAnswer(
          spark.read.format("delta").load(tempDir.getCanonicalPath),
          df
        )
        checkAnswer(statsDF(deltaLog), correctStats)
      }
    }
  }

  statsTest("recompute stats with file filter") {
    withTempDir { tempDir =>
      withSQLConf(DeltaSQLConf.DELTA_COLLECT_STATS.key -> "false") {
        val df = Seq(
          (1, 0, 10), (1, 2, 20), (1, 4, 30), (2, 6, 40), (2, 8, 50), (3, 10, 60), (4, 12, 70))
          .toDF("a", "b", "c")

        df.write.format("delta").partitionBy("a").save(tempDir.toString())
        val deltaLog = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
        assert(statsDF(deltaLog).where('numRecords.isNotNull).count() == 0)

        val biggest = deltaLog.snapshot.allFiles.agg(max('size)).first().getLong(0)

        {
          StatisticsCollection.recompute(
            spark, deltaLog, catalogTable = None, fileFilter = _.size == biggest)
        }

        checkAnswer(
          spark.read.format("delta").load(tempDir.getCanonicalPath),
          df
        )
        val statsDf = statsDF(deltaLog)
        assert(statsDf.where('numRecords.isNotNull).count() == 1)
        checkAnswer(statsDf, Seq(
          Row(null, Row(null, null), Row(null, null)),
          Row(null, Row(null, null), Row(null, null)),
          Row(null, Row(null, null), Row(null, null)),
          Row(3, Row(0, 10), Row(4, 30))))
      }
    }
  }

  test("Truncate min string") {
    // scalastyle:off nonascii
    val inputToExpected = Seq(
      (s"abcd", s"abc", 3),
      (s"abcdef", s"abcdef", 6),
      (s"abcde�", s"abcde�", 6),
      (s"$UTF8_MAX_CHARACTER$UTF8_MAX_CHARACTER$UTF8_MAX_CHARACTER$UTF8_MAX_CHARACTER",
        s"$UTF8_MAX_CHARACTER",
        1),
      (s"$UTF8_MAX_CHARACTER$UTF8_MAX_CHARACTER", s"$UTF8_MAX_CHARACTER", 1),
      (s"abcd", null, 0)
    )

    inputToExpected.foreach {
      case (input, expected, prefixLen) =>
        val actual = StatisticsCollection.truncateMinStringAgg(prefixLen)(input)
        val debugMsg = s"input:$input, actual:$actual, expected:$expected"
        assert(actual == expected, debugMsg)
        if (actual != null) {
          assert(input.startsWith(actual), debugMsg)
        }
    }
    // scalastyle:on nonascii
  }

  test("Truncate max string") {
    // scalastyle:off nonascii
    val inputToExpected = Seq(
      (s"abcd", null, 0),
      (s"a${UTF8_MAX_CHARACTER}d", s"a$UTF8_MAX_CHARACTER$ASCII_MAX_CHARACTER", 2),
      (s"abcd", s"abcd", 6),
      (s"abcdef", s"abcdef", 6),
      (s"abcde�", s"abcde�", 6),
      (s"abcd�abcd", s"abcd�a$ASCII_MAX_CHARACTER", 6),
      (s"�abcd", s"�abcd", 6),
      (s"abcdef�", s"abcdef$UTF8_MAX_CHARACTER", 6),
      (s"abcdef��", s"abcdef$UTF8_MAX_CHARACTER", 6),
      (s"abcdef-abcdef�", s"abcdef$ASCII_MAX_CHARACTER", 6),
      (s"abcdef�abcdef", s"abcdef$UTF8_MAX_CHARACTER", 6),
      (s"abcde�abcdef�abcdef�abcdef", s"abcde�$ASCII_MAX_CHARACTER", 6),
      (s"漢字仮名한글தமி", s"漢字仮名한글$UTF8_MAX_CHARACTER", 6),
      (s"漢字仮名한글��", s"漢字仮名한글$UTF8_MAX_CHARACTER", 6),
      (s"漢字仮名한글", s"漢字仮名한글", 6),
      (s"abcdef🚀", s"abcdef$UTF8_MAX_CHARACTER", 6),
      (s"$UTF8_MAX_CHARACTER$UTF8_MAX_CHARACTER$UTF8_MAX_CHARACTER$UTF8_MAX_CHARACTER", null, 1),
      (s"$UTF8_MAX_CHARACTER$UTF8_MAX_CHARACTER$UTF8_MAX_CHARACTER$UTF8_MAX_CHARACTER",
        s"$UTF8_MAX_CHARACTER$UTF8_MAX_CHARACTER$UTF8_MAX_CHARACTER$UTF8_MAX_CHARACTER",
        4),
      (s"����", s"��$UTF8_MAX_CHARACTER", 2),
      (s"���", s"�$UTF8_MAX_CHARACTER", 1),
      ("abcdefghijklm💞😉💕\n🥀🌹💐🌺🌷🌼🌻🌷🥀",
        s"abcdefghijklm💞😉💕\n🥀🌹💐🌺🌷🌼$UTF8_MAX_CHARACTER",
        32)
    )

    inputToExpected.foreach {
      case (input, expected, prefixLen) =>
        val actual = StatisticsCollection.truncateMaxStringAgg(prefixLen)(input)
        // `Actual` should be higher or equal than `input` in UTF-8 encoded binary order.
        val debugMsg = s"input:$input, actual:$actual, expected:$expected"
        assert(actual == expected, debugMsg)
    }
    // scalastyle:off nonascii
  }


  test(s"Optimize Zorder for delta statistics column: table creation") {
    val tableName = "delta_table"
    withTable(tableName) {
      sql("create table delta_table (c1 long, c2 long) " +
        "using delta " +
        "TBLPROPERTIES('delta.dataSkippingStatsColumns' = 'c1,c2', " +
        "'delta.dataSkippingNumIndexedCols' = 0)")
      for (_ <- 1 to 10) {
        sql("insert into delta_table values(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8)")
      }
      sql("optimize delta_table zorder by (c1)")
      sql("optimize delta_table zorder by (c2)")
      sql("optimize delta_table zorder by (c1,c2)")
    }
  }

  test(s"Optimize Zorder for delta statistics column: alter TBLPROPERTIES") {
    val tableName = "delta_table"
    withTable(tableName) {
      sql("create table delta_table (c1 long, c2 long) " +
        "using delta TBLPROPERTIES('delta.dataSkippingNumIndexedCols' = 0)")
      intercept[DeltaAnalysisException] { sql("optimize delta_table zorder by (c1)") }
      intercept[DeltaAnalysisException] { sql("optimize delta_table zorder by (c2)") }
      intercept[DeltaAnalysisException] { sql("optimize delta_table zorder by (c1,c2)") }
      for (_ <- 1 to 10) {
        sql("insert into delta_table values(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8)")
      }
      sql("ALTER TABLE delta_table SET TBLPROPERTIES ('delta.dataSkippingStatsColumns' = 'c1,c2')")
      sql("optimize delta_table zorder by (c1)")
      sql("optimize delta_table zorder by (c2)")
      sql("optimize delta_table zorder by (c1,c2)")
    }
  }

  test(s"Delta statistic column: special characters") {
    val tableName = "delta_table_1"
    withTable(tableName) {
      sql(
        s"create table $tableName (`c1.` long, `c2*` long, `c3,` long, `c-4` long) using delta " +
        s"TBLPROPERTIES(" +
        s"'delta.dataSkippingStatsColumns'='`c1.`,`c2*`,`c3,`,`c-4`'," +
        s"'delta.columnMapping.mode' = 'name')"
      )
      val dataSkippingStatsColumns = sql(s"SHOW TBLPROPERTIES $tableName")
        .collect()
        .map { row => row.getString(0) -> row.getString(1) }
        .filter(_._1 == "delta.dataSkippingStatsColumns")
        .toSeq
      val result1 = Seq(("delta.dataSkippingStatsColumns", "`c1.`,`c2*`,`c3,`,`c-4`"))
      assert(dataSkippingStatsColumns == result1)
    }
  }

  Seq("c1.", "c2*", "c3,", "c-4").foreach { col =>
    test(s"Delta statistic column: invalid special characters $col") {
      val tableName = "delta_table_1"
      withTable(tableName) {
        val except = intercept[Exception] {
          sql(
            s"create table $tableName (`c1.` long, `c2*` long, `c3,` long, c4 long) using delta " +
            s"TBLPROPERTIES(" +
            s"'delta.dataSkippingStatsColumns'='$col'," +
            s"'delta.columnMapping.mode' = 'name')"
          )
        }
      }
    }
  }

  Seq(
    ("BINARY", "BinaryType"),
    ("BOOLEAN", "BooleanType"),
    ("ARRAY<TINYINT>", "ArrayType(ByteType,true)"),
    ("MAP<DATE, INT>", "MapType(DateType,IntegerType,true)"),
    ("STRUCT<c60:INT, c61:ARRAY<INT>>", "ArrayType(IntegerType,true)")
  ).foreach { case (invalidType, typename) =>
    val tableName1 = "delta_table_1"
    val tableName2 = "delta_table_2"
    test(s"Delta statistic column: invalid data type $invalidType") {
      withTable(tableName1, tableName2) {
        val columnName = if (typename.equals("ArrayType(IntegerType,true)")) "c2.c61" else "c2"
        val exceptOne = intercept[DeltaIllegalArgumentException] {
          sql(
            s"create table $tableName1 (c1 long, c2 $invalidType) using delta " +
            s"TBLPROPERTIES('delta.dataSkippingStatsColumns'='c2')"
          )
        }
        assert(
          exceptOne.getErrorClass == "DELTA_COLUMN_DATA_SKIPPING_NOT_SUPPORTED_TYPE" &&
          exceptOne.getMessageParametersArray.toSeq == Seq(columnName, typename)
        )
        sql(s"create table $tableName2 (c1 long, c2 $invalidType) using delta")
        val exceptTwo = interceptWithUnwrapping[DeltaIllegalArgumentException] {
          sql(s"ALTER TABLE $tableName2 SET TBLPROPERTIES('delta.dataSkippingStatsColumns' = 'c2')")
        }
        assert(
          exceptTwo.getErrorClass == "DELTA_COLUMN_DATA_SKIPPING_NOT_SUPPORTED_TYPE" &&
          exceptTwo.getMessageParametersArray.toSeq == Seq(columnName, typename)
        )
      }
    }

    test(s"Delta statistic column: invalid data type $invalidType in nested column") {
      withTable(tableName1, tableName2) {
        val columnName = if (typename == "ArrayType(IntegerType,true)") "c2.c21.c61" else "c2.c21"
        val exceptOne = intercept[DeltaIllegalArgumentException] {
          sql(
            s"create table $tableName1 (c1 long, c2 STRUCT<c20:INT, c21:$invalidType>) " +
              s"using delta TBLPROPERTIES('delta.dataSkippingStatsColumns' = 'c2.c21')"
          )
        }
        assert(
          exceptOne.getErrorClass == "DELTA_COLUMN_DATA_SKIPPING_NOT_SUPPORTED_TYPE" &&
            exceptOne.getMessageParametersArray.toSeq == Seq(columnName, typename)
        )
        val exceptTwo = intercept[DeltaIllegalArgumentException] {
          sql(
            s"create table $tableName1 (c1 long, c2 STRUCT<c20:INT, c21:$invalidType>) " +
              s"using delta TBLPROPERTIES('delta.dataSkippingStatsColumns' = 'c2')"
          )
        }
        assert(
          exceptTwo.getErrorClass == "DELTA_COLUMN_DATA_SKIPPING_NOT_SUPPORTED_TYPE" &&
          exceptTwo.getMessageParametersArray.toSeq == Seq(columnName, typename)
        )
        sql(s"create table $tableName2 (c1 long, c2 STRUCT<c20:INT, c21:$invalidType>) using delta")
        val exceptThree = interceptWithUnwrapping[DeltaIllegalArgumentException] {
          sql(
            s"ALTER TABLE $tableName2 SET TBLPROPERTIES('delta.dataSkippingStatsColumns'='c2.c21')"
          )
        }
        assert(
          exceptThree.getErrorClass == "DELTA_COLUMN_DATA_SKIPPING_NOT_SUPPORTED_TYPE" &&
          exceptThree.getMessageParametersArray.toSeq == Seq(columnName, typename)
        )
        val exceptFour = interceptWithUnwrapping[DeltaIllegalArgumentException] {
          sql(s"ALTER TABLE $tableName2 SET TBLPROPERTIES('delta.dataSkippingStatsColumns'='c2')")
        }
        assert(
          exceptFour.getErrorClass == "DELTA_COLUMN_DATA_SKIPPING_NOT_SUPPORTED_TYPE" &&
          exceptFour.getMessageParametersArray.toSeq == Seq(columnName, typename)
        )
      }
    }
  }

  test(s"Delta statistic column: mix case column name") {
    val tableName = "delta_table_1"
    withTable(tableName) {
      sql(
        s"create table $tableName (cOl1 LONG, COL2 struct<COL20 INT, CoL21 LONG>, CoL3 LONG) " +
        s"using delta TBLPROPERTIES" +
        s"('delta.dataSkippingStatsColumns' = 'coL1, COL2.col20, COL2.col21, cOl3');"
      )
      (1 to 10).foreach { _ =>
        sql(
          s"""insert into $tableName values
             |(1, struct(1, 10), 1), (2, struct(2, 20), 2), (3, struct(3, 30), 3),
             |(4, struct(4, 40), 4), (5, struct(5, 50), 5), (6, struct(6, 60), 6),
             |(7, struct(7, 70), 7), (8, struct(8, 80), 8), (9, struct(9, 90), 9),
             |(10, struct(10, 100), 10), (null, struct(null, null), null),
             |(-1, struct(-1, -100), -1), (null, struct(null, null), null);""".stripMargin
        )
      }
      sql(s"optimize $tableName")
      val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tableName))
      val df = deltaLog.update().withStatsDeduplicated
      val analyzedDfPlan = df.queryExecution.analyzed.toString
      val stats = if (analyzedDfPlan.indexOf("stats_parsed") > 0) "stats_parsed" else "stats"
      df.select(s"$stats.numRecords", s"$stats.nullCount", s"$stats.minValues", s"$stats.maxValues")
        .collect()
        .foreach { row =>
          assert(row(0) == 130)
          assert(row(1).asInstanceOf[GenericRow] == Row(20, Row(20, 20), 20))
          assert(row(2) == Row(-1, Row(-1, -100), -1))
          assert(row(3) == Row(10, Row(10, 100), 10))
        }
    }
  }

  Seq(
    "BIGINT", "DATE", "DECIMAL(3, 2)", "DOUBLE", "FLOAT", "INT", "SMALLINT", "STRING",
    "TIMESTAMP", "TIMESTAMP_NTZ", "TINYINT"
  ).foreach { validType =>
    val tableName1 = "delta_table_1"
    val tableName2 = "delta_table_2"
    test(s"Delta statistic column: valid data type $validType") {
      withTable(tableName1, tableName2) {
        sql(
          s"create table $tableName1 (c1 long, c2 $validType) using delta " +
          s"TBLPROPERTIES('delta.dataSkippingStatsColumns' = 'c2')"
        )
        sql(s"create table $tableName2 (c1 long, c2 $validType) using delta")
        sql(s"ALTER TABLE $tableName2 SET TBLPROPERTIES('delta.dataSkippingStatsColumns'='c2')")
      }
    }

    test(s"Delta statistic column: valid data type $validType in nested column") {
      val tableName3 = "delta_table_3"
      val tableName4 = "delta_table_4"
      withTable(tableName1, tableName2, tableName3, tableName4) {
        sql(
          s"create table $tableName1 (c1 long, c2 STRUCT<c20:INT, c21:$validType>) " +
          s"using delta TBLPROPERTIES('delta.dataSkippingStatsColumns' = 'c2.c21')"
        )
        sql(
          s"create table $tableName2 (c1 long, c2 STRUCT<c20:INT, c21:$validType>) " +
          s"using delta TBLPROPERTIES('delta.dataSkippingStatsColumns' = 'c2')"
        )
        sql(s"create table $tableName3 (c1 long, c2 STRUCT<c20:INT, c21:$validType>) using delta")
        sql(s"ALTER TABLE $tableName3 SET TBLPROPERTIES('delta.dataSkippingStatsColumns'='c2.c21')")
        sql(s"create table $tableName4 (c1 long, c2 STRUCT<c20:INT, c21:$validType>) using delta")
        sql(s"ALTER TABLE $tableName4 SET TBLPROPERTIES('delta.dataSkippingStatsColumns'='c2')")
      }
    }
  }

  Seq("create", "alter").foreach { label =>
    val tableName = "delta_table"
    val propertyName = "delta.dataSkippingStatsColumns"
    test(s"Delta statistics column with partition column: $label") {
      withTable(tableName) {
        if (label == "create") {
          val except = intercept[DeltaIllegalArgumentException] {
            sql(
              "create table delta_table(c0 int, c1 int) using delta partitioned by(c1) " +
              "TBLPROPERTIES('delta.dataSkippingStatsColumns' = 'c1')"
            )
          }
          assert(
            except.getErrorClass == "DELTA_COLUMN_DATA_SKIPPING_NOT_SUPPORTED_PARTITIONED_COLUMN" &&
            except.getMessageParametersArray.toSeq == Seq("c1")
          )
        } else {
          sql("create table delta_table(c0 int, c1 int) using delta partitioned by(c1)")
          val except = interceptWithUnwrapping[DeltaIllegalArgumentException] {
            sql(
              "ALTER TABLE delta_table SET TBLPROPERTIES ('delta.dataSkippingStatsColumns' = 'c1')"
            )
          }
          assert(
            except.getErrorClass == "DELTA_COLUMN_DATA_SKIPPING_NOT_SUPPORTED_PARTITIONED_COLUMN" &&
            except.getMessageParametersArray.toSeq == Seq("c1")
          )
        }
      }
    }

    test(s"Rename Nested Columns with delta statistics column: $label") {
      withTable(tableName) {
        if (label == "create") {
          sql(
            "create table delta_table (" +
            " id long," +
            " info STRUCT <title: String, value: long, depart STRUCT <org: long, perf: long>>, " +
            " prev_job STRUCT <title: String, depart STRUCT <org: long, perf: long>>)" +
            " using delta TBLPROPERTIES(" +
            s"'$propertyName' = 'info.title,info.depart.org,info.depart.perf'," +
            "'delta.columnMapping.mode' = 'name', " +
            "'delta.minReaderVersion' = '2', " +
            "'delta.minWriterVersion' = '5')"
          )
        } else {
          sql(
            "create table delta_table (" +
            " id long," +
            " info STRUCT <title: String, value: long, depart STRUCT <org: long, perf: long>>, " +
            " prev_job STRUCT <title: String, depart STRUCT <org: long, perf: long>>)" +
            " using delta TBLPROPERTIES(" +
            "'delta.columnMapping.mode' = 'name', " +
            "'delta.minReaderVersion' = '2', " +
            "'delta.minWriterVersion' = '5')"
          )
        }
        if (label == "alter") {
          sql(s"alter table delta_table set TBLPROPERTIES(" +
            s"'$propertyName' = 'info.title,info.depart.org,info.depart.perf')")
        }
        // Rename nested column leaf.
        sql("ALTER TABLE delta_table RENAME COLUMN info.title TO title_name;")
        var dataSkippingStatsColumns = sql("SHOW TBLPROPERTIES delta_table")
          .collect()
          .map { row => row.getString(0) -> row.getString(1) }
          .filter(_._1 == propertyName)
          .toSeq
        val result1 = Seq((propertyName, "info.title_name,info.depart.org,info.depart.perf"))
        assert(dataSkippingStatsColumns == result1)
        // Rename nested column root.
        sql("ALTER TABLE delta_table RENAME COLUMN info TO detail")
        dataSkippingStatsColumns = sql("SHOW TBLPROPERTIES delta_table")
          .collect()
          .map { row => row.getString(0) -> row.getString(1) }
          .filter(_._1 == propertyName)
          .toSeq
        val result2 = Seq(
          (propertyName, "detail.title_name,detail.depart.org,detail.depart.perf")
        )
        assert(dataSkippingStatsColumns == result2)
        // Rename nested column intermediate node.
        sql("ALTER TABLE delta_table RENAME COLUMN detail.DEPART TO organization")
        dataSkippingStatsColumns = sql("SHOW TBLPROPERTIES delta_table")
          .collect()
          .map { row => row.getString(0) -> row.getString(1) }
          .filter(_._1 == propertyName)
          .toSeq
        val result3 = Seq(
          (propertyName, "detail.title_name,detail.organization.org,detail.organization.perf")
        )
        assert(dataSkippingStatsColumns == result3)
      }
    }

    test(s"Drop Nested Columns with delta statistics column: $label") {
      withTable(tableName) {
        if (label == "create") {
          sql(
            "create table delta_table (" +
            " id long, " +
            " info STRUCT <title: String, value: long, depart STRUCT <org: long, perf: long>>, " +
            " prev_job STRUCT <title: String, depart STRUCT <org: long, perf: long>>)" +
            " using delta TBLPROPERTIES(" +
            s"'$propertyName' = " +
            "'info.title,info.depart.org,info.depart.perf,prev_job.title,prev_job.depart.perf', " +
            "'delta.columnMapping.mode' = 'name', " +
            "'delta.minReaderVersion' = '2', " +
            "'delta.minWriterVersion' = '5')"
          )
        } else {
          sql(
            "create table delta_table (" +
            " id long," +
            " info STRUCT<title: String, value: long, depart STRUCT<org: long, perf: long>>, " +
            " prev_job STRUCT<title: String, depart STRUCT<org: long, perf: long>>)" +
            " using delta TBLPROPERTIES(" +
            "'delta.columnMapping.mode' = 'name', " +
            "'delta.minReaderVersion' = '2', " +
            "'delta.minWriterVersion' = '5')"
          )
        }
        if (label == "alter") {
          sql(
            s"alter table delta_table set TBLPROPERTIES(" +
              s"'$propertyName' = " +
              s"'info.title,info.depart.org,info.depart.perf,prev_job.title,prev_job.depart.perf')"
          )
        }
        // Drop nested column leaf.
        sql("ALTER TABLE delta_table DROP COLUMN info.title;")
        var dataSkippingStatsColumns = sql("SHOW TBLPROPERTIES delta_table")
          .collect()
          .map { row => row.getString(0) -> row.getString(1) }
          .filter(_._1 == propertyName)
          .toSeq
        val result1 = Seq(
          (propertyName, "info.depart.org,info.depart.perf,prev_job.title,prev_job.depart.perf")
        )
        assert(dataSkippingStatsColumns == result1)
        // Drop nested column intermediate node.
        sql("ALTER TABLE delta_table DROP COLUMN info.depart;")
        dataSkippingStatsColumns = sql("SHOW TBLPROPERTIES delta_table")
          .collect()
          .map { row => row.getString(0) -> row.getString(1) }
          .filter(_._1 == propertyName)
          .toSeq
        val result3 = Seq((propertyName, "prev_job.title,prev_job.depart.perf"))
        assert(dataSkippingStatsColumns == result3)

        // Rename nested column root node.
        sql("ALTER TABLE delta_table DROP COLUMN prev_job;")
        dataSkippingStatsColumns = sql("SHOW TBLPROPERTIES delta_table")
          .collect()
          .map { row => row.getString(0) -> row.getString(1) }
          .filter(_._1 == propertyName)
          .toSeq
        val result2 = Seq((propertyName, ""))
        assert(dataSkippingStatsColumns == result2)
      }
    }
  }

  test("Change Columns with delta statistics column") {
    Seq(
      "BIGINT", "DATE", "DECIMAL(3, 2)", "DOUBLE", "FLOAT", "INT", "SMALLINT", "STRING",
      "TIMESTAMP", "TIMESTAMP_NTZ", "TINYINT"
    ).foreach { validType =>
      Seq(
        "BINARY", "BOOLEAN", "ARRAY<TINYINT>", "MAP<DATE, INT>", "STRUCT<c60:INT, c61:ARRAY<INT>>"
      ).foreach { invalidType =>
        withTable("delta_table") {
          sql(
            s"create table delta_table (c0 long, c1 long, c2 $validType) using delta " +
            s"TBLPROPERTIES('delta.dataSkippingStatsColumns' = 'c1,c2', " +
            "'delta.columnMapping.mode' = 'name', " +
            "'delta.minReaderVersion' = '2', " +
            "'delta.minWriterVersion' = '5')"
          )
          intercept[AnalysisException] {
            sql(s"ALTER TABLE delta_table Change c2 TYPE $invalidType;")
          }
        }
      }
    }
  }

  test("Duplicated delta statistic columns: create") {
    Seq(
      ("'c0,c0'", "c0"),
      ("'c1,c1.c11'", "c1.c11"),
      ("'c1.c11,c1.c11'", "c1.c11"),
      ("'c1,c1'", "c1.c11,c1.c12")
    ).foreach { case (statsColumns, duplicatedColumns) =>
      val exception = intercept[DeltaIllegalArgumentException] {
        sql(
          s"create table delta_table (c0 long, c1 struct<c11: long, c12 long>) using delta " +
          s"TBLPROPERTIES('delta.dataSkippingStatsColumns' = $statsColumns, " +
          "'delta.columnMapping.mode' = 'name')"
        )
      }
      assert(
        exception.getErrorClass == "DELTA_DUPLICATE_DATA_SKIPPING_COLUMNS" &&
        exception.getMessageParametersArray.toSeq == Seq(duplicatedColumns)
      )
    }
  }

  test("Duplicated delta statistic columns: alter") {
    sql(
      s"create table delta_table_t1 (c0 long, c1 struct<c11: long, c12 long>) using delta " +
      s"TBLPROPERTIES('delta.columnMapping.mode' = 'name')"
    )
    Seq(
      ("'c0,c0'", "c0"),
      ("'c1,c1.c11'", "c1.c11"),
      ("'c1.c11,c1.c11'", "c1.c11"),
      ("'c1,c1'", "c1.c11,c1.c12")
    ).foreach { case (statsColumns, duplicatedColumns) =>
      val exception = interceptWithUnwrapping[DeltaIllegalArgumentException] {
        sql(
          s"ALTER TABLE delta_table_t1 " +
          s"SET TBLPROPERTIES('delta.dataSkippingStatsColumns'=$statsColumns)"
        )
      }
      assert(
        exception.getErrorClass == "DELTA_DUPLICATE_DATA_SKIPPING_COLUMNS" &&
        exception.getMessageParametersArray.toSeq == Seq(duplicatedColumns)
      )
    }
  }

  private def recordsScanned(df: DataFrame): Long = {
    val scan = df.queryExecution.executedPlan.find {
      case FileScanExecNode(_) => true
      case _ => false
    }.get

    var executedScan = false

    if (!executedScan) {
      if (scan.supportsColumnar) {
        scan.executeColumnar().count()
      } else {
        scan.execute().count()
      }
    }
    scan.metrics.get("numOutputRows").get.value
  }

  private def statsDF(deltaLog: DeltaLog): DataFrame = {
    // use physical name if possible
    val dataColumns = deltaLog.snapshot.metadata.dataSchema.map(DeltaColumnMapping.getPhysicalName)
    val minValues = struct(dataColumns.map(c => $"minValues.$c"): _*)
    val maxValues = struct(dataColumns.map(c => $"maxValues.$c"): _*)
    val df = getStatsDf(deltaLog, Seq($"numRecords", minValues, maxValues))
    val numRecordsCol = df.schema.head.name
    df.withColumnRenamed(numRecordsCol, "numRecords")
  }
}

class StatsCollectionNameColumnMappingSuite extends StatsCollectionSuite
  with DeltaColumnMappingEnableNameMode {

  override protected def runOnlyTests = Seq(
    "on write",
    "recompute stats with partition predicates"
  )
}

