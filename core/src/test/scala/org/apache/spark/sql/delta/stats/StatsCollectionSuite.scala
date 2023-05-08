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

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.DeltaOperations.ManualUpdate
import org.apache.spark.sql.delta.actions.Protocol
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.{DeltaSQLCommandTest, TestsStatistics}
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.hadoop.fs.Path
import org.scalatest.exceptions.TestFailedException

import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

class StatsCollectionSuite
    extends QueryTest
    with SharedSparkSession    with DeltaColumnMappingTestUtils
    with TestsStatistics
    with DeltaSQLCommandTest
    with DeletionVectorsTestUtils {

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
        override def tableDataSchema = dataRenamed.schema
        override def dataSchema = dataRenamed.schema
        override val numIndexedCols = DeltaConfigs.DATA_SKIPPING_NUM_INDEXED_COLS.fromString(
          DeltaConfigs.DATA_SKIPPING_NUM_INDEXED_COLS.defaultValue)
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
          "Statistics re-computation on a Delta table with deletion vectors is not yet supported.")
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
          StatisticsCollection.recompute(spark, deltaLog, fileFilter = _.size == biggest)
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

  test("Truncate max string") {
    // scalastyle:off nonascii
    val prefixLen = 6
    // � is the max unicode character with value \ufffd
    val inputToExpected = Seq(
      (s"abcd", s"abcd"),
      (s"abcdef", s"abcdef"),
      (s"abcde�", s"abcde�"),
      (s"abcd�abcd", s"abcd�a�"),
      (s"�abcd", s"�abcd"),
      (s"abcdef�", s"abcdef��"),
      (s"abcdef-abcdef�", s"abcdef�"),
      (s"abcdef�abcdef", s"abcdef��"),
      (s"abcdef��abcdef", s"abcdef���"),
      (s"abcdef�abcdef�abcdef�abcdef", s"abcdef��")
    )
    inputToExpected.foreach {
      case (input, expected) =>
        val actual = StatisticsCollection.truncateMaxStringAgg(prefixLen)(input)
        assert(actual == expected, s"input:$input, actual:$actual, expected:$expected")
    }
    // scalastyle:off nonascii
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

