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
import com.databricks.spark.util.DatabricksLogging
import org.apache.spark.sql.delta.DeltaTestUtils.BOOLEAN_DOMAIN
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.StatsUtils
import org.apache.spark.sql.delta.test.{DeltaSQLCommandTest, ScanReportHelper}
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

trait DeltaLimitPushDownTests extends QueryTest
    with SharedSparkSession
    with DatabricksLogging
    with ScanReportHelper
    with DeletionVectorsTestUtils
    with StatsUtils
    with DeltaSQLCommandTest {

  import testImplicits._


  test("no filter or projection") {
    val dir = Utils.createTempDir()
    val ds = Seq(1, 1, 2, 2, 3, 3).toDS().repartition(5, $"value")
    ds.write.format("delta").save(dir.toString)

    val Seq(deltaScan, deltaScanWithLimit) = getScanReport {
      spark.read.format("delta").load(dir.toString).collect()
      val res = spark.read.format("delta").load(dir.toString).limit(3).collect()
      assert(res.size == 3)
    }

    assert(deltaScan.size("total").bytesCompressed ===
      deltaScanWithLimit.size("total").bytesCompressed)

    assert(deltaScan.size("scanned").bytesCompressed !=
      deltaScanWithLimit.size("scanned").bytesCompressed)

    assert(deltaScanWithLimit.size("scanned").rows === Some(4L))
  }

  test("limit larger than total") {
    val dir = Utils.createTempDir()
    val data = Seq(1, 1, 2, 2)
    val ds = data.toDS().repartition($"value")
    ds.write.format("delta").save(dir.toString)

    val Seq(deltaScan, deltaScanWithLimit) = getScanReport {
      spark.read.format("delta").load(dir.toString).collect()
      checkAnswer(spark.read.format("delta").load(dir.toString).limit(5), data.toDF())
    }

    assert(deltaScan.size("total").bytesCompressed ===
      deltaScanWithLimit.size("total").bytesCompressed)

    assert(deltaScan.size("scanned").bytesCompressed ===
      deltaScanWithLimit.size("scanned").bytesCompressed)
  }

  test("limit 0") {
    val records = getScanReport {
      val dir = Utils.createTempDir()
      val ds = Seq(1, 1, 2, 2, 3, 3).toDS().repartition($"value")
      ds.write.format("delta").save(dir.toString)
      val res = spark.read.format("delta")
        .load(dir.toString)
        .limit(0)

      checkAnswer(res, Seq())
    }
  }

  test("insufficient rows have stats") {
    val tempDir = Utils.createTempDir()
    val deltaLog = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))

    val file = Seq(1, 2).toDS().coalesce(1)

    withSQLConf(DeltaSQLConf.DELTA_COLLECT_STATS.key -> "false") {
      file.write.format("delta").mode("append").save(deltaLog.dataPath.toString)
    }
    withSQLConf(DeltaSQLConf.DELTA_COLLECT_STATS.key -> "false") {
      file.write.format("delta").mode("append").save(deltaLog.dataPath.toString)
    }
    withSQLConf(DeltaSQLConf.DELTA_COLLECT_STATS.key -> "true") {
      file.write.format("delta").mode("append").save(deltaLog.dataPath.toString)
    }

    val deltaScan = deltaLog.snapshot.filesForScan(3)

    assert(deltaScan.scanned.bytesCompressed === deltaScan.total.bytesCompressed)
  }

  test("sufficient rows have stats") {
    val tempDir = Utils.createTempDir()
    val deltaLog = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))

    val file = Seq(1, 2).toDS().coalesce(1)

    withSQLConf(DeltaSQLConf.DELTA_COLLECT_STATS.key -> "false") {
      file.write.format("delta").mode("append").save(deltaLog.dataPath.toString)
    }
    withSQLConf(DeltaSQLConf.DELTA_COLLECT_STATS.key -> "true") {
      file.write.format("delta").mode("append").save(deltaLog.dataPath.toString)
    }
    withSQLConf(DeltaSQLConf.DELTA_COLLECT_STATS.key -> "true") {
      file.write.format("delta").mode("append").save(deltaLog.dataPath.toString)
    }

    val deltaScan = deltaLog.snapshot.filesForScan(3)

    assert(deltaScan.scanned.rows === Some(4))
    assert(deltaScan.scanned.bytesCompressed != deltaScan.total.bytesCompressed)
  }

  test("with projection only") {
    val dir = Utils.createTempDir()
    val ds = Seq((1, 1), (2, 1), (3, 1)).toDF("key", "value").as[(Int, Int)]
    ds.write.format("delta").partitionBy("key").save(dir.toString)

    val Seq(deltaScan) = getScanReport {
      val res = spark.read.format("delta").load(dir.toString).select("value").limit(1).collect()
      assert(res === Seq(Row(1)))
    }

    assert(deltaScan.size("scanned").rows === Some(1L))
  }

  test("with partition filter only") {
    val dir = Utils.createTempDir()
    val ds = Seq((1, 4), (2, 5), (3, 6)).toDF("key", "value").as[(Int, Int)]
    ds.write.format("delta").partitionBy("key").save(dir.toString)

    val Seq(deltaScan, deltaScanWithLimit, deltaScanWithLimit2) = getScanReport {
      spark.read.format("delta").load(dir.toString).where("key > 1").collect()
      val res1 = spark.read.format("delta").load(dir.toString).where("key > 1").limit(1).collect()
      assert(res1 === Seq(Row(2, 5)) || res1 === Seq(Row(3, 6)))
      val res2 = spark.read.format("delta").load(dir.toString).where("key == 1").limit(2).collect()
      assert(res2 === Seq(Row(1, 4)))
    }

    assert(deltaScan.size("total").bytesCompressed ===
      deltaScanWithLimit.size("total").bytesCompressed)

    assert(deltaScan.size("scanned").bytesCompressed !=
      deltaScanWithLimit.size("scanned").bytesCompressed)

    assert(deltaScan.size("scanned").bytesCompressed.get <
      deltaScan.size("total").bytesCompressed.get)
    assert(deltaScanWithLimit.size("scanned").rows === Some(1L))
    assert(deltaScanWithLimit2.size("scanned").rows === Some(1L))
  }

  test("with non-partition filter") {
    val dir = Utils.createTempDir()
    val ds = Seq((1, 4), (2, 5), (3, 6)).toDF("key", "value").as[(Int, Int)]
    ds.write.format("delta").partitionBy("key").save(dir.toString)

    val Seq(deltaScan) = getScanReport { // this query should not trigger limit push-down
      spark.read.format("delta").load(dir.toString)
        .where("key > 1")
        .where("value > 4")
        .limit(1)
        .collect()
    }
    assert(deltaScan.size("scanned").rows === Some(2L))
  }

  test("limit push-down flag") {
    val dir = Utils.createTempDir()
    val ds = Seq((1, 4), (2, 5), (3, 6)).toDF("key", "value").as[(Int, Int)]
    ds.write.format("delta").partitionBy("key").save(dir.toString)

    val Seq(baseline, scan, scan2) = getScanReport {
      withSQLConf(DeltaSQLConf.DELTA_LIMIT_PUSHDOWN_ENABLED.key -> "true") {
        spark.read.format("delta").load(dir.toString).where("key > 1").limit(1).collect()
      }
      withSQLConf(DeltaSQLConf.DELTA_LIMIT_PUSHDOWN_ENABLED.key -> "false") {
        spark.read.format("delta").load(dir.toString).where("key > 1").limit(1).collect()
        spark.read.format("delta").load(dir.toString).limit(2).collect()
      }
    }
    assert(scan.size("scanned").bytesCompressed.get > baseline.size("scanned").bytesCompressed.get)
    assert(scan2.size("scanned").bytesCompressed === scan2.size("total").bytesCompressed)
  }

  test("GlobalLimit should be kept") {
    val dir = Utils.createTempDir()
    (1 to 10).toDF.repartition(5).write.format("delta").save(dir.toString)
    assert(spark.read.format("delta").load(dir.toString).limit(5).collect().size == 5)
  }

  test("Works with union") {
    val dir = Utils.createTempDir()
    (1 to 10).toDF.repartition(5).write.format("delta").save(dir.toString)
    val t1 = spark.read.format("delta").load(dir.toString)
    val t2 = spark.read.format("delta").load(dir.toString)
    val union = t1.union(t2)

    withSQLConf(DeltaSQLConf.DELTA_LIMIT_PUSHDOWN_ENABLED.key -> "true") {
      val Seq(scanFull1, scanFull2) = getScanReport {
        union.collect()
      }
      val Seq(scanLimit1, scanLimit2) = getScanReport {
        union.limit(1).collect()
      }

      assert(scanFull1.size("scanned").bytesCompressed.get >
        scanLimit1.size("scanned").bytesCompressed.get)
      assert(scanFull2.size("scanned").bytesCompressed.get >
        scanLimit2.size("scanned").bytesCompressed.get)
    }
  }

  private def withDVSettings(thunk: => Unit): Unit = {
    withSQLConf(
      DeltaSQLConf.DELTA_OPTIMIZE_METADATA_QUERY_ENABLED.key -> "false"
    ) {
      withDeletionVectorsEnabled() {
        thunk
      }
    }
  }

  test(s"Verify limit correctness in the presence of DVs") {
    withDVSettings {
      val targetDF = spark.range(start = 0, end = 100, step = 1, numPartitions = 2)
        .withColumn("value", col("id"))

      withTempDeltaTable(targetDF) { (targetTable, targetLog) =>
        removeRowsFromAllFilesInLog(targetLog, numRowsToRemovePerFile = 10)
        verifyDVsExist(targetLog, 2)

        val targetDF = targetTable().toDF

        // We have 2 files 50 rows each. We deleted 10 rows from the first file. The first file
        // now contains 50 physical rows and 40 logical. Failing to take into account the DVs in
        // the first file results into prematurely terminating the scan and returning an
        // incorrect result. Note, the corner case in terms of correctness is when the limit is
        // set to 50. When statistics collection is disabled, we read both files.
        val limitToExpectedNumberOfFilesReadSeq = Range(10, 90, 10)
          .map(n => (n, if (n < 50) 1 else 2))

        for ((limit, expectedNumberOfFilesRead) <- limitToExpectedNumberOfFilesReadSeq) {
          val df = targetDF.limit(limit)

          // Assess correctness.
          assert(df.count === limit)

          val scanStats = getStats(df)

          // Check we do not read more files than needed.
          assert(scanStats.scanned.files === Some(expectedNumberOfFilesRead))

          // Verify physical and logical rows are updated correctly.
          val numDeletedRows = 10
          val numPhysicalRowsPerFile = 50
          val numTotalPhysicalRows = numPhysicalRowsPerFile * expectedNumberOfFilesRead
          val numTotalLogicalRows = numTotalPhysicalRows -
            (numDeletedRows * expectedNumberOfFilesRead)
          val expectedNumTotalPhysicalRows = Some(numTotalPhysicalRows)
          val expectedNumTotalLogicalRows = Some(numTotalLogicalRows)

          assert(scanStats.scanned.rows === expectedNumTotalPhysicalRows)
          assert(scanStats.scanned.logicalRows === expectedNumTotalLogicalRows)
        }
      }
    }
  }
}

class DeltaLimitPushDownV1Suite extends DeltaLimitPushDownTests

