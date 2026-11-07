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

package org.apache.spark.sql.delta.skipping.clustering

import java.sql.Date

import org.apache.spark.sql.delta.{DeltaAnalysisException, DeltaLog, DeltaOperations}
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.commands.optimize.ClusteringColumnFileOrdering
import org.apache.spark.sql.delta.commands.optimize.ClusteringColumnFileOrdering.ValueRange
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.DeltaStatistics.{MAX, MIN}
import org.apache.spark.sql.delta.test.{DeltaSQLCommandTest, DeltaSQLTestUtils}

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession

class LightweightClusteringSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest
  with DeltaSQLTestUtils {

  private val enabledKey = DeltaSQLConf.DELTA_OPTIMIZE_CLUSTERING_LIGHTWEIGHT_ENABLED.key
  private val columnKey = DeltaSQLConf.DELTA_OPTIMIZE_CLUSTERING_LIGHTWEIGHT_COLUMN.key
  private val thresholdKey =
    DeltaSQLConf.DELTA_OPTIMIZE_CLUSTERING_LIGHTWEIGHT_MAX_UNCLUSTERED_BYTES.key
  private val startDate = Date.valueOf("2026-01-01")

  // One file is written per day. The row counts make the size order interleave the days
  // (0, 2, 4, 6, 1, 3, 5, 7), so grouping files by size mixes non-adjacent days.
  private val rowsPerDay = Seq(0, 4, 1, 5, 2, 6, 3, 7).map(1000 + 50 * _)

  private val groupedByDay = Set((0, 1), (2, 3), (4, 5), (6, 7))
  private val groupedBySize = Set((0, 2), (4, 6), (1, 3), (5, 7))

  private def createTable(
      path: String,
      clusterBy: String,
      columnMappingMode: String = "none"): Unit = {
    sql(
      s"""CREATE TABLE delta.`$path` (id BIGINT, d DATE, n STRUCT<d: DATE>, s STRING)
         |USING delta CLUSTER BY ($clusterBy)
         |TBLPROPERTIES ('delta.columnMapping.mode' = '$columnMappingMode')""".stripMargin)
  }

  private def appendOneFilePerDay(path: String, days: Seq[Int] = rowsPerDay.indices): Unit = {
    days.foreach { day =>
      spark.range(rowsPerDay(day))
        .selectExpr(
          "id",
          s"date_add(date'$startDate', $day) AS d",
          s"named_struct('d', date_add(date'$startDate', $day)) AS n",
          "cast(id AS STRING) AS s")
        .coalesce(1)
        .write.format("delta").mode("append").save(path)
    }
  }

  private def files(path: String): Seq[AddFile] =
    DeltaLog.forTable(spark, path).update().allFiles.collect().toSeq

  /** Bins hold exactly two of the files written per day: any two fit, no three do. */
  private def maxFileSizeForTwoFileBins(path: String): Long = {
    val sizes = files(path).map(_.size).sorted
    assert(sizes.take(3).sum > sizes.takeRight(2).sum)
    sizes.takeRight(2).sum
  }

  /** The (min, max) day of the column `d` of the given files, or of every file in the table. */
  private def dayRanges(path: String, paths: Option[Set[String]] = None): Set[(Int, Int)] = {
    val snapshot = DeltaLog.forTable(spark, path).update()
    def day(date: Date): Int =
      (date.toLocalDate.toEpochDay - startDate.toLocalDate.toEpochDay).toInt
    snapshot.withStats
      .select(
        snapshot.getStatsColumnOrNullLiteral(MIN, Seq("d")),
        snapshot.getStatsColumnOrNullLiteral(MAX, Seq("d")),
        snapshot.withStats.col("path"))
      .collect()
      .filter(r => paths.forall(_.contains(r.getString(2))))
      .map(r => (day(r.getDate(0)), day(r.getDate(1))))
      .toSet
  }

  private def optimize(path: String, maxFileSize: Long, full: Boolean = false): Unit = {
    withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_MAX_FILE_SIZE.key -> maxFileSize.toString) {
      sql(s"OPTIMIZE delta.`$path`${if (full) " FULL" else ""}")
    }
  }

  private def lastOptimizeClusterBy(path: String): String = {
    val lastCommit = DeltaLog.forTable(spark, path).history.getHistory(Some(1)).head
    assert(lastCommit.operation === DeltaOperations.OPTIMIZE_OPERATION_NAME)
    lastCommit.operationParameters(DeltaOperations.CLUSTERING_PARAMETER_KEY)
  }

  private def isClustered(file: AddFile): Boolean = file.clusteringProvider.nonEmpty

  private def checkData(path: String, days: Seq[Int] = rowsPerDay.indices): Unit = {
    val expected = days.flatMap { day =>
      (0 until rowsPerDay(day)).map { id =>
        val date = Date.valueOf(startDate.toLocalDate.plusDays(day))
        Row(id.toLong, date, Row(date), id.toString)
      }
    }
    checkAnswer(spark.read.format("delta").load(path), expected)
  }

  test("OPTIMIZE clusters by default") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      createTable(path, clusterBy = "id, d")
      appendOneFilePerDay(path)
      optimize(path, maxFileSizeForTwoFileBins(path))
      assert(files(path).forall(isClustered))
      assert(lastOptimizeClusterBy(path) === """["id","d"]""")
      checkData(path)
    }
  }

  for (columnMappingMode <- Seq("none", "name"))
  test(s"files are grouped by the narrowest clustering column - " +
      s"columnMapping: $columnMappingMode") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      // Every file covers all values of `id` and `s`, but a single day of `d`.
      createTable(path, clusterBy = "id, s, d", columnMappingMode)
      appendOneFilePerDay(path)
      withSQLConf(enabledKey -> "true") {
        optimize(path, maxFileSizeForTwoFileBins(path))
      }
      assert(files(path).size === 4)
      assert(files(path).forall(!isClustered(_)))
      assert(dayRanges(path) === groupedByDay)
      assert(lastOptimizeClusterBy(path) === "[]")
      checkData(path)
    }
  }

  test("files are grouped by size if no clustering column is narrow") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      createTable(path, clusterBy = "id, s")
      appendOneFilePerDay(path)
      withSQLConf(enabledKey -> "true") {
        optimize(path, maxFileSizeForTwoFileBins(path))
      }
      assert(files(path).forall(!isClustered(_)))
      assert(dayRanges(path) === groupedBySize)
      checkData(path)
    }
  }

  for ((column, expected) <- Seq(
      "d" -> groupedByDay,
      "D" -> groupedByDay,
      "`n`.d" -> groupedByDay,
      // Every file has the same min `id`, so files are ordered by their max `id`, i.e. by size.
      "id" -> groupedBySize))
  test(s"files are grouped by the configured clustering column - column: $column") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      createTable(path, clusterBy = "id, d, n.d")
      appendOneFilePerDay(path)
      withSQLConf(enabledKey -> "true", columnKey -> column) {
        optimize(path, maxFileSizeForTwoFileBins(path))
      }
      assert(dayRanges(path) === expected)
      checkData(path)
    }
  }

  test("configured column must be a clustering column") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      createTable(path, clusterBy = "id, d")
      appendOneFilePerDay(path)
      withSQLConf(enabledKey -> "true", columnKey -> "s") {
        val e = intercept[DeltaAnalysisException] {
          optimize(path, maxFileSizeForTwoFileBins(path))
        }
        checkError(
          e,
          "DELTA_INVALID_LIGHTWEIGHT_CLUSTERING_COLUMN",
          parameters = Map(
            "column" -> "s",
            "config" -> columnKey,
            "clusteringColumns" -> "id, d"))
      }
    }
  }

  test("clustered files are not rewritten") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      createTable(path, clusterBy = "id, d")
      appendOneFilePerDay(path, days = 0 until 4)
      optimize(path, maxFileSize = 1L << 30)
      val clusteredFiles = files(path).map(_.path).toSet
      assert(files(path).forall(isClustered))

      appendOneFilePerDay(path, days = 4 until 8)
      val newFiles = files(path).filterNot(isClustered)
      val sizes = newFiles.map(_.size).sorted
      withSQLConf(enabledKey -> "true") {
        // Room for two new files per bin.
        optimize(path, maxFileSize = sizes.takeRight(2).sum)
      }
      val filesAfter = files(path)
      assert(clusteredFiles.subsetOf(filesAfter.map(_.path).toSet))
      val compactedFiles = filesAfter.filterNot(f => clusteredFiles.contains(f.path))
      assert(compactedFiles.forall(!isClustered(_)))
      assert(dayRanges(path, Some(compactedFiles.map(_.path).toSet)) === Set((4, 5), (6, 7)))
      checkData(path)
    }
  }

  test("OPTIMIZE clusters once unclustered data reaches the threshold") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      createTable(path, clusterBy = "id, d")
      appendOneFilePerDay(path)
      val unclusteredBytes = files(path).map(_.size).sum
      withSQLConf(enabledKey -> "true", thresholdKey -> unclusteredBytes.toString) {
        optimize(path, maxFileSizeForTwoFileBins(path))
      }
      assert(files(path).forall(isClustered))
      assert(lastOptimizeClusterBy(path) === """["id","d"]""")
      checkData(path)
    }
  }

  test("OPTIMIZE FULL always clusters") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      createTable(path, clusterBy = "id, d")
      appendOneFilePerDay(path)
      withSQLConf(enabledKey -> "true") {
        optimize(path, maxFileSizeForTwoFileBins(path), full = true)
      }
      assert(files(path).forall(isClustered))
      checkData(path)
    }
  }

  test("lightweight compaction output is clustered later") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      createTable(path, clusterBy = "id, d")
      appendOneFilePerDay(path)
      withSQLConf(enabledKey -> "true") {
        optimize(path, maxFileSizeForTwoFileBins(path))
      }
      assert(files(path).forall(!isClustered(_)))
      optimize(path, maxFileSize = 1L << 30)
      assert(files(path).forall(isClustered))
      checkData(path)
    }
  }

  test("auto compaction runs lightweight compaction") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      createTable(path, clusterBy = "id, d")
      withSQLConf(
          enabledKey -> "true",
          DeltaSQLConf.DELTA_AUTO_COMPACT_ENABLED.key -> "true",
          DeltaSQLConf.DELTA_AUTO_COMPACT_MIN_NUM_FILES.key -> rowsPerDay.size.toString) {
        appendOneFilePerDay(path)
      }
      val lastCommit = DeltaLog.forTable(spark, path).history.getHistory(Some(1)).head
      assert(lastCommit.operation === DeltaOperations.OPTIMIZE_OPERATION_NAME)
      assert(lastCommit.operationParameters("auto") === "true")
      assert(lastOptimizeClusterBy(path) === "[]")
      assert(files(path).size < rowsPerDay.size)
      assert(files(path).forall(!isClustered(_)))
      checkData(path)
    }
  }

  test("relativeFileRange") {
    import ClusteringColumnFileOrdering.relativeFileRange
    def ranges(rs: (Any, Any)*): Seq[ValueRange] =
      rs.map { case (min, max) => ValueRange(min, max) }
    assert(relativeFileRange(ranges((1, 1), (2, 2), (3, 3))) === Some(0.0))
    assert(relativeFileRange(ranges((1, 2), (3, 4))) === Some(1.0 / 3))
    assert(relativeFileRange(ranges((1, 9), (1, 9))) === Some(1.0))
    assert(relativeFileRange(ranges(("a", "z"), ("b", "c"))) === Some((3.0 + 1.0) / 2 / 3))
    // Not enough files with statistics, or a single value.
    assert(relativeFileRange(ranges((1, 2), (null, null))) === None)
    assert(relativeFileRange(ranges((1, 1), (1, 1))) === None)
  }
}
