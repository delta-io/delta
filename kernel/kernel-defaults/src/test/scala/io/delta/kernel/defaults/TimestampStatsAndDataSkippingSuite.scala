/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults

import java.io.File
import java.time.{LocalDateTime, ZoneOffset}

import scala.collection.immutable.Seq
import scala.jdk.CollectionConverters.collectionAsScalaIterableConverter

import io.delta.kernel.data.{ColumnarBatch, ColumnVector}
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch
import io.delta.kernel.defaults.internal.data.vector.DefaultGenericVector
import io.delta.kernel.defaults.internal.parquet.ParquetSuiteBase
import io.delta.kernel.defaults.utils.WriteUtils
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.util.JsonUtils
import io.delta.kernel.types.{StructType, TimestampNTZType, TimestampType}

import org.apache.spark.sql.delta.DeltaLog

import org.apache.hadoop.fs.Path
import org.scalatest.funsuite.AnyFunSuite

/**
 * Tests timestamp statistics serialization and data skipping behavior
 * for TIMESTAMP and TIMESTAMP_NTZ types.
 */
class TimestampStatsAndDataSkippingSuite extends AnyFunSuite with WriteUtils
    with DataSkippingDeltaTestsUtils
    with ParquetSuiteBase {

  test("verify on-disk TIMESTAMP stats format is equal when writing through spark and kernel") {
    withTempDirAndEngine { (dir, engine) =>
      // Test with TIMESTAMP and TIMESTAMP_NTZ to verify serialization format
      val schema = new StructType()
        .add("timestampCol", TimestampType.TIMESTAMP)
        .add("timestampNtzCol", TimestampNTZType.TIMESTAMP_NTZ)

      // Create different batches with different timestamp ranges to test multiple boundaries
      val timestampRanges = Seq(
        ("2019-06-09 01:02:04.123456", "2019-09-09 01:02:04.123999"),
        ("2019-06-09 01:02:04.123456", "2019-09-09 01:02:05.123999"),
        // Microsecond boundary
        ("2019-09-09 01:02:03.456789", "2019-09-09 01:02:03.456999"),
        // End-of-millisecond boundary
        ("2019-09-09 01:02:03.999000", "2019-09-09 01:02:03.999999"),
        ("2019-09-09 01:02:04.123456", "2019-09-09 01:02:04.123999"))

      // Create "kernel" and "spark-copy" directories
      val kernelPath = new File(dir, "kernel").getAbsolutePath
      val sparkTablePath = new File(dir, "spark-copy").getAbsolutePath

      // Write through Kernel
      timestampRanges.zipWithIndex.foreach { case ((minTs, maxTs), fileIndex) =>
        val batch = createTimestampBatch(schema, minTs, maxTs, rowsPerFile = 10)
        appendData(
          engine,
          kernelPath,
          isNewTable = fileIndex == 0,
          schema = if (fileIndex == 0) schema else null,
          partCols = Seq.empty,
          data = Seq(Map.empty[String, Literal] -> Seq(batch.toFiltered(Option.empty))))
      }

      val kernelDf = spark.read.format("delta").load(kernelPath)
      val kernelFiles = kernelDf.inputFiles

      log.info(s"Found ${kernelFiles.length} files from Kernel")

      withSparkTimeZone("UTC") {
        kernelFiles.zipWithIndex.foreach { case (filePath, fileIndex) =>
          val singleFileDf = spark.read.parquet(filePath)
          if (fileIndex == 0) {
            singleFileDf.write.format("delta").mode("overwrite").save(sparkTablePath)
          } else {
            singleFileDf.write.format("delta").mode("append").save(sparkTablePath)
          }
        }
      }

      val mapper = JsonUtils.mapper()
      val kernelStats = collectStatsFromAddFiles(engine, kernelPath).map(mapper.readTree)
      val sparkStats = collectStatsFromAddFiles(engine, sparkTablePath).map(mapper.readTree)

      require(
        kernelStats.nonEmpty && sparkStats.nonEmpty,
        "stats collected from AddFiles should be non-empty")
      assert(
        kernelStats.toSet == sparkStats.toSet,
        s"\nKernel stats:\n${kernelStats.mkString("\n")}\n" +
          s"Spark  stats:\n${sparkStats.mkString("\n")}")
    }
  }

  //////////////////////////////////////////////////////////////////////////////////
  // Timestamp Data Skipping Tests that mirror those of Delta-Spark
  //////////////////////////////////////////////////////////////////////////////////
  for (timestampType <- Seq(TimestampType.TIMESTAMP, TimestampNTZType.TIMESTAMP_NTZ)) {
    test(s"validate basic delta-spark data-skipping on ${timestampType.getClass.getSimpleName}") {
      withTempDirAndEngine { (kernelPath, engine) =>
        val schema = new StructType().add("ts", timestampType)

        // Generate multiple files with different timestamp ranges.
        val timestampRanges = Seq(
          ("2019-01-01 12:00:00.123456", "2019-01-01 18:00:00.999999"),
          ("2019-09-09 01:02:03.456789", "2019-09-09 01:02:03.456789"),
          ("2019-12-31 20:00:00.100000", "2019-12-31 23:59:59.999999"),
          ("2020-06-15 10:30:45.555555", "2020-06-15 15:45:30.888888"),
          ("2021-03-20 08:15:22.777777", "2021-03-20 16:42:18.333333"))

        timestampRanges.zipWithIndex.foreach { case ((minTs, maxTs), fileIndex) =>
          val batch = createTimestampBatch(schema, minTs, maxTs, rowsPerFile = 10)

          appendData(
            engine,
            kernelPath,
            isNewTable = fileIndex == 0,
            schema = if (fileIndex == 0) schema else null,
            partCols = Seq.empty,
            data = Seq(Map.empty[String, Literal] -> Seq(batch.toFiltered(Option.empty))))
        }

        // Query with all predicates in UTC for this test.
        // We mainly want to validate that the scan results are correct.
        withSparkTimeZone("UTC") {
          val deltaLogPath = DeltaLog.forTable(spark, new Path(kernelPath))

          val exactHits = Seq(
            // Files 1,2,3,4
            (s"ts >= ${createTimestampLiteral(timestampType, "2019-09-09 01:02:03.456789")}", 4),
            // Files 1,2,3,4 (millisecond expansion)
            (s"ts <= ${createTimestampLiteral(timestampType, "2019-09-09 01:02:03.456789")}", 2),
            // Files 1,2,3,4 (tests millisecond expansion in MAX due to truncation)
            (s"ts >= ${createTimestampLiteral(timestampType, "2019-09-09 01:02:03.456790")}", 4),
            // File 1 only
            (s"ts = ${createTimestampLiteral(timestampType, "2019-09-09 01:02:03.456789")}", 1))

          exactHits.foreach { case (predicate, expectedFiles) =>
            val filesHit = filesReadCount(spark, deltaLogPath, predicate)
            assert(
              filesHit == expectedFiles,
              s"Expected exactly $expectedFiles files for: $predicate, but got $filesHit files")
          }

          // Test range queries (should hit multiple files)
          val rangeHits = Seq(
            (s"ts >= ${createTimestampLiteral(timestampType, "2019-01-01 00:00:00")}", 5),
            (s"ts >= ${createTimestampLiteral(timestampType, "2020-01-01 00:00:00")}", 3),
            (s"ts >= ${createTimestampLiteral(timestampType, "2019-06-01 00:00:00")}", 4),
            (s"ts <= ${createTimestampLiteral(timestampType, "2019-06-01 00:00:00")}", 1),
            (
              s"ts BETWEEN ${createTimestampLiteral(timestampType, "2019-09-01 00:00:00")} " +
                s"AND ${createTimestampLiteral(timestampType, "2019-09-30 23:59:59")}",
              1),
            (
              s"ts BETWEEN ${createTimestampLiteral(timestampType, "2019-01-01 00:00:00")} " +
                s"AND ${createTimestampLiteral(timestampType, "2019-12-31 23:59:59")}",
              3))

          rangeHits.foreach { case (predicate, expectedFiles) =>
            val filesHit = filesReadCount(spark, deltaLogPath, predicate)
            assert(
              filesHit == expectedFiles,
              s"Expected exactly $expectedFiles files for: $predicate, but got $filesHit files")
          }

          // Test precise misses (outside the millisecond range due to truncation )
          val preciseCases = Seq(
            // Files 1, 2, 3. Next millisecond
            // ${createTimestampLiteral(timestampType, "2019-01-01 00:00:00")}
            (s"ts > ${createTimestampLiteral(timestampType, "2019-09-09 01:02:03.457000")}", 3),
            (s"ts < ${createTimestampLiteral(timestampType, "2019-09-09 01:02:03.456000")}", 1),
            // Year boundary
            (s"ts >= ${createTimestampLiteral(timestampType, "2020-01-01 00:00:00.000001")}", 2),
            // True misses (0 files)
            (s"ts >= ${createTimestampLiteral(timestampType, "2022-01-01 00:00:00")}", 0),
            (s"ts <= ${createTimestampLiteral(timestampType, "2018-01-01 00:00:00")}", 0))

          preciseCases.foreach { case (predicate, expectedFiles) =>
            val filesHit = filesReadCount(spark, deltaLogPath, predicate)
            assert(
              filesHit == expectedFiles,
              s"Expected exactly $expectedFiles files for: $predicate, but got $filesHit files")
          }

          // Test that we have the expected total number of files
          val totalFiles = filesReadCount(spark, deltaLogPath, "TRUE")
          assert(totalFiles == 5, s"Expected 5 total files, but got $totalFiles")
        }
      }
    }
  }

  // Test timezone boundary behavior with a single timestamp across multiple timezones
  test(s"kernel data skipping timezone boundary behavior for TIMESTAMP type") {
    withTempDirAndEngine { (kernelPath, engine) =>
      val timestampType = TimestampType.TIMESTAMP
      val schema = new StructType().add("ts", timestampType)

      val testTimestamp = "2019-09-09 01:02:03.456789"
      val batch = createTimestampBatch(schema, testTimestamp, testTimestamp, rowsPerFile = 1)

      appendData(
        engine,
        kernelPath,
        isNewTable = true,
        schema,
        partCols = Seq.empty,
        data = Seq(Map.empty[String, Literal] -> Seq(batch.toFiltered(Option.empty))))

      val sparkLog = DeltaLog.forTable(spark, new Path(kernelPath))

      // Test UTC timezone-aware queries
      val utcHits = Seq(
        s"ts >= ${createTimestampLiteral(timestampType, "2019-09-09 01:02:03.456789+00:00")}",
        s"ts <= ${createTimestampLiteral(timestampType, "2019-09-09 01:02:03.456789+00:00")}",
        s"ts >= ${createTimestampLiteral(timestampType, "2019-09-09 01:02:03.456789 UTC")}",
        s"TS >= ${createTimestampLiteral(timestampType, "2019-09-09 01:02:03.456789+00:00")}")

      val utcMisses = Seq(
        s"ts >= ${createTimestampLiteral(timestampType, "2019-09-09 01:02:03.457001+00:00")}",
        s"ts <= ${createTimestampLiteral(timestampType, "2019-09-04 01:02:03.455999+00:00")}",
        s"TS >= ${createTimestampLiteral(timestampType, "2019-09-09 01:02:03.457001 UTC")}")

      // Test PST timezone-aware queries
      val pstHits = Seq(
        s"ts >= ${createTimestampLiteral(timestampType, "2019-09-08 17:02:03.456789-08:00")}",
        s"ts <= ${createTimestampLiteral(timestampType, "2019-09-08 17:02:03.456789-08:00")}",
        s"ts >= ${createTimestampLiteral(timestampType, "2019-09-08 17:02:03.456789 PST")}")

      val pstMisses = Seq(
        s"ts >= ${createTimestampLiteral(timestampType, "2019-09-08 17:02:03.457001-08:00")}",
        s"ts <= ${createTimestampLiteral(timestampType, "2019-09-08 17:02:03.455999-08:00")}")

      utcHits.foreach { predicate =>
        val filesHit = filesReadCount(spark, sparkLog, predicate)
        assert(filesHit == 1, s"Expected UTC hit but got miss for $predicate")
      }

      utcMisses.foreach { predicate =>
        val filesHit = filesReadCount(spark, sparkLog, predicate)
        assert(filesHit == 0, s"Expected UTC miss but got hit for $predicate")
      }

      pstHits.foreach { predicate =>
        val filesHit = filesReadCount(spark, sparkLog, predicate)
        assert(filesHit == 1, s"Expected PST hit but got miss for $predicate")
      }

      pstMisses.foreach { predicate =>
        val filesHit = filesReadCount(spark, sparkLog, predicate)
        assert(filesHit == 0, s"Expected PST miss but got hit for $predicate")
      }
    }
  }

  private def createTimestampBatch(
      schema: StructType,
      minTimestampStr: String,
      maxTimestampStr: String,
      rowsPerFile: Int): ColumnarBatch = {
    val minMicros = parseTimestampToMicros(minTimestampStr)
    val maxMicros = parseTimestampToMicros(maxTimestampStr)

    val timestampValues = (0 until rowsPerFile).map { rowIndex =>
      if (rowIndex != 0 && rowIndex % 4 == 0) {
        null
      } else {
        val fraction = if (rowsPerFile == 1) 0.0 else rowIndex.toDouble / (rowsPerFile - 1)
        val interpolatedMicros = minMicros + ((maxMicros - minMicros) * fraction).toLong
        interpolatedMicros
      }
    }.toArray.asInstanceOf[Array[AnyRef]]

    val vectors = schema.fields().asScala.map { field =>
      DefaultGenericVector.fromArray(field.getDataType, timestampValues)
    }.toArray.asInstanceOf[Array[ColumnVector]]

    new DefaultColumnarBatch(rowsPerFile, schema, vectors)
  }

  /**
   * Parse timestamp string to microseconds since epoch
   */
  private def parseTimestampToMicros(timestampStr: String): Long = {
    // Parse "2019-09-09 01:02:03.456789" format
    val localDateTime = LocalDateTime.parse(timestampStr.replace(" ", "T"))
    val instant = localDateTime.toInstant(ZoneOffset.UTC)
    instant.getEpochSecond * 1000000L + instant.getNano / 1000L
  }

  // Create type-appropriate literal function
  def createTimestampLiteral(
      timestampType: io.delta.kernel.types.DataType,
      timestamp: String): String = {
    timestampType match {
      case _: TimestampType => s"TIMESTAMP'$timestamp'"
      case _: TimestampNTZType => s"TIMESTAMP_NTZ'$timestamp'"
      case _ => throw new IllegalArgumentException(s"Unsupported type: $timestampType")
    }
  }
}
