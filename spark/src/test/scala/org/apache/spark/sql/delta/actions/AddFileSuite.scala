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

package org.apache.spark.sql.delta.actions

import org.apache.spark.sql.delta.{DeltaConfigs, DeltaLog, DeltaRuntimeException}
import org.apache.spark.sql.delta.DeltaTestUtils.BOOLEAN_DOMAIN
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{Cast, Literal}
import org.apache.spark.sql.errors.QueryErrorsBase
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class AddFileSuite extends SparkFunSuite with SharedSparkSession with QueryErrorsBase {

  private def createAddFileWithPartitionValue(partitionValues: Map[String, String]): AddFile = {
    AddFile(
      path = "test.parquet",
      partitionValues = partitionValues,
      size = 100,
      modificationTime = 0,
      dataChange = true)
  }

  private def timestampLiteral(value: String, tz: String = "UTC"): Literal = {
    Literal.create(
      Cast(Literal(value), TimestampType, Some(tz), ansiEnabled = false).eval(),
      TimestampType)
  }

  private def dateLiteral(value: String): Literal = {
    Literal.create(
      Cast(Literal(value), DateType, None, ansiEnabled = false).eval(),
      DateType)
  }

  private def timestampNTZLiteral(value: String): Literal = {
    Literal.create(
      Cast(Literal(value), TimestampNTZType, None, ansiEnabled = false).eval(),
      TimestampNTZType)
  }

  test("normalizedPartitionValues for non-timestamp partitions returns typed literals") {
    withSQLConf(DeltaSQLConf.DELTA_NORMALIZE_PARTITION_VALUES_ON_READ.key -> "true") {
      withTempDir { tempDir =>
        spark.createDataFrame(
          spark.sparkContext.emptyRDD[org.apache.spark.sql.Row],
          StructType(Seq(
            StructField("data", StringType),
            StructField("strCol", StringType),
            StructField("intCol", IntegerType)
          ))
        ).write.format("delta").partitionBy("strCol", "intCol").save(tempDir.getCanonicalPath)
        val deltaTxn = DeltaLog.forTable(spark, tempDir.getCanonicalPath).startTransaction()

        val file = createAddFileWithPartitionValue(Map("strCol" -> "value1", "intCol" -> "42"))
        val normalized = file.normalizedPartitionValues(spark, deltaTxn)

        assert(normalized("strCol") == Literal("value1"))
        assert(normalized("intCol") == Literal.create(42, IntegerType))
      }
    }
  }

  test("normalizedPartitionValues for timestamp partitions with well formatted values") {
    withSQLConf(DeltaSQLConf.DELTA_NORMALIZE_PARTITION_VALUES_ON_READ.key -> "true") {
      withTempDir { tempDir =>
        spark.createDataFrame(
          spark.sparkContext.emptyRDD[org.apache.spark.sql.Row],
          StructType(Seq(
            StructField("data", StringType),
            StructField("tsCol", TimestampType)
          ))
        ).write.format("delta").partitionBy("tsCol").save(tempDir.getCanonicalPath)
        val deltaTxn = DeltaLog.forTable(spark, tempDir.getCanonicalPath).startTransaction()

        val file = createAddFileWithPartitionValue(Map("tsCol" -> "2000-01-01T20:00:00.000000Z"))
        val normalized = file.normalizedPartitionValues(spark, deltaTxn)

        assert(normalized("tsCol") == timestampLiteral("2000-01-01T20:00:00.000000Z"))
      }
    }
  }

  for (enableNormalization <- BOOLEAN_DOMAIN) {
    test("normalizedPartitionValues for UTC timestamps partitions with different string formats, " +
      s"enableNormalization=$enableNormalization") {
      withSQLConf(
        DeltaSQLConf.DELTA_NORMALIZE_PARTITION_VALUES_ON_READ.key ->
          enableNormalization.toString,
        "spark.sql.session.timeZone" -> "UTC") {
        withTempDir { tempDir =>
          // Create empty Delta table with tsCol as partition column
          spark.createDataFrame(
            spark.sparkContext.emptyRDD[org.apache.spark.sql.Row],
            StructType(Seq(
              StructField("data", StringType),
              StructField("tsCol", TimestampType)
            ))
          ).write.format("delta").partitionBy("tsCol").save(tempDir.getCanonicalPath)
          val deltaTxn = DeltaLog.forTable(spark, tempDir.getCanonicalPath).startTransaction()

          val fileNonUtc = createAddFileWithPartitionValue(Map("tsCol" -> "2000-01-01 12:00:00"))
          val fileUtc =
            createAddFileWithPartitionValue(Map("tsCol" -> "2000-01-01T12:00:00.000000Z"))
          val normalizedNonUtc = fileNonUtc.normalizedPartitionValues(spark, deltaTxn)
          val normalizedUtc = fileUtc.normalizedPartitionValues(spark, deltaTxn)

          if (enableNormalization) {
            assert(normalizedNonUtc == normalizedUtc)
          } else {
            assert(normalizedNonUtc != normalizedUtc)
          }
        }
      }
    }
  }

  for (enableNormalization <- BOOLEAN_DOMAIN) {
    test("normalizedPartitionValues for TimestampNTZ partitions returns the correct literal, " +
      s"enableNormalization=$enableNormalization") {
      // Per Delta protocol, TimestampNTZ values should be stored as
      // "{year}-{month}-{day} {hour}:{minute}:{second}" without any time zone conversion
      withSQLConf(
        DeltaSQLConf.DELTA_NORMALIZE_PARTITION_VALUES_ON_READ.key -> enableNormalization.toString
      ) {
        withTempDir { tempDir =>
          spark.createDataFrame(
            spark.sparkContext.emptyRDD[org.apache.spark.sql.Row],
            StructType(Seq(
              StructField("data", StringType),
              StructField("tsCol", TimestampNTZType)
            ))
          ).write.format("delta").partitionBy("tsCol").save(tempDir.getCanonicalPath)
          val deltaTxn = DeltaLog.forTable(spark, tempDir.getCanonicalPath).startTransaction()

          val file = createAddFileWithPartitionValue(Map("tsCol" -> "2000-01-01 12:00:00"))
          val normalized = file.normalizedPartitionValues(spark, deltaTxn)

          if (enableNormalization) {
            assert(normalized("tsCol") == timestampNTZLiteral("2000-01-01 12:00:00"))
          } else {
            assert(normalized("tsCol") == Literal("2000-01-01 12:00:00"))
          }
        }
      }
    }
  }

  test("normalizedPartitionValues preserves null partition values") {
    withSQLConf(DeltaSQLConf.DELTA_NORMALIZE_PARTITION_VALUES_ON_READ.key -> "true") {
      withTempDir { tempDir =>
        spark.createDataFrame(
          spark.sparkContext.emptyRDD[org.apache.spark.sql.Row],
          StructType(Seq(
            StructField("data", StringType),
            StructField("tsCol", TimestampType),
            StructField("strCol", StringType)
          ))
        ).write.format("delta").partitionBy("tsCol", "strCol").save(tempDir.getCanonicalPath)
        val deltaTxn = DeltaLog.forTable(spark, tempDir.getCanonicalPath).startTransaction()

        val file = createAddFileWithPartitionValue(Map("tsCol" -> null, "strCol" -> "value"))
        val normalized = file.normalizedPartitionValues(spark, deltaTxn)

        assert(normalized("tsCol") == timestampLiteral(null))
        assert(normalized("strCol") == Literal("value"))
      }
    }
  }

  for (enableNormalization <- BOOLEAN_DOMAIN) {
    test("normalizedPartitionValues with mixed timestamp and non-timestamp partitions, " +
      s"enableNormalization=$enableNormalization") {
      withSQLConf(
        DeltaSQLConf.DELTA_NORMALIZE_PARTITION_VALUES_ON_READ.key ->
          enableNormalization.toString,
        "spark.sql.session.timeZone" -> "UTC"
      ) {
        withTempDir { tempDir =>
          spark.createDataFrame(
            spark.sparkContext.emptyRDD[org.apache.spark.sql.Row],
            StructType(Seq(
              StructField("data", StringType),
              StructField("tsCol", TimestampType),
              StructField("strCol", StringType),
              StructField("intCol", IntegerType)
            ))
          ).write.format("delta")
            .partitionBy("tsCol", "strCol", "intCol")
            .save(tempDir.getCanonicalPath)
          val deltaTxn = DeltaLog.forTable(spark, tempDir.getCanonicalPath).startTransaction()

          val file1 = createAddFileWithPartitionValue(
            Map("tsCol" -> "2000-01-01 12:00:00", "strCol" -> "value", "intCol" -> "42"))
          val file2 = createAddFileWithPartitionValue(
            Map("tsCol" -> "2000-01-01T12:00:00.000000Z", "strCol" -> "value", "intCol" -> "42"))
          val normalized1 = file1.normalizedPartitionValues(spark, deltaTxn)
          val normalized2 = file2.normalizedPartitionValues(spark, deltaTxn)

          if (enableNormalization) {
            // Timestamp columns should normalize to same value (same microseconds)
            assert(normalized1("tsCol") == normalized2("tsCol"))
            // Non-timestamp columns should be typed literals
            assert(normalized1("strCol") == Literal("value"))
            assert(normalized1("intCol") == Literal.create(42, IntegerType))
          } else {
            // Without normalization the partition values are different string literals
            assert(normalized1 != normalized2)
            // Normalized partition values should be string literals of original values
            assert(normalized1("tsCol") == Literal("2000-01-01 12:00:00"))
            assert(normalized2("tsCol") == Literal("2000-01-01T12:00:00.000000Z"))
          }
        }
      }
    }
  }

  for (enableNormalization <- BOOLEAN_DOMAIN) {
    test("normalizedPartitionValues for equal timestamps with different time zone offsets, " +
      s"enableNormalization=$enableNormalization") {
      withSQLConf(
        DeltaSQLConf.DELTA_NORMALIZE_PARTITION_VALUES_ON_READ.key ->
          enableNormalization.toString,
        "spark.sql.session.timeZone" -> "UTC"
      ) {
        withTempDir { tempDir =>
          spark.createDataFrame(
            spark.sparkContext.emptyRDD[org.apache.spark.sql.Row],
            StructType(Seq(
              StructField("data", StringType),
              StructField("tsCol", TimestampType)
            ))
          ).write.format("delta").partitionBy("tsCol").save(tempDir.getCanonicalPath)
          val deltaTxn = DeltaLog.forTable(spark, tempDir.getCanonicalPath).startTransaction()

          // All three represent the same instant: 2000-01-01 12:00:00 UTC
          val fileUtc =
            createAddFileWithPartitionValue(Map("tsCol" -> "2000-01-01T12:00:00.000+0000"))
          // EST is UTC-5, so 07:00 EST = 12:00 UTC
          val fileEst =
            createAddFileWithPartitionValue(Map("tsCol" -> "2000-01-01T07:00:00.000-0500"))
          // PST is UTC-8, so 04:00 PST = 12:00 UTC
          val filePst =
            createAddFileWithPartitionValue(Map("tsCol" -> "2000-01-01T04:00:00.000-0800"))

          val normalizedUtc = fileUtc.normalizedPartitionValues(spark, deltaTxn)
          val normalizedEst = fileEst.normalizedPartitionValues(spark, deltaTxn)
          val normalizedPst = filePst.normalizedPartitionValues(spark, deltaTxn)

          if (enableNormalization) {
            // All should normalize to the same value since they represent the same moment
            assert(normalizedUtc == normalizedEst)
            assert(normalizedUtc == normalizedPst)
            assert(normalizedEst == normalizedPst)
          } else {
            // Without normalization, returns string literals of original values
            assert(normalizedUtc("tsCol") == Literal("2000-01-01T12:00:00.000+0000"))
            assert(normalizedEst("tsCol") == Literal("2000-01-01T07:00:00.000-0500"))
            assert(normalizedPst("tsCol") == Literal("2000-01-01T04:00:00.000-0800"))
            // The normalized values should be different since they're just string literals
            assert(normalizedUtc != normalizedEst)
            assert(normalizedUtc != normalizedPst)
            assert(normalizedEst != normalizedPst)
          }
        }
      }
    }
  }

  for (enableNormalization <- BOOLEAN_DOMAIN) {
    test("normalizedPartitionValues for same timestamp with different time zone notations, " +
      s"enableNormalization=$enableNormalization") {
      withSQLConf(
        DeltaSQLConf.DELTA_NORMALIZE_PARTITION_VALUES_ON_READ.key ->
          enableNormalization.toString,
        "spark.sql.session.timeZone" -> "UTC"
      ) {
        withTempDir { tempDir =>
          spark.createDataFrame(
            spark.sparkContext.emptyRDD[org.apache.spark.sql.Row],
            StructType(Seq(
              StructField("data", StringType),
              StructField("tsCol", TimestampType)
            ))
          ).write.format("delta").partitionBy("tsCol").save(tempDir.getCanonicalPath)
          val deltaTxn = DeltaLog.forTable(spark, tempDir.getCanonicalPath).startTransaction()

          // All three represent the same instant: 2000-01-15 12:00:00 UTC
          // CET time zone abbreviation (UTC+1)
          val fileCet = createAddFileWithPartitionValue(Map("tsCol" -> "2000-01-15 13:00:00 CET"))
          // Europe/Berlin time zone name (UTC+1 in winter)
          val fileEuropeBerlin =
            createAddFileWithPartitionValue(Map("tsCol" -> "2000-01-15 13:00:00 Europe/Berlin"))
          // Numeric offset notation: +0100
          val fileNumericOffset =
            createAddFileWithPartitionValue(Map("tsCol" -> "2000-01-15T13:00:00.000+0100"))

          val normalizedCet = fileCet.normalizedPartitionValues(spark, deltaTxn)
          val normalizedEuropeBerlin = fileEuropeBerlin.normalizedPartitionValues(spark, deltaTxn)
          val normalizedNumeric = fileNumericOffset.normalizedPartitionValues(spark, deltaTxn)

          if (enableNormalization) {
            // All should normalize to the same value since they represent the same moment
            assert(normalizedCet == normalizedEuropeBerlin,
              s"CET and Europe/Berlin should match: $normalizedCet vs $normalizedEuropeBerlin")
            assert(normalizedCet == normalizedNumeric,
              s"CET and numeric offset should match: $normalizedCet vs $normalizedNumeric")
          } else {
            // Without normalization, returns string literals of the original values
            assert(normalizedCet("tsCol") == Literal("2000-01-15 13:00:00 CET"))
            assert(normalizedEuropeBerlin("tsCol") == Literal("2000-01-15 13:00:00 Europe/Berlin"))
            assert(normalizedNumeric("tsCol") == Literal("2000-01-15T13:00:00.000+0100"))
            // The normalized values should be different since they're just string literals
            assert(normalizedCet != normalizedEuropeBerlin)
            assert(normalizedCet != normalizedNumeric)
            assert(normalizedNumeric != normalizedEuropeBerlin)
          }
        }
      }
    }
  }

  test("normalizedPartitionValues for DateType should return the original date string") {
    withSQLConf(
      DeltaSQLConf.DELTA_NORMALIZE_PARTITION_VALUES_ON_READ.key -> "true",
    ) {
      withTempDir { tempDir =>
        spark.createDataFrame(
          spark.sparkContext.emptyRDD[org.apache.spark.sql.Row],
          StructType(Seq(
            StructField("data", StringType),
            StructField("dateCol", DateType)
          ))
        ).write.format("delta").partitionBy("dateCol").save(tempDir.getCanonicalPath)
        val deltaTxn = DeltaLog.forTable(spark, tempDir.getCanonicalPath).startTransaction()

        val originalDateString = "2000-01-01"
        val file = createAddFileWithPartitionValue(Map("dateCol" -> originalDateString))

        val normalized = file.normalizedPartitionValues(spark, deltaTxn)
        val normalizedDateValue = normalized("dateCol")

        assert(normalizedDateValue == dateLiteral(originalDateString))
      }
    }
  }

  test("normalizedPartitionValues should handle __HIVE_DEFAULT_PARTITION__") {
    withSQLConf(
      DeltaSQLConf.DELTA_NORMALIZE_PARTITION_VALUES_ON_READ.key -> "true",
    ) {
      withTempDir { tempDir =>
        spark.createDataFrame(
          spark.sparkContext.emptyRDD[org.apache.spark.sql.Row],
          StructType(Seq(
            StructField("data", StringType),
            StructField("foo", IntegerType)
          ))
        ).write.format("delta").partitionBy("data").save(tempDir.getCanonicalPath)
        val deltaTxn = DeltaLog.forTable(spark, tempDir.getCanonicalPath).startTransaction()

        // Tombstone value __HIVE_DEFAULT_PARTITION__ should be preserved as a string for AddFiles
        val file = createAddFileWithPartitionValue(Map("data" -> "__HIVE_DEFAULT_PARTITION__"))
        val normalized = file.normalizedPartitionValues(spark, deltaTxn)

        assert(normalized("data") == Literal.create("__HIVE_DEFAULT_PARTITION__", StringType))
      }
    }
  }

  test("normalizedPartitionValues preserves escaped characters in AddFile partition values") {
    withSQLConf(
      DeltaSQLConf.DELTA_NORMALIZE_PARTITION_VALUES_ON_READ.key -> "true",
    ) {
      withTempDir { tempDir =>
        spark.createDataFrame(
          spark.sparkContext.emptyRDD[org.apache.spark.sql.Row],
          StructType(Seq(
            StructField("data", StringType),
            StructField("foo", IntegerType)
          ))
        ).write.format("delta").partitionBy("data").save(tempDir.getCanonicalPath)
        val deltaTxn = DeltaLog.forTable(spark, tempDir.getCanonicalPath).startTransaction()

        // Escaped characters like %aa should be preserved as-is in AddFile partition values
        // since they are not unescaped in AddFile partition values.
        val escapedValue = "test%aa%20value"
        val file = createAddFileWithPartitionValue(Map("data" -> escapedValue))
        val normalized = file.normalizedPartitionValues(spark, deltaTxn)

        assert(normalized("data") == Literal.create(escapedValue, StringType))
      }
    }
  }

  test("normalizedPartitionValues with a non UTC session time zone gets converted to UTC") {
    withSQLConf(
      DeltaSQLConf.DELTA_NORMALIZE_PARTITION_VALUES_ON_READ.key -> "true",
      "spark.sql.session.timeZone" -> "Europe/Berlin" // UTC + 1 in winter time
    ) {
      withTempDir { tempDir =>
        spark.createDataFrame(
          spark.sparkContext.emptyRDD[org.apache.spark.sql.Row],
          StructType(Seq(
            StructField("data", StringType),
            StructField("tsCol", TimestampType)
          ))
        ).write.format("delta").partitionBy("tsCol").save(tempDir.getCanonicalPath)
        val deltaTxn = DeltaLog.forTable(spark, tempDir.getCanonicalPath).startTransaction()

        val file = createAddFileWithPartitionValue(Map("tsCol" -> "2000-01-01 12:00:00"))
        // The normalized timestamp should be 11:00 UTC
        // Parsed in Europe/Berlin (UTC+1), so 12:00 Berlin = 11:00 UTC
        val normalizedTimestamp = file.normalizedPartitionValues(spark, deltaTxn)("tsCol")

        assert(normalizedTimestamp == timestampLiteral("2000-01-01 12:00:00", "Europe/Berlin"))
        assert(normalizedTimestamp == timestampLiteral("2000-01-01 11:00:00", "UTC"))
      }
    }
  }

  test("normalizedPartitionValues of timestamps strings with time zone offsets " +
    "and a non UTC session time zone gets converted to UTC.") {
    withSQLConf(
      DeltaSQLConf.DELTA_NORMALIZE_PARTITION_VALUES_ON_READ.key -> "true",
      "spark.sql.session.timeZone" -> "America/Los_Angeles" // UTC - 8 in winter time
    ) {
      withTempDir { tempDir =>
        spark.createDataFrame(
          spark.sparkContext.emptyRDD[org.apache.spark.sql.Row],
          StructType(Seq(
            StructField("data", StringType),
            StructField("tsCol", TimestampType)
          ))
        ).write.format("delta").partitionBy("tsCol").save(tempDir.getCanonicalPath)
        val deltaTxn = DeltaLog.forTable(spark, tempDir.getCanonicalPath).startTransaction()

        // Timestamp at 17:30 with a +05:30 offset (India Standard Time) is 12:00 UTC
        val fileWithIstOffset =
          createAddFileWithPartitionValue(Map("tsCol" -> "2000-01-01T17:30:00.000+0530"))
        // Timestamp at 13:00 in a Europe/Berlin time zone (UTC+1 in winter) is 12:00 UTC
        val fileWithCetOffset =
          createAddFileWithPartitionValue(Map("tsCol" -> "2000-01-01 13:00:00 Europe/Berlin"))

        val normalizedIstTimestamp =
          fileWithIstOffset.normalizedPartitionValues(spark, deltaTxn)("tsCol")
        val normalizedCetTimestamp =
          fileWithCetOffset.normalizedPartitionValues(spark, deltaTxn)("tsCol")
        // Both should represent 2000-01-01 12:00:00 UTC
        val expectedTimestamp = timestampLiteral("2000-01-01 12:00:00", "UTC")

        assert(normalizedIstTimestamp == expectedTimestamp)
        assert(normalizedCetTimestamp == expectedTimestamp)
      }
    }
  }

  test("normalizedPartitionValues with missing leading zeroes in timestamp are accepted") {
    withSQLConf(
      DeltaSQLConf.DELTA_NORMALIZE_PARTITION_VALUES_ON_READ.key -> "true",
      "spark.sql.session.timeZone" -> "UTC"
    ) {
      withTempDir { tempDir =>
        spark.createDataFrame(
          spark.sparkContext.emptyRDD[org.apache.spark.sql.Row],
          StructType(Seq(
            StructField("data", StringType),
            StructField("tsCol", TimestampType)
          ))
        ).write.format("delta").partitionBy("tsCol").save(tempDir.getCanonicalPath)
        val deltaTxn = DeltaLog.forTable(spark, tempDir.getCanonicalPath).startTransaction()

        def getNormalizedTimestamp(tsValue: String): Literal =
          createAddFileWithPartitionValue(Map("tsCol" -> tsValue))
            .normalizedPartitionValues(spark, deltaTxn)("tsCol")

        // Missing leading zero in hours: "1:00:00" vs "01:00:00"
        val hoursWithout = getNormalizedTimestamp("2000-01-01 1:00:00")
        val hoursWith = getNormalizedTimestamp("2000-01-01 01:00:00")
        val expectedHours = timestampLiteral("2000-01-01 01:00:00", "UTC")
        assert(hoursWithout == hoursWith)
        assert(hoursWith == expectedHours)

        // Missing leading zero in minutes: "01:2:00" vs "01:02:00"
        val minutesWithout = getNormalizedTimestamp("2000-01-01 01:2:00")
        val minutesWith = getNormalizedTimestamp("2000-01-01 01:02:00")
        val expectedMinutes = timestampLiteral("2000-01-01 01:02:00", "UTC")
        assert(minutesWithout == minutesWith)
        assert(minutesWith == expectedMinutes)

        // Missing leading zero in seconds: "01:02:3" vs "01:02:03"
        val secondsWithout = getNormalizedTimestamp("2000-01-01 01:02:3")
        val secondsWith = getNormalizedTimestamp("2000-01-01 01:02:03")
        val expectedSeconds = timestampLiteral("2000-01-01 01:02:03", "UTC")
        assert(secondsWithout == secondsWith)
        assert(secondsWith == expectedSeconds)

        // All missing leading zeroes: "1:2:3" vs "01:02:03"
        val allWithout = getNormalizedTimestamp("2000-01-01 1:2:3")
        val allWith = getNormalizedTimestamp("2000-01-01 01:02:03")
        val expectedAll = timestampLiteral("2000-01-01 01:02:03", "UTC")
        assert(allWithout == allWith)
        assert(allWith == expectedAll)
      }
    }
  }

  test("normalizedPartitionValues with ISO 8601 format with T separator but no time zone") {
    withSQLConf(
      DeltaSQLConf.DELTA_NORMALIZE_PARTITION_VALUES_ON_READ.key -> "true",
      "spark.sql.session.timeZone" -> "Europe/Berlin" // UTC + 1 in winter time
    ) {
      withTempDir { tempDir =>
        spark.createDataFrame(
          spark.sparkContext.emptyRDD[org.apache.spark.sql.Row],
          StructType(Seq(
            StructField("data", StringType),
            StructField("tsCol", TimestampType)
          ))
        ).write.format("delta").partitionBy("tsCol").save(tempDir.getCanonicalPath)
        val deltaTxn = DeltaLog.forTable(spark, tempDir.getCanonicalPath).startTransaction()

        // ISO 8601 format with 'T' separator but no time zone should use the session time zone
        val file = createAddFileWithPartitionValue(Map("tsCol" -> "2000-01-01T12:00:00"))
        // The normalized timestamp should be 11:00 UTC (12:00 Berlin = 11:00 UTC)
        val normalized = file.normalizedPartitionValues(spark, deltaTxn)
        assert(normalized("tsCol") == timestampLiteral("2000-01-01 12:00:00", "Europe/Berlin"))
      }
    }
  }
}
