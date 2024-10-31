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

import java.util.Locale

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.DeltaOptions.{OVERWRITE_SCHEMA_OPTION, PARTITION_OVERWRITE_MODE_OPTION}
import org.apache.spark.sql.delta.actions.{Action, FileAction}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.FileNames
import org.apache.commons.io.FileUtils
import org.apache.parquet.format.CompressionCodec

import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.internal.SQLConf.PARTITION_OVERWRITE_MODE
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

class DeltaOptionSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

  import testImplicits._



  test("support for setting dataChange to false") {
    val tempDir = Utils.createTempDir()

    spark.range(100)
      .write
      .format("delta")
      .save(tempDir.toString)

    val df = spark.read.format("delta").load(tempDir.toString)

    df
      .write
      .format("delta")
      .mode("overwrite")
      .option("dataChange", "false")
      .save(tempDir.toString)

    val deltaLog = DeltaLog.forTable(spark, tempDir)
    val version = deltaLog.snapshot.version
    val commitActions = deltaLog.store
      .read(FileNames.unsafeDeltaFile(deltaLog.logPath, version), deltaLog.newDeltaHadoopConf())
      .map(Action.fromJson)
    val fileActions = commitActions.collect { case a: FileAction => a }

    assert(fileActions.forall(!_.dataChange))
  }

  test("dataChange is by default set to true") {
    val tempDir = Utils.createTempDir()

    spark.range(100)
      .write
      .format("delta")
      .save(tempDir.toString)

    val df = spark.read.format("delta").load(tempDir.toString)

    df
      .write
      .format("delta")
      .mode("overwrite")
      .save(tempDir.toString)

    val deltaLog = DeltaLog.forTable(spark, tempDir)
    val version = deltaLog.snapshot.version
    val commitActions = deltaLog.store
      .read(FileNames.unsafeDeltaFile(deltaLog.logPath, version), deltaLog.newDeltaHadoopConf())
      .map(Action.fromJson)
    val fileActions = commitActions.collect { case a: FileAction => a }

    assert(fileActions.forall(_.dataChange))
  }

  test("dataChange is set to false on metadata changing operation") {
    withTempDir { tempDir =>
      // Initialize a table while having dataChange set to false.
      val e = intercept[AnalysisException] {
        spark.range(100)
          .write
          .format("delta")
          .option("dataChange", "false")
          .save(tempDir.getAbsolutePath)
      }
      assert(e.getMessage ===
        DeltaErrors.unexpectedDataChangeException("Create a Delta table").getMessage)
      spark.range(100)
        .write
        .format("delta")
        .save(tempDir.getAbsolutePath)

      // Adding a new column to the existing table while having dataChange set to false.
      val e2 = intercept[AnalysisException] {
        val df = spark.read.format("delta").load(tempDir.getAbsolutePath)
        df.withColumn("id2", 'id + 1)
          .write
          .format("delta")
          .mode("overwrite")
          .option("mergeSchema", "true")
          .option("dataChange", "false")
          .save(tempDir.getAbsolutePath)
      }
      assert(e2.getMessage ===
        DeltaErrors.unexpectedDataChangeException("Change the Delta table schema").getMessage)

      // Overwriting the schema of the existing table while having dataChange as false.
      val e3 = intercept[AnalysisException] {
        spark.range(50)
          .withColumn("id3", 'id + 1)
          .write
          .format("delta")
          .mode("overwrite")
          .option("dataChange", "false")
          .option("overwriteSchema", "true")
          .save(tempDir.getAbsolutePath)
      }
      assert(e3.getMessage ===
        DeltaErrors.unexpectedDataChangeException("Overwrite the Delta table schema or " +
          "change the partition schema").getMessage)
    }
  }


  test("support the maxRecordsPerFile write option: path") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      withTable("maxRecordsPerFile") {
        spark.range(100)
          .write
          .format("delta")
          .option("maxRecordsPerFile", 5)
          .save(path)
        assert(FileUtils.listFiles(tempDir, Array("parquet"), false).size === 20)
      }
    }
  }

  test("support the maxRecordsPerFile write option: external table") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      withTable("maxRecordsPerFile") {
        spark.range(100)
          .write
          .format("delta")
          .option("maxRecordsPerFile", 5)
          .option("path", path)
          .saveAsTable("maxRecordsPerFile")
        assert(FileUtils.listFiles(tempDir, Array("parquet"), false).size === 20)
      }
    }
  }

  test("support the maxRecordsPerFile write option: v2 write") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      withTable("maxRecordsPerFile") {
        spark.range(100)
          .writeTo("maxRecordsPerFile")
          .using("delta")
          .option("maxRecordsPerFile", 5)
          .tableProperty("location", path)
          .create()
        assert(FileUtils.listFiles(tempDir, Array("parquet"), false).size === 20)
      }
    }
  }

  test("support no compression write option (defaults to snappy)") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      withTable("compression") {
        spark.range(100)
          .writeTo("compression")
          .using("delta")
          .tableProperty("location", path)
          .create()
        assert(FileUtils.listFiles(tempDir, Array("snappy.parquet"), false).size > 0)
      }
    }
  }

  // LZO and BROTLI left out as additional library dependencies needed
  val codecsAndSubExtensions = Seq(
    CompressionCodec.UNCOMPRESSED -> "",
    CompressionCodec.SNAPPY -> "snappy.",
    CompressionCodec.GZIP -> "gz.",
    CompressionCodec.LZ4 -> "lz4hadoop.",
    // CompressionCodec.LZ4_RAW -> "lz4raw.", // Support is not yet available in Spark 3.5
    CompressionCodec.ZSTD -> "zstd."
  )

  codecsAndSubExtensions.foreach { case (codec, subExt) =>
    val codecName = codec.name().toLowerCase(Locale.ROOT)
    test(s"support compression codec '$codecName' as write option") {
      withTempDir { tempDir =>
        val path = tempDir.getCanonicalPath
        withTable(s"compression_$codecName") {
          spark.range(100)
            .writeTo(s"compression_$codecName")
            .using("delta")
            .option("compression", codecName)
            .tableProperty("location", path)
            .create()
          assert(FileUtils.listFiles(tempDir, Array(s"${subExt}parquet"), false).size > 0)
        }
      }
    }
  }

  test("invalid compression write option") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      withTable("compression") {
        assert(
          intercept[java.lang.IllegalArgumentException] {
            spark.range(100)
              .writeTo("compression")
              .using("delta")
              .option("compression", "???")
              .tableProperty("location", path)
              .create()
          }.getMessage.nonEmpty)
      }
    }
  }

  test("DeltaSQLConf.DYNAMIC_PARTITION_OVERWRITE_ENABLED = true: " +
    "partitionOverwriteMode is set to invalid value in options") {
    withSQLConf(DeltaSQLConf.DYNAMIC_PARTITION_OVERWRITE_ENABLED.key -> "true") {
      withTempDir { tempDir =>
        val invalidMode = "ADAPTIVE"
        val e = intercept[IllegalArgumentException] {
          Seq(1, 2, 3).toDF
            .withColumn("part", $"value" % 2)
            .write
            .format("delta")
            .partitionBy("part")
            .option("partitionOverwriteMode", invalidMode)
            .save(tempDir.getAbsolutePath)
        }
        assert(e.getMessage ===
          DeltaErrors.illegalDeltaOptionException(
            PARTITION_OVERWRITE_MODE_OPTION, invalidMode, "must be 'STATIC' or 'DYNAMIC'"
          ).getMessage
        )
      }
    }
  }

  test("DeltaSQLConf.DYNAMIC_PARTITION_OVERWRITE_ENABLED = false: " +
    "partitionOverwriteMode is set to invalid value in options") {
    // partitionOverwriteMode is ignored and no error is thrown
    withSQLConf(DeltaSQLConf.DYNAMIC_PARTITION_OVERWRITE_ENABLED.key -> "false") {
      withTempDir { tempDir =>
        val invalidMode = "ADAPTIVE"
        Seq(1, 2, 3).toDF
          .withColumn("part", $"value" % 2)
          .write
          .format("delta")
          .partitionBy("part")
          .option("partitionOverwriteMode", invalidMode)
          .save(tempDir.getAbsolutePath)
      }
    }
  }

  test("overwriteSchema=true should be invalid with partitionOverwriteMode=dynamic") {
    withTempDir { tempDir =>
      val e = intercept[DeltaIllegalArgumentException] {
        withSQLConf(DeltaSQLConf.DYNAMIC_PARTITION_OVERWRITE_ENABLED.key -> "true") {
          Seq(1, 2, 3).toDF
            .withColumn("part", $"value" % 2)
            .write
            .mode("overwrite")
            .format("delta")
            .partitionBy("part")
            .option(OVERWRITE_SCHEMA_OPTION, "true")
            .option(PARTITION_OVERWRITE_MODE_OPTION, "dynamic")
            .save(tempDir.getAbsolutePath)
        }
      }
      assert(e.getCondition == "DELTA_OVERWRITE_SCHEMA_WITH_DYNAMIC_PARTITION_OVERWRITE")
    }
  }

  test("overwriteSchema=true should be invalid with partitionOverwriteMode=dynamic, " +
      "saveAsTable") {
    withTable("temp") {
      val e = intercept[DeltaIllegalArgumentException] {
        withSQLConf(DeltaSQLConf.DYNAMIC_PARTITION_OVERWRITE_ENABLED.key -> "true") {
          Seq(1, 2, 3).toDF
            .withColumn("part", $"value" % 2)
            .write
            .mode("overwrite")
            .format("delta")
            .partitionBy("part")
            .option(OVERWRITE_SCHEMA_OPTION, "true")
            .option(PARTITION_OVERWRITE_MODE_OPTION, "dynamic")
            .saveAsTable("temp")
        }
      }
      assert(e.getCondition == "DELTA_OVERWRITE_SCHEMA_WITH_DYNAMIC_PARTITION_OVERWRITE")
    }
  }

  test("Prohibit spark.databricks.delta.dynamicPartitionOverwrite.enabled=false in " +
    "dynamic partition overwrite mode") {
    withTempDir { tempDir =>
      withSQLConf(DeltaSQLConf.DYNAMIC_PARTITION_OVERWRITE_ENABLED.key -> "false") {
        var e = intercept[DeltaIllegalArgumentException] {
          Seq(1, 2, 3).toDF
            .withColumn("part", $"value" % 2)
            .write
            .format("delta")
            .partitionBy("part")
            .option("partitionOverwriteMode", "dynamic")
            .save(tempDir.getAbsolutePath)
        }
        assert(e.getCondition == "DELTA_DYNAMIC_PARTITION_OVERWRITE_DISABLED")
        withSQLConf(PARTITION_OVERWRITE_MODE.key -> "dynamic") {
          e = intercept[DeltaIllegalArgumentException] {
            Seq(1, 2, 3).toDF
              .withColumn("part", $"value" % 2)
              .write
              .format("delta")
              .partitionBy("part")
              .save(tempDir.getAbsolutePath)
          }
        }
        assert(e.getCondition == "DELTA_DYNAMIC_PARTITION_OVERWRITE_DISABLED")
      }
    }
  }

  for (createOrReplace <- Seq("CREATE OR REPLACE", "REPLACE")) {
    test(s"$createOrReplace table command should not respect " +
      "dynamic partition overwrite mode") {
      withTempDir { tempDir =>
        Seq(0, 1).toDF
          .withColumn("key", $"value" % 2)
          .withColumn("stringColumn", lit("string"))
          .withColumn("part", $"value" % 2)
          .write
          .format("delta")
          .partitionBy("part")
          .save(tempDir.getAbsolutePath)
        withSQLConf(PARTITION_OVERWRITE_MODE.key -> "dynamic") {
          // Write only to one partition with a different schema type of stringColumn.
          sql(
            s"""
               |$createOrReplace TABLE delta.`${tempDir.getAbsolutePath}`
               |USING delta
               |PARTITIONED BY (part)
               |LOCATION '${tempDir.getAbsolutePath}'
               |AS SELECT -1 as value, 0 as part, 0 as stringColumn
               |""".stripMargin)
          assert(spark.read.format("delta").load(tempDir.getAbsolutePath).count() == 1,
            "Table should be fully replaced even with DPO mode enabled")
        }
      }
    }
  }

  // Same test as above but using DeltaWriter V2.
  test("create or replace table V2 should not respect dynamic partition overwrite mode") {
    withTable("temp") {
      Seq(0, 1).toDF
        .withColumn("part", $"value" % 2)
        .write
        .format("delta")
        .partitionBy("part")
        .saveAsTable("temp")
      withSQLConf(PARTITION_OVERWRITE_MODE.key -> "dynamic") {
        // Write to one partition only.
        Seq(0).toDF
          .withColumn("part", $"value" % 2)
          .writeTo("temp")
          .using("delta")
          .createOrReplace()
        assert(spark.read.format("delta").table("temp").count() == 1,
          "Table should be fully replaced even with DPO mode enabled")
      }
    }
  }

  // Same test as above but using saveAsTable.
  test("saveAsTable with overwrite should respect dynamic partition overwrite mode") {
    withTable("temp") {
      Seq(0, 1).toDF
        .withColumn("part", $"value" % 2)
        .write
        .format("delta")
        .partitionBy("part")
        .saveAsTable("temp")
      // Write to one partition only.
      Seq(0).toDF
        .withColumn("part", $"value" % 2)
        .write
        .mode("overwrite")
        .option(PARTITION_OVERWRITE_MODE_OPTION, "dynamic")
        .partitionBy("part")
        .format("delta")
        .saveAsTable("temp")
      assert(spark.read.format("delta").table("temp").count() == 2,
        "Table should keep the original partition with DPO mode enabled.")
    }
  }
}
