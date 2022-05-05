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

package org.apache.spark.sql.delta.cdc

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.{Action, AddCDCFile}
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.commands.cdc.CDCReader._
import org.apache.spark.sql.delta.files.DelayedCommitProtocol
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.datasources.FileFormatWriter
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession

class CDCReaderSuite
  extends QueryTest  with CheckCDCAnswer
  with SharedSparkSession
  with DeltaSQLCommandTest
  with DeltaColumnMappingTestUtils {

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey, "true")

  /**
   * Write a commit with just CDC data. Returns the committed version.
   */
  private def writeCdcData(
      log: DeltaLog,
      data: DataFrame,
      extraActions: Seq[Action] = Seq.empty): Long = {
    log.withNewTransaction { txn =>
      val qe = data.queryExecution
      val basePath = log.dataPath.toString

      // column mapped mode forces to use random file prefix
      val randomPrefixes = if (columnMappingEnabled) {
        Some(DeltaConfigs.RANDOM_PREFIX_LENGTH.fromMetaData(log.snapshot.metadata))
      } else {
        None
      }
      // we need to convert to physical name in column mapping mode
      val mappedOutput = if (columnMappingEnabled) {
        val metadata = log.snapshot.metadata
        DeltaColumnMapping.createPhysicalAttributes(
          qe.analyzed.output, metadata.schema, metadata.columnMappingMode
        )
      } else {
        qe.analyzed.output
      }

      SQLExecution.withNewExecutionId(qe) {
        var committer = new DelayedCommitProtocol("delta", basePath, randomPrefixes)
        FileFormatWriter.write(
          sparkSession = spark,
          plan = qe.executedPlan,
          fileFormat = log.fileFormat(),
          committer = committer,
          outputSpec = FileFormatWriter.OutputSpec(basePath, Map.empty, mappedOutput),
          hadoopConf = log.newDeltaHadoopConf(),
          partitionColumns = Seq.empty,
          bucketSpec = None,
          statsTrackers = Seq.empty,
          options = Map.empty)

        val cdc = committer.addedStatuses.map { a =>
          AddCDCFile(a.path, Map.empty, a.size)
        }
        txn.commit(extraActions ++ cdc, DeltaOperations.ManualUpdate)
      }
    }
  }

  test("simple CDC scan") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(spark, dir.getAbsolutePath)
      val data = spark.range(10)
      val cdcData = spark.range(20, 25).withColumn(CDC_TYPE_COLUMN_NAME, lit("insert"))

      data.write.format("delta").save(dir.getAbsolutePath)
      sql(s"DELETE FROM delta.`${dir.getAbsolutePath}`")
      writeCdcData(log, cdcData)

      // For this basic test, we check each of the versions individually in addition to the full
      // range to try and catch weird corner cases.
      checkCDCAnswer(
        log,
        CDCReader.changesToBatchDF(log, 0, 0, spark),
        data.withColumn(CDC_TYPE_COLUMN_NAME, lit("insert"))
          .withColumn(CDC_COMMIT_VERSION, lit(0))
      )
      checkCDCAnswer(
        log,
        CDCReader.changesToBatchDF(log, 1, 1, spark),
        data.withColumn(CDC_TYPE_COLUMN_NAME, lit("delete"))
          .withColumn(CDC_COMMIT_VERSION, lit(1))
      )
      checkCDCAnswer(
        log,
        CDCReader.changesToBatchDF(log, 2, 2, spark),
        cdcData.withColumn(CDC_COMMIT_VERSION, lit(2))
      )
      checkCDCAnswer(
        log,
        CDCReader.changesToBatchDF(log, 0, 2, spark),
        data.withColumn(CDC_TYPE_COLUMN_NAME, lit("insert"))
          .withColumn(CDC_COMMIT_VERSION, lit(0))
          .unionAll(data
            .withColumn(CDC_TYPE_COLUMN_NAME, lit("delete"))
            .withColumn(CDC_COMMIT_VERSION, lit(1)))
          .unionAll(cdcData.withColumn(CDC_COMMIT_VERSION, lit(2)))
      )
    }
  }

  test("cdc update ops") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(spark, dir.getAbsolutePath)
      val data = spark.range(10)

      data.write.format("delta").save(dir.getAbsolutePath)
      writeCdcData(
        log,
        spark.range(20, 25).toDF().withColumn(CDC_TYPE_COLUMN_NAME, lit("update_pre")))
      writeCdcData(
        log,
        spark.range(30, 35).toDF().withColumn(CDC_TYPE_COLUMN_NAME, lit("update_post")))

      checkCDCAnswer(
        log,
        CDCReader.changesToBatchDF(log, 0, 2, spark),
        data.withColumn(CDC_TYPE_COLUMN_NAME, lit("insert"))
          .withColumn(CDC_COMMIT_VERSION, lit(0))
          .unionAll(spark.range(20, 25).withColumn(CDC_TYPE_COLUMN_NAME, lit("update_pre"))
              .withColumn(CDC_COMMIT_VERSION, lit(1))
          )
          .unionAll(spark.range(30, 35).withColumn(CDC_TYPE_COLUMN_NAME, lit("update_post"))
              .withColumn(CDC_COMMIT_VERSION, lit(2))
          )
      )
    }
  }

  test("dataChange = false operations ignored") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(spark, dir.getAbsolutePath)
      val data = spark.range(10)

      data.write.format("delta").save(dir.getAbsolutePath)
      sql(s"OPTIMIZE delta.`${dir.getAbsolutePath}`")

      checkCDCAnswer(
        log,
        CDCReader.changesToBatchDF(log, 0, 1, spark),
        data.withColumn(CDC_TYPE_COLUMN_NAME, lit("insert"))
          .withColumn(CDC_COMMIT_VERSION, lit(0))
      )
    }
  }

  test("range with start and end equal") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(spark, dir.getAbsolutePath)
      val data = spark.range(10)
      val cdcData = spark.range(0, 5).withColumn(CDC_TYPE_COLUMN_NAME, lit("delete"))
          .withColumn(CDC_COMMIT_VERSION, lit(1))

      data.write.format("delta").save(dir.getAbsolutePath)
      writeCdcData(log, cdcData)

      checkCDCAnswer(
        log,
        CDCReader.changesToBatchDF(log, 0, 0, spark),
        data.withColumn(CDC_TYPE_COLUMN_NAME, lit("insert"))
          .withColumn(CDC_COMMIT_VERSION, lit(0))
      )

      checkCDCAnswer(
        log,
        CDCReader.changesToBatchDF(log, 1, 1, spark),
        cdcData)
    }
  }

  test("range past the end of the log") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(spark, dir.getAbsolutePath)
      spark.range(10).write.format("delta").save(dir.getAbsolutePath)

      checkCDCAnswer(
        log,
        CDCReader.changesToBatchDF(log, 0, 1, spark),
        spark.range(10).withColumn(CDC_TYPE_COLUMN_NAME, lit("insert"))
          .withColumn(CDC_COMMIT_VERSION, lit(0))
      )
    }
  }

  test("invalid range - end before start") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(spark, dir.getAbsolutePath)
      spark.range(10).write.format("delta").save(dir.getAbsolutePath)
      spark.range(20).write.format("delta").mode("append").save(dir.getAbsolutePath)

      intercept[IllegalArgumentException] {
        CDCReader.changesToBatchDF(log, 1, 0, spark)
      }
    }
  }

}

