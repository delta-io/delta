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

import java.io.PrintWriter

import org.apache.spark.sql.delta.GeneratedAsIdentityType.{GeneratedAlways, GeneratedByDefault}

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.types._

/**
 * Identity Column test suite for ingestion, including insert-only MERGE.
 * Tests with identity columns where MERGE does data modification should be
 * in IdentityColumnDMLSuiteBase.
 */
trait IdentityColumnIngestionSuiteBase extends IdentityColumnTestUtils {

  import testImplicits._

  private val tempCsvFileName = "test.csv"

  /** Helper function to write a single 'value' column into `sourcePath`. */
  private def setupSimpleCsvFiles(sourcePath: String, start: Int, end: Int): Unit = {
    val writer = new PrintWriter(s"$sourcePath/$tempCsvFileName")
    // Write header.
    writer.write("value\n")
    // Write values.
    (start to end).foreach { v =>
      writer.write(s"$v\n")
    }
    writer.close()
  }

  object IngestMode extends Enumeration {
    // Ingest using data frame append v1.
    val appendV1 = Value

    // Ingest using data frame append v2.
    val appendV2 = Value

    // Ingest using "INSERT INTO ... VALUES".
    val insertIntoValues = Value

    // Ingest using "INSERT INTO ... SELECT ...".
    val insertIntoSelect = Value

    // Ingest using "INSERT OVERWRITE ... VALUES".
    val insertOverwriteValues = Value

    // Ingest using "INSERT OVERWRITE ... SELECT ...".
    val insertOverwriteSelect = Value


    // Ingest using streaming query.
    val streaming = Value

    // Ingest using MERGE INTO ... WHEN NOT MATCHED INSERT
    val mergeInsert = Value
  }

  case class IngestTestCase(start: Long, step: Long, iteration: Int, batchSize: Int)

  /**
   * Helper function to test ingesting data to delta table with IDENTITY columns.
   *
   * @param start     IDENTITY start configuration.
   * @param step      IDENTITY step configuration.
   * @param iteration How many batch to ingest.
   * @param batchSize How many rows to ingest in each batch.
   * @param mode      Specifies what command to use to ingest data.
   */
  private def testIngestData(
      start: Long,
      step: Long,
      iteration: Int,
      batchSize: Int,
      mode: IngestMode.Value): Unit = {
    var highWaterMark = start - step
    val tblName = getRandomTableName
    withTable(tblName) {
      createTableWithIdColAndIntValueCol(
        tblName, GeneratedAlways, startsWith = Some(start), incrementBy = Some(step))
      val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tblName))
      for (iter <- 0 to iteration - 1) {
        val batchStart = iter * batchSize + 1
        val batchEnd = (iter + 1) * batchSize

        // Used by data frame append v1 and append v2.
        val df = (batchStart to batchEnd).toDF("value")
        // Used by insertInto, insertIntoSelect, insertOverwrite, insertOverwriteSelect
        val insertValues = (batchStart to batchEnd).map(v => s"($v)").mkString(",")
        val tempTblName = s"${getRandomTableName}_temp"

        mode match {
          case IngestMode.appendV1 =>
            df.write.format("delta").mode("append").save(deltaLog.dataPath.toString)

          case IngestMode.appendV2 =>
            df.writeTo(tblName).append()

          case IngestMode.insertIntoValues =>
            val insertStmt = s"INSERT INTO $tblName(value) VALUES $insertValues;"
            sql(insertStmt)

          case IngestMode.insertIntoSelect =>
            withTable(tempTblName) {
              // Insert values into a separate table, then select into the destination table.
              createTable(
                tempTblName, Seq(TestColumnSpec(colName = "value", dataType = IntegerType)))
              sql(s"INSERT INTO $tempTblName VALUES $insertValues")
              sql(s"INSERT INTO $tblName(value) SELECT value FROM $tempTblName")
            }

          case IngestMode.insertOverwriteSelect =>
            withTable(tempTblName) {
              // Insert values into a separate table, then select into the destination table.
              createTable(
                tempTblName, Seq(TestColumnSpec(colName = "value", dataType = IntegerType)))
              sql(s"INSERT INTO $tempTblName VALUES $insertValues")
              sql(s"INSERT OVERWRITE $tblName(value) SELECT value FROM $tempTblName")
            }

          case IngestMode.insertOverwriteValues =>
            val insertStmt = s"INSERT OVERWRITE $tblName(value) VALUES $insertValues"
            sql(insertStmt)

          case IngestMode.streaming =>
            withTempDir { checkpointDir =>
              val stream = MemoryStream[Int]
              val q = stream
                .toDF
                .toDF("value")
                .writeStream
                .format("delta")
                .outputMode("append")
                .option("checkpointLocation", checkpointDir.getCanonicalPath)
                .start(deltaLog.dataPath.toString)
              stream.addData(batchStart to batchEnd)
              q.processAllAvailable()
              q.stop()
            }

          case IngestMode.mergeInsert =>
            withTable(tempTblName) {
              // Insert values into a separate table, then merge into the destination table.
              createTable(
                tempTblName, Seq(TestColumnSpec(colName = "value", dataType = IntegerType)))
              sql(s"INSERT INTO $tempTblName VALUES $insertValues")
              sql(
                s"""
                   |MERGE INTO $tblName
                   |  USING $tempTblName ON $tblName.value = $tempTblName.value
                   |  WHEN NOT MATCHED THEN INSERT (value) VALUES ($tempTblName.value)
                   |""".stripMargin)
            }

          case _ => assert(false, "Unrecognized ingestion mode")
        }

        val expectedRowCount = mode match {
          case _@(IngestMode.insertOverwriteValues | IngestMode.insertOverwriteSelect) =>
            // These modes keep the row count unchanged.
            batchSize
          case _ => batchSize * (iter + 1)
        }

        highWaterMark = validateIdentity(tblName, expectedRowCount, start, step,
          batchStart, batchEnd, highWaterMark)
      }
    }
  }

  test("append v1") {
    val testCases = Seq(
      IngestTestCase(1, 1, 4, 250),
      IngestTestCase(1, -3, 10, 23)
    )
    for (tc <- testCases) {
      testIngestData(tc.start, tc.step, tc.iteration, tc.batchSize, IngestMode.appendV1)
    }
  }

  test("append v2") {
    val testCases = Seq(
      IngestTestCase(100, 100, 3, 300),
      IngestTestCase(Integer.MAX_VALUE.toLong + 1, -1000, 10, 23)
    )
    for (tc <- testCases) {
      testIngestData(tc.start, tc.step, tc.iteration, tc.batchSize, IngestMode.appendV2)
    }
  }

  test("insert into values") {
    val testCases = Seq(
      IngestTestCase(100, -100, 4, 201),
      IngestTestCase(Integer.MAX_VALUE.toLong + 1, 1000, 10, 37)
    )
    for (tc <- testCases) {
      testIngestData(tc.start, tc.step, tc.iteration, tc.batchSize, IngestMode.insertIntoValues)
    }
  }

  test("insert into select") {
    val testCases = Seq(
      IngestTestCase(23, 102, 3, 77),
      IngestTestCase(Integer.MAX_VALUE.toLong - 12345, 99, 8, 25)
    )
    for (tc <- testCases) {
      testIngestData(tc.start, tc.step, tc.iteration, tc.batchSize, IngestMode.insertIntoSelect)
    }
  }

  test("insert overwrite values") {
    val testCases = Seq(
      IngestTestCase(-10, 3, 5, 30),
      IngestTestCase(Integer.MIN_VALUE.toLong - 1000, -18, 2, 100)
    )
    for (tc <- testCases) {
      testIngestData(tc.start, tc.step, tc.iteration, tc.batchSize,
        IngestMode.insertOverwriteValues)
    }
  }

  test("insert overwrite select") {
    val testCases = Seq(
      IngestTestCase(-15, 20, 4, 35),
      IngestTestCase(200, 50, 3, 7)
    )
    for (tc <- testCases) {
      testIngestData(tc.start, tc.step, tc.iteration, tc.batchSize,
        IngestMode.insertOverwriteSelect)
    }
  }

  test("streaming") {
    val testCases = Seq(
      IngestTestCase(-2000, 19, 5, 20),
      IngestTestCase(10, 10, 4, 17)
    )
    for (tc <- testCases) {
      testIngestData(tc.start, tc.step, tc.iteration, tc.batchSize, IngestMode.streaming)
    }
  }

  test("merge insert") {
    val testCases = Seq(
      IngestTestCase(10, 20, 5, 8),
      IngestTestCase(-5000, 37, 7, 99)
    )
    for (tc <- testCases) {
      testIngestData(tc.start, tc.step, tc.iteration, tc.batchSize, IngestMode.mergeInsert)
    }
  }

  test("explicit insert not allowed") {
    val tblName = getRandomTableName
    withIdentityColumnTable(GeneratedAlways, tblName) {
      val ex = intercept[AnalysisException](sql(s"INSERT INTO $tblName values(1,1);"))
      assert(ex.getMessage.contains("Providing values for GENERATED ALWAYS AS IDENTITY"))
    }
  }

  test("explicit insert should not update high water mark") {
    val tblName = getRandomTableName
    withIdentityColumnTable(GeneratedByDefault, tblName) {
      val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tblName))
      val schema1 = deltaLog.update().metadata.schemaString

      // System generated IDENTITY value - should update schema.
      sql(s"INSERT INTO $tblName(value) VALUES (1);")
      val snapshot2 = deltaLog.update()
      val highWatermarkAfterGeneration = getHighWaterMark(snapshot2, "id")
      assert(highWatermarkAfterGeneration.isDefined)
      val schema2 = snapshot2.metadata.schemaString
      assert(schema1 != schema2)

      // Explicitly provided IDENTITY value - should not update schema.
      sql(s"INSERT INTO $tblName VALUES (1,1);")
      val snapshot3 = deltaLog.update()
      val schema3 = snapshot3.metadata.schemaString
      val highWatermarkAfterUserInsert = getHighWaterMark(snapshot3, "id")
      assert(highWatermarkAfterUserInsert == highWatermarkAfterGeneration)
      assert(schema2 == schema3)
    }
  }

  test("merge command with nondeterministic functions in conditions") {
    val source = "identity_merge_source"
    val target = "identity_merge_target"
    withIdentityColumnTable(GeneratedByDefault, target) {
      withTable(source) {
        createTable(
          source,
          Seq(
            TestColumnSpec(colName = "id2", dataType = LongType),
            TestColumnSpec(colName = "value2", dataType = LongType)
          )
        )

        val ex1 = intercept[AnalysisException] {
          sql(
            s"""
               |MERGE INTO $target
               |  USING $source ON $target.value = $source.value2 + rand()
               |  WHEN NOT MATCHED THEN INSERT (value) VALUES ($source.value2)
               |""".stripMargin)
        }
        assert(ex1.getMessage.contains("Non-deterministic functions are not supported"))
        val ex2 = intercept[AnalysisException] {
          sql(
            s"""
               |MERGE INTO $target
               |  USING $source ON $target.value = $source.value2
               |  WHEN NOT MATCHED AND $source.value2 = rand()
               |    THEN INSERT (value) VALUES ($source.value2)
               |""".stripMargin)
        }
        assert(ex2.getMessage.contains("Non-deterministic functions are not supported"))
      }
    }
  }
}

class IdentityColumnIngestionScalaSuite
  extends IdentityColumnIngestionSuiteBase
  with ScalaDDLTestUtils

class IdentityColumnIngestionScalaIdColumnMappingSuite
  extends IdentityColumnIngestionSuiteBase
  with ScalaDDLTestUtils
  with DeltaColumnMappingEnableIdMode

class IdentityColumnIngestionScalaNameColumnMappingSuite
  extends IdentityColumnIngestionSuiteBase
  with ScalaDDLTestUtils
  with DeltaColumnMappingEnableNameMode
