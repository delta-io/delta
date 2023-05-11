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

package org.apache.spark.sql.delta.schema

import java.io.File

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaTestImplicits._

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryException}
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}

class CaseSensitivitySuite extends QueryTest
  with SharedSparkSession  with SQLTestUtils
  with DeltaSQLCommandTest {

  import testImplicits._

  private def testWithCaseSensitivity(name: String)(f: => Unit): Unit = {
    testQuietly(name) {
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        f
      }

      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
        f
      }
    }
  }

  private def getPartitionValues(allFiles: Dataset[AddFile], colName: String): Array[String] = {
    allFiles.select(col(s"partitionValues.$colName")).where(col(colName).isNotNull)
      .distinct().as[String].collect()
  }

  testWithCaseSensitivity("case sensitivity of partition fields") {
    withTempDir { tempDir =>
      val query = "SELECT id + 1 as Foo, id as Bar FROM RANGE(1)"
      sql(query).write.partitionBy("foo").format("delta").save(tempDir.getAbsolutePath)
      checkAnswer(
        sql(query),
        spark.read.format("delta").load(tempDir.getAbsolutePath)
      )

      val allFiles = DeltaLog.forTable(spark, tempDir.getAbsolutePath).snapshot.allFiles
      assert(getPartitionValues(allFiles, "Foo") === Array("1"))
      checkAnswer(
        spark.read.format("delta").load(tempDir.getAbsolutePath),
        Row(1L, 0L)
      )
    }
  }

  testQuietly("case sensitivity of partition fields (stream)") {
    // DataStreamWriter auto normalizes partition columns, therefore we don't need to check
    // case sensitive case
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      withTempDir { tempDir =>
        val memSource = MemoryStream[(Long, Long)]
        val stream1 = startStream(memSource.toDF().toDF("Foo", "Bar"), tempDir)
        try {
          memSource.addData((1L, 0L))
          stream1.processAllAvailable()
        } finally {
          stream1.stop()
        }

        checkAnswer(
          spark.read.format("delta").load(tempDir.getAbsolutePath),
          Row(1L, 0L)
        )

        val allFiles = DeltaLog.forTable(spark, tempDir.getAbsolutePath).snapshot.allFiles
        assert(getPartitionValues(allFiles, "Foo") === Array("1"))
      }
    }
  }

  testWithCaseSensitivity("two fields with same name") {
    withTempDir { tempDir =>
      intercept[AnalysisException] {
        val query = "SELECT id as Foo, id as foo FROM RANGE(1)"
        sql(query).write.partitionBy("foo").format("delta").save(tempDir.getAbsolutePath)
      }
    }
  }

  testWithCaseSensitivity("two fields with same name (stream)") {
    withTempDir { tempDir =>
      val memSource = MemoryStream[(Long, Long)]
      val stream1 = startStream(memSource.toDF().toDF("Foo", "foo"), tempDir)
      try {
        val e = intercept[StreamingQueryException] {
          memSource.addData((0L, 0L))
          stream1.processAllAvailable()
        }
        assert(e.cause.isInstanceOf[AnalysisException])
      } finally {
        stream1.stop()
      }
    }
  }

  testWithCaseSensitivity("schema merging is case insenstive but preserves original case") {
    withTempDir { tempDir =>
      val query1 = "SELECT id as foo, id as bar FROM RANGE(1)"
      sql(query1).write.format("delta").save(tempDir.getAbsolutePath)

      val query2 = "SELECT id + 1 as Foo, id as bar FROM RANGE(1)" // notice how 'F' is capitalized
      sql(query2).write.format("delta").mode("append").save(tempDir.getAbsolutePath)

      val query3 = "SELECT id as bAr, id + 2 as Foo FROM RANGE(1)" // changed order as well
      sql(query3).write.format("delta").mode("append").save(tempDir.getAbsolutePath)

      val df = spark.read.format("delta").load(tempDir.getAbsolutePath)
      checkAnswer(
        df,
        Row(0, 0) :: Row(1, 0) :: Row(2, 0) :: Nil
      )
      assert(df.schema.fieldNames === Seq("foo", "bar"))
    }
  }

  testWithCaseSensitivity("schema merging preserving column case (stream)") {
    withTempDir { tempDir =>
      val memSource = MemoryStream[(Long, Long)]
      val stream1 = startStream(memSource.toDF().toDF("Foo", "Bar"), tempDir, None)
      try {
        memSource.addData((0L, 0L))
        stream1.processAllAvailable()
      } finally {
        stream1.stop()
      }
      val stream2 = startStream(memSource.toDF().toDF("foo", "Bar"), tempDir, None)
      try {
        memSource.addData((1L, 2L))
        stream2.processAllAvailable()
      } finally {
        stream2.stop()
      }

      val df = spark.read.format("delta").load(tempDir.getAbsolutePath)
      checkAnswer(
        df,
        Row(0L, 0L) :: Row(1L, 2L) :: Nil
      )
      assert(df.schema.fieldNames === Seq("Foo", "Bar"))
    }
  }

  test("SC-12677: replaceWhere predicate should be case insensitive") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      Seq((1, "a"), (2, "b")).toDF("Key", "val").write
        .partitionBy("key").format("delta").mode("append").save(path)

      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
        Seq((2, "c")).toDF("Key", "val").write
          .format("delta")
          .mode("overwrite")
          .option("replaceWhere", "key = 2") // note the different case
          .save(path)
      }

      checkAnswer(
        spark.read.format("delta").load(path),
        Row(1, "a") :: Row(2, "c") :: Nil
      )

      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        val e = intercept[AnalysisException] {
          Seq((2, "d")).toDF("Key", "val").write
            .format("delta")
            .mode("overwrite")
            .option("replaceWhere", "key = 2") // note the different case
            .save(path)
        }
        assert(e.getErrorClass == "UNRESOLVED_COLUMN.WITHOUT_SUGGESTION"
          || e.getErrorClass == "MISSING_COLUMN"
          || e.getErrorClass == "UNRESOLVED_COLUMN.WITH_SUGGESTION")
      }

      checkAnswer(
        spark.read.format("delta").load(path),
        Row(1, "a") :: Row(2, "c") :: Nil
      )
    }
  }

  private def startStream(
      df: Dataset[_],
      tempDir: File,
      partitionBy: Option[String] = Some("foo")): StreamingQuery = {
    val writer = df.writeStream
      .option("checkpointLocation", new File(tempDir, "_checkpoint").getAbsolutePath)
      .format("delta")
    partitionBy.foreach(writer.partitionBy(_))
    writer.start(tempDir.getAbsolutePath)
  }
}
