/*
 * Copyright (2020) The Delta Lake Project Authors.
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

import java.io.{File, FileInputStream, OutputStream}
import java.net.URI
import java.util.UUID

import scala.concurrent.duration._
import scala.language.implicitConversions

import org.apache.spark.sql.delta.actions.{AddFile, InvalidProtocolVersionException, Protocol}
import org.apache.spark.sql.delta.sources.{DeltaSourceOffset, DeltaSQLConf}
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{FileStatus, Path, RawLocalFileSystem}

import org.apache.spark.sql.{AnalysisException, Dataset, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.SessionCatalog.DEFAULT_DATABASE
import org.apache.spark.sql.catalyst.util.IntervalUtils
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.sources.{StreamSinkProvider, StreamSourceProvider}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, StreamingQueryException, StreamTest}
import org.apache.spark.sql.streaming.Trigger._
import org.apache.spark.sql.streaming.util.StreamManualClock
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.{ManualClock, Utils}

class DeltaSourceTableAPISuite extends StreamTest with DeltaSQLCommandTest {

  import testImplicits._
  test("table API") {
    withTempDir { tempDir =>
      val tblName = "my_table"
      val dir = tempDir.getAbsolutePath
      withTable(tblName) {
        spark.range(3).write.format("delta").option("path", dir).saveAsTable(tblName)

        testStream(spark.readStream.table(tblName))(
          ProcessAllAvailable(),
          CheckAnswer(0, 1, 2)
        )
      }
    }
  }

  test("table API with database") {
    withTempDir { tempDir =>
      val tblName = "my_table"
      val dir = tempDir.getAbsolutePath
      withTempDatabase { db =>
        withTable(tblName) {
          spark.sql(s"USE $db")
          spark.range(3).write.format("delta").option("path", dir).saveAsTable(tblName)
          spark.sql(s"USE $DEFAULT_DATABASE")

          testStream(spark.readStream.table(s"$db.$tblName"))(
            ProcessAllAvailable(),
            CheckAnswer(0, 1, 2)
          )
        }
      }
    }
  }

  private def startTableStream(
      ds: Dataset[_],
      tableName: String,
      baseDir: Option[File] = None,
      partitionColumns: Seq[String] = Nil,
      format: String = "delta"): StreamingQuery = {
    val checkpoint = baseDir.map(new File(_, "_checkpoint"))
      .getOrElse(Utils.createTempDir().getCanonicalFile)
    val dsw = ds.writeStream.format(format).partitionBy(partitionColumns: _*)
    baseDir.foreach { output =>
      dsw.option("path", output.getCanonicalPath)
    }
    dsw.option("checkpointLocation", checkpoint.getCanonicalPath).toTable(tableName)
  }

  test("writeStream.table - create new external table") {
    withTempDir { dir =>
      val memory = MemoryStream[Int]
      val tableName = "stream_test"
      withTable(tableName) {
        val sq = startTableStream(memory.toDS(), tableName, Some(dir))
        memory.addData(1, 2, 3)
        sq.processAllAvailable()

        checkDatasetUnorderly(
          spark.table(tableName).as[Int],
          1, 2, 3)

        checkDatasetUnorderly(
          spark.read.format("delta").load(dir.getCanonicalPath).as[Int],
          1, 2, 3)
      }
    }
  }

  test("writeStream.table - create new managed table") {
    val memory = MemoryStream[Int]
    val tableName = "stream_test"
    withTable(tableName) {
      val sq = startTableStream(memory.toDS(), tableName, None)
      memory.addData(1, 2, 3)
      sq.processAllAvailable()

      checkDatasetUnorderly(
        spark.table(tableName).as[Int],
        1, 2, 3)

      val path = spark.sessionState.catalog.defaultTablePath(TableIdentifier(tableName))
      checkDatasetUnorderly(
        spark.read.format("delta").load(new File(path).getCanonicalPath).as[Int],
        1, 2, 3)
    }
  }

  test("writeStream.table - create new managed table with database") {
    val memory = MemoryStream[Int]
    val db = "my_db"
    val tableName = s"$db.stream_test"
    withDatabase(db) {
      sql(s"create database $db")
      withTable(tableName) {
        val sq = startTableStream(memory.toDS(), tableName, None)
        memory.addData(1, 2, 3)
        sq.processAllAvailable()

        checkDatasetUnorderly(
          spark.table(tableName).as[Int],
          1, 2, 3)

        val path = spark.sessionState.catalog.defaultTablePath(
          spark.sessionState.sqlParser.parseTableIdentifier(tableName))
        checkDatasetUnorderly(
          spark.read.format("delta").load(new File(path).getCanonicalPath).as[Int],
          1, 2, 3)
      }
    }
  }

  test("writeStream.table - create table from existing output") {
    withTempDir { dir =>
      Seq(4, 5, 6).toDF("value").write.format("delta").save(dir.getCanonicalPath)
      val memory = MemoryStream[Int]
      val tableName = "stream_test"
      withTable(tableName) {
        val sq = startTableStream(memory.toDS(), tableName, Some(dir))
        memory.addData(1, 2, 3)
        sq.processAllAvailable()

        checkDatasetUnorderly(
          spark.table(tableName).as[Int],
          1, 2, 3, 4, 5, 6)

        checkDatasetUnorderly(
          spark.read.format("delta").load(dir.getCanonicalPath).as[Int],
          1, 2, 3, 4, 5, 6)
      }
    }
  }

  test("writeStream.table - fail writing into a view") {
    val memory = MemoryStream[Int]
    val tableName = "stream_test"
    withTable(tableName) {
      val viewName = tableName + "_view"
      withView(viewName) {
        Seq(4, 5, 6).toDF("value").write.saveAsTable(tableName)
        sql(s"create view $viewName as select * from $tableName")
        val e = intercept[AnalysisException] {
          startTableStream(memory.toDS(), viewName, None)
        }
        assert(e.getMessage.contains("views"))
      }
    }
  }

  testQuietly("writeStream.table - fail due to different schema than existing Delta table") {
    withTempDir { dir =>
      Seq(4, 5, 6).toDF("id").write.format("delta").save(dir.getCanonicalPath)
      val memory = MemoryStream[Int]
      val tableName = "stream_test"
      withTable(tableName) {
        val e = intercept[AnalysisException] {
          val sq = startTableStream(memory.toDS(), tableName, Some(dir))
          memory.addData(1, 2, 3)
          sq.processAllAvailable()
        }
        assert(e.getMessage.contains("The specified schema does not match the existing schema"))
      }
    }
  }

  testQuietly("writeStream.table - fail due to different partitioning on existing Delta table") {
    withTempDir { dir =>
      Seq(4 -> "a").toDF("id", "key").write.format("delta").save(dir.getCanonicalPath)
      val memory = MemoryStream[(Int, String)]
      val tableName = "stream_test"
      withTable(tableName) {
        val e = intercept[AnalysisException] {
          val sq = startTableStream(
            memory.toDS().toDF("id", "key"), tableName, Some(dir), Seq("key"))
          memory.addData(1 -> "a")
          sq.processAllAvailable()
        }
        assert(e.getMessage.contains(
          "The specified partitioning does not match the existing partitioning"))
      }
    }
  }

  test("writeStream.table - fail writing into an external nonDelta table") {
    withTempDir { dir =>
      val memory = MemoryStream[(Int, String)]
      val tableName = "stream_test"
      withTable(tableName) {
        Seq(1).toDF("value").write.format("parquet")
          .option("path", dir.getCanonicalPath).saveAsTable(tableName)
        val e = intercept[AnalysisException] {
          startTableStream(memory.toDS(), tableName, Some(dir))
        }
        assert(e.getMessage.contains("delta"))
      }
    }
  }

  test("writeStream.table - fail writing into an external nonDelta path") {
    withTempDir { dir =>
      val memory = MemoryStream[Int]
      val tableName = "stream_test"
      withTable(tableName) {
        Seq(1).toDF("value").write.mode("append").parquet(dir.getCanonicalPath)
        val e = intercept[AnalysisException] {
          startTableStream(memory.toDS(), tableName, Some(dir))
        }
        assert(e.getMessage.contains("Delta"))
      }
    }
  }
}
