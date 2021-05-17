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

import java.io.File
import java.net.URI
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Locale

import scala.concurrent.duration._
import scala.language.implicitConversions

import org.apache.spark.sql.delta.DeltaTestUtils.OptimisticTxnTestHelper
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.util.FileNames
import org.apache.hadoop.fs.Path
import org.scalatest.GivenWhenThen

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

/** A set of tests which we can open source after Spark 3.0 is released. */
trait DeltaTimeTravelTests extends QueryTest
    with SharedSparkSession
    with GivenWhenThen {
  protected implicit def durationToLong(duration: FiniteDuration): Long = {
    duration.toMillis
  }

  protected implicit def longToTimestamp(ts: Long): Timestamp = new Timestamp(ts)

  protected val timeFormatter = new SimpleDateFormat("yyyyMMddHHmmssSSS")

  protected def modifyCommitTimestamp(deltaLog: DeltaLog, version: Long, ts: Long): Unit = {
    val file = new File(FileNames.deltaFile(deltaLog.logPath, version).toUri)
    file.setLastModified(ts)
    val crc = new File(FileNames.checksumFile(deltaLog.logPath, version).toUri)
    if (crc.exists()) {
      crc.setLastModified(ts)
    }
  }

  protected def getTableLocation(table: String): String = {
    spark.sessionState.catalog.getTableMetadata(TableIdentifier(table)).location.toString
  }

  /** Generate commits with the given timestamp in millis. */
  protected def generateCommitsCheap(
      deltaLog: DeltaLog, commits: Long*): Unit = {
    var startVersion = deltaLog.snapshot.version + 1
    commits.foreach { ts =>
      val action = AddFile(startVersion.toString, Map.empty, 10L, startVersion, dataChange = true)
      deltaLog.startTransaction().commitManually(action)
      modifyCommitTimestamp(deltaLog, startVersion, ts)
      startVersion += 1
    }
  }


  /** Alternate for `withTables` as we leave some tables in an unusable state for clean up */
  override protected def withTable(tableNames: String*)(f: => Unit): Unit = {
    val tablePaths = tableNames.map(t =>
      spark.sessionState.catalog.defaultTablePath(TableIdentifier(t)))
    try f finally {
      tablePaths.foreach(p => Utils.deleteRecursively(new File(p)))
      tableNames.foreach(t => sql(s"DROP TABLE IF EXISTS $t"))
    }
  }

  protected implicit def longToTimestampExpr(value: Long): String = {
    s"cast($value / 1000 as timestamp)"
  }

  import testImplicits._

  test("time travel with partition changes and data skipping - should instantiate old schema") {
    withTempDir { dir =>
      val tblLoc = dir.getCanonicalPath
      val v0 = spark.range(10).withColumn("part5", 'id % 5)

      v0.write.format("delta").partitionBy("part5").mode("append").save(tblLoc)
      val deltaLog = DeltaLog.forTable(spark, tblLoc)

      val schemaString = spark.range(10, 20).withColumn("part2", 'id % 2).schema.json
        deltaLog.startTransaction().commit(
          Seq(deltaLog.snapshot.metadata.copy(
            schemaString = schemaString,
            partitionColumns = Seq("part2"))),
          DeltaOperations.ManualUpdate
        )
      checkAnswer(
        spark.read.option("versionAsOf", 0).format("delta").load(tblLoc).where("part5 = 1"),
        v0.where("part5 = 1"))
    }
  }

  test("can't provide both version and timestamp in DataFrameReader") {
    val e = intercept[IllegalArgumentException] {
      spark.read.option("versionaSof", 1)
          .option("timestampAsOF", "fake").format("delta").load("/some/fake")
    }
    assert(e.getMessage.contains("either provide 'timestampAsOf' or 'versionAsOf'"))
  }


  test("don't time travel a valid non-delta path with @ syntax") {
    val format = "json"
    withTempDir { dir =>
      val path = new File(dir, "base@v0").getCanonicalPath
      spark.range(10).write.format(format).mode("append").save(path)
      spark.range(10).write.format(format).mode("append").save(path)

      checkAnswer(
        spark.read.format(format).load(path),
        spark.range(10).union(spark.range(10)).toDF()
      )

      checkAnswer(
        spark.table(s"$format.`$path`"),
        spark.range(10).union(spark.range(10)).toDF()
      )

      intercept[AnalysisException] {
        spark.read.format(format).load(path + "@v0").count()
      }

      intercept[AnalysisException] {
        spark.table(s"$format.`$path@v0`").count()
      }
    }
  }
}

abstract class DeltaHistoryManagerBase extends DeltaTimeTravelTests // Edge // Edge
  {

}

/** Uses V2 resolution code paths */
class DeltaHistoryManagerSuite extends DeltaHistoryManagerBase {
  override protected def sparkConf: SparkConf = {
    super.sparkConf.set(SQLConf.USE_V1_SOURCE_LIST.key, "parquet,json")
  }
}
