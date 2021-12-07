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

import java.io.File
import java.sql.Timestamp
import scala.language.implicitConversions

import io.delta.implicits.DeltaDataFrameWriter
import io.delta.tables.DeltaTable.{forPath => DeltaTableForPath}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SQLTestUtils, SharedSparkSession}
import org.apache.spark.sql.{DataFrame, QueryTest, SaveMode}

trait DeltaRestoreTableSuiteBase extends QueryTest
  with SharedSparkSession
  with SQLTestUtils
  with DeltaTestUtilsForTempViews {

  private lazy val versionZeroData = spark.range(2).withSequenceColumn("seq").toDF()
  private lazy val versionOneData = spark.range(2, 4)
    .withSequenceColumn("seq")
    .withColumnRenamed("id", "new_id")
  private lazy val allDatasets = Seq(versionZeroData, versionOneData)

  test("basic case - Scala restore table using version - Non-partitioned") {
    withMultiVersionedDeltaTable(allDatasets) { deltaPath =>

      DeltaTableForPath(deltaPath).restore(0)

      testRestoredTable(deltaPath)
    }
  }

  test("basic case - Scala restore table using version - Partitioned") {
    withMultiVersionedDeltaTable(allDatasets, Some("seq")) { deltaPath =>

      DeltaTableForPath(deltaPath).restore(0)

      testRestoredTable(deltaPath)
    }
  }

  test("basic case - Scala restore table using timestamp - Non-partitioned") {
    withMultiVersionedDeltaTable(allDatasets) { deltaPath =>
      val zeroVersionTs = new Timestamp(
        DeltaLog.forTable(spark, deltaPath).getSnapshotAt(0).timestamp)

      DeltaTableForPath(deltaPath).restore(zeroVersionTs)

      testRestoredTable(deltaPath)
    }
  }

  test("basic case - Scala restore table using timestamp - Partitioned") {
    withMultiVersionedDeltaTable(allDatasets, Some("seq")) { deltaPath =>
      val zeroVersionTs = new Timestamp(
        DeltaLog.forTable(spark, deltaPath).getSnapshotAt(0).timestamp)

      DeltaTableForPath(deltaPath).restore(zeroVersionTs)

      testRestoredTable(deltaPath)
    }
  }

  test("basic case - Scala restore failed if any data file is missed" +
      " and spark.sql.files.ignoreMissingFiles=false") {
    withMultiVersionedDeltaTable(allDatasets) { deltaPath =>
      import testImplicits._
      // Remove one data files from table
      val file = DeltaLog.forTable(spark, deltaPath).getSnapshotAt(0).allFiles.map(_.path).head()
      FileUtils.deleteQuietly(new File(new Path(deltaPath, file).toUri.toString))

      val ex = intercept[IllegalArgumentException](DeltaTableForPath(deltaPath).restore(0))
      assert(ex.getMessage.contains(file), s"Error message doesn't contain missed file $file."
        + s"Error message:\n ${ex.getMessage}")
    }
  }

  test("basic case - Scala restore failed if any data file is missed" +
      " and spark.sql.files.ignoreMissingFiles=true") {
    withSQLConf(SQLConf.IGNORE_MISSING_FILES.key -> "true") {
      withMultiVersionedDeltaTable(allDatasets) { deltaPath =>
        import testImplicits._
        // Remove one data files from table
        val file = DeltaLog.forTable(spark, deltaPath).getSnapshotAt(0).allFiles.map(_.path).head()
        FileUtils.deleteQuietly(new File(new Path(deltaPath, file).toUri.toString))

        DeltaTableForPath(deltaPath).restore(0)

        testRestoredTable(deltaPath, validateData = false)
      }
    }
  }

  test("basic case - Scala restore failed to restore incorrect version") {
    withMultiVersionedDeltaTable(allDatasets) { deltaPath =>

      intercept[IllegalArgumentException](DeltaTableForPath(deltaPath).restore(Long.MaxValue))
    }
  }

  def withMultiVersionedDeltaTable(
    datasets: Seq[DataFrame],
    partitionBy: Option[String] = None)(f: String => Unit): Unit = {
    withTempDir { tempDir =>
      val deltaPath = tempDir.getAbsolutePath

      datasets.foreach { ds =>
        val writer = ds.write.option("mergeSchema", "true").mode(SaveMode.Overwrite)
        partitionBy.foreach(writer.partitionBy(_))
        writer.delta(deltaPath)
      }

      f(deltaPath)
    }
  }

  def testRestoredTable(deltaPath: String, validateData: Boolean = true): Unit = {
    val expected = DeltaLog.forTable(spark, deltaPath).getSnapshotAt(0)
    val restored = DeltaLog.forTable(spark, deltaPath).update()

    assert(expected.metadata == restored.metadata, "Incorrect metadata was restored")
    checkAnswer(expected.allFiles.toDF(), restored.allFiles.toDF())
    if (validateData) checkAnswer(DeltaTableForPath(deltaPath).toDF, versionZeroData)
  }
}

class DeltaRestoreTableSuite
  extends DeltaRestoreTableSuiteBase with DeltaSQLCommandTest
