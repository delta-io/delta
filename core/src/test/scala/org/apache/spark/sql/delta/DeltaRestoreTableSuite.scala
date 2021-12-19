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
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, SaveMode}

trait DeltaRestoreTableSuiteBase extends QueryTest
  with SharedSparkSession
  with SQLTestUtils
  with DeltaTestUtilsForTempViews {

  private lazy val versionZeroData = spark.range(2).withSequenceColumn("seq").toDF()
  private lazy val versionOneData = spark.range(2, 4)
    .withSequenceColumn("seq")
    .withColumnRenamed("id", "new_id")
  private lazy val allDatasets = Seq(versionZeroData, versionOneData)

  for((tableType, partition) <- Seq("partitioned" -> Some("seq"), "non-partitioned" -> None)) {

    test(s"basic case - Scala restore table using version - $tableType") {
      withMultiVersionedDeltaTable(allDatasets, partition) { deltaPath =>

        DeltaTableForPath(deltaPath).restoreToVersion(0)

        testRestoredTable(deltaPath)
      }
    }

    test(s"basic case - Scala restore table using timestamp - $tableType") {
      withMultiVersionedDeltaTable(allDatasets, partition) { deltaPath =>
        val zeroVersionTs = new Timestamp(
          DeltaLog.forTable(spark, deltaPath).getSnapshotAt(0).timestamp)

        DeltaTableForPath(deltaPath).restoreToTimestamp(zeroVersionTs.toString)

        testRestoredTable(deltaPath)
      }
    }
  }

  test("basic case - Scala restore failed if any data file is missed" +
      " and spark.sql.files.ignoreMissingFiles=false") {
    withMultiVersionedDeltaTable(allDatasets) { deltaPath =>
      import testImplicits._
      // Remove one data files from table
      val file = DeltaLog.forTable(spark, deltaPath).getSnapshotAt(0).allFiles.map(_.path).head()
      FileUtils.deleteQuietly(new File(new Path(deltaPath, file).toUri.toString))

      val ex = intercept[IllegalArgumentException](DeltaTableForPath(deltaPath).restoreToVersion(0))
      assert(ex.getMessage.contains(file), s"Error message doesn't contain missed file $file."
        + s"Error message:\n ${ex.getMessage}")
    }
  }

  test("basic case - Scala restore failed to restore incorrect version") {
    withMultiVersionedDeltaTable(allDatasets) { deltaPath =>
      val lastVersion = DeltaLog.forTable(spark, deltaPath).update().version.toString
      val ex = intercept[IllegalArgumentException](DeltaTableForPath(deltaPath)
        .restoreToVersion(Long.MaxValue))

      assert(
        ex.getMessage.contains(Long.MaxValue.toString) && ex.getMessage.contains(lastVersion),
        s"Error doesn't contain version to restore or last version. Error:\n ${ex.getMessage}")
    }
  }

  test("basic case - Scala restore failed to restore incorrect timestamp format") {
    withMultiVersionedDeltaTable(allDatasets) { deltaPath =>

      val ex = intercept[AnalysisException](DeltaTableForPath(deltaPath)
        .restoreToTimestamp("9999/99/99"))
      assert(
        ex.getMessage.contains("9999/99/99"),
        s"Error message doesn't contain incorrect timestamp. Error message:\n ${ex.getMessage}")
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
