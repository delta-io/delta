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
import java.text.SimpleDateFormat

import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.FileNames

import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/** Base suite containing the restore tests. */
trait RestoreTableSuiteBase extends QueryTest with SharedSparkSession  with DeltaSQLCommandTest {

  import testImplicits._

  // Will be overridden in sub-class
  /**
   * @param tblId            - the table identifier either table name or path
   * @param version          - version to restore to
   * @param isMetastoreTable - whether its a path based table or metastore table
   * @param expectNoOp       - whether the restore is no-op or not
   */
  protected def restoreTableToVersion(
     tblId: String,
     version: Int,
     isMetastoreTable: Boolean,
     expectNoOp: Boolean = false): DataFrame

  /**
   * @param tblId            - the table identifier either table name or path
   * @param timestamp        - timestamp to restore to
   * @param isMetastoreTable - whether its a path based table or a metastore table.
   * @param expectNoOp       - whether the restore is no-op or not
   */
  protected def restoreTableToTimestamp(
     tblId: String,
     timestamp: String,
     isMetastoreTable: Boolean,
     expectNoOp: Boolean = false): DataFrame

  test("path based table") {
    withTempDir { tempDir =>
      val path = tempDir.getAbsolutePath

      val df1 = Seq(1, 2, 3, 4, 5).toDF("id")
      val df2 = Seq(6, 7).toDF("id")
      val df3 = Seq(8, 9, 10).toDF("id")

      // write version 0 of the table
      df1.write.format("delta").save(path) // version 0

      val deltaLog = DeltaLog.forTable(spark, path)
      require(deltaLog.snapshot.version == 0)

      // append df2 to the table
      df2.write.format("delta").mode("append").save(path) // version  1

      // append df3 to the table
      df3.write.format("delta").mode("append").save(path) // version 2

      // check if the table has all the three dataframes written
      checkAnswer(spark.read.format("delta").load(path), df1.union(df2).union(df3))

      // restore by version to version 1
      restoreTableToVersion(path, 1, false)
      checkAnswer(spark.read.format("delta").load(path), df1.union(df2))

      // Set a custom timestamp for the commit
      val desiredTime = "1996-01-12"
      val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
      val time = format.parse(desiredTime).getTime
      val file = new File(FileNames.deltaFile(deltaLog.logPath, 0).toUri)
      file.setLastModified(time)

      // restore by timestamp to version 0
      restoreTableToTimestamp(path, desiredTime, false)
      checkAnswer(spark.read.format("delta").load(path), df1)
    }
  }

  test("metastore based table") {
    val identifier = "tbl"
    withTable(identifier) {

      val df1 = Seq(1, 2, 3, 4, 5).toDF("id")
      val df2 = Seq(6, 7).toDF("id")

      // write first version of the table
      df1.write.format("delta").saveAsTable(identifier) // version 0

      val deltaLog = DeltaLog.forTable(spark, new TableIdentifier(identifier))
      require(deltaLog.snapshot.version == 0)

      // append df2 to the table
      df2.write.format("delta").mode("append").saveAsTable(identifier) // version  1

      // check if the table has all the three dataframes written
      checkAnswer(spark.read.format("delta").table(identifier), df1.union(df2))


      // restore by version to version 0
      restoreTableToVersion(identifier, 0, true)
      checkAnswer(spark.read.format("delta").table(identifier), df1)
    }
  }

  test("restore a restore back to pre-restore version") {
    withTempDir { tempDir =>
      val df1 = Seq(1, 2, 3).toDF("id")
      val df2 = Seq(4, 5, 6).toDF("id")
      val df3 = Seq(7, 8, 9).toDF("id")
      df1.write.format("delta").save(tempDir.getAbsolutePath)
      val deltaLog = DeltaLog.forTable(spark, tempDir.getAbsolutePath)
      require(deltaLog.snapshot.version == 0)

      df2.write.format("delta").mode("append").save(tempDir.getAbsolutePath)
      assert(deltaLog.update().version == 1)

      df3.write.format("delta").mode("append").save(tempDir.getAbsolutePath)
      assert(deltaLog.update().version == 2)

      // we have three versions now, let's restore to version 1 first
      restoreTableToVersion(tempDir.getAbsolutePath, 1, false)

      checkAnswer(spark.read.format("delta").load(tempDir.getAbsolutePath), df1.union(df2))
      assert(deltaLog.update().version == 3)

      restoreTableToVersion(tempDir.getAbsolutePath, 2, false)
      checkAnswer(
        spark.read.format("delta").load(tempDir.getAbsolutePath), df1.union(df2).union(df3))

      assert(deltaLog.update().version == 4)
    }
  }

  test("restore to a restored version") {
    withTempDir { tempDir =>
      val df1 = Seq(1, 2, 3).toDF("id")
      val df2 = Seq(4, 5, 6).toDF("id")
      val df3 = Seq(7, 8, 9).toDF("id")
      df1.write.format("delta").save(tempDir.getAbsolutePath)
      val deltaLog = DeltaLog.forTable(spark, tempDir.getAbsolutePath)
      require(deltaLog.update().version == 0)

      df2.write.format("delta").mode("append").save(tempDir.getAbsolutePath)
      assert(deltaLog.update().version == 1)

      // we have two versions now, let's restore to version 0 first
      restoreTableToVersion(tempDir.getAbsolutePath, 0, false)

      checkAnswer(spark.read.format("delta").load(tempDir.getAbsolutePath), df1)
      assert(deltaLog.update().version == 2)

      df3.write.format("delta").mode("append").save(tempDir.getAbsolutePath)
      assert(deltaLog.update().version == 3)

      // now we restore a restored version
      restoreTableToVersion(tempDir.getAbsolutePath, 2, false)
      checkAnswer(spark.read.format("delta").load(tempDir.getAbsolutePath), df1)
      assert(deltaLog.update().version == 4)
    }
  }
}
