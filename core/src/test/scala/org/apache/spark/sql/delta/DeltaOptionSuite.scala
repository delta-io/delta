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

import org.apache.spark.sql.delta.actions.{Action, FileAction}
import org.apache.spark.sql.delta.util.FileNames

import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

class DeltaOptionSuite extends QueryTest
  with SharedSparkSession {

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
    val commitActions = deltaLog.store.read(FileNames.deltaFile(deltaLog.logPath, version))
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
    val commitActions = deltaLog.store.read(FileNames.deltaFile(deltaLog.logPath, version))
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
}
