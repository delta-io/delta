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

package org.apache.spark.sql.delta.optimize

import org.apache.spark.sql.delta.DeletionVectorsTestUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.test.SharedSparkSession

class DeltaReorgSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest
  with DeletionVectorsTestUtils {

  import testImplicits._

  def executePurge(table: String, condition: Option[String] = None): Unit = {
    condition match {
      case Some(cond) => sql(s"REORG TABLE delta.`$table` WHERE $cond APPLY (PURGE)")
      case None => sql(s"REORG TABLE delta.`$table` APPLY (PURGE)")
    }
  }

  test("Purge DVs will combine small files") {
    val targetDf = spark.range(0, 100, 1, numPartitions = 5).toDF
    withTempDeltaTable(targetDf) { (_, log) =>
      val path = log.dataPath.toString

      sql(s"DELETE FROM delta.`$path` WHERE id IN (0, 99)")
      assert(log.update().allFiles.filter(_.deletionVector != null).count() === 2)
      withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_MAX_FILE_SIZE.key -> "1073741824") { // 1gb
        executePurge(path)
      }
      val (addFiles, _) = getFileActionsInLastVersion(log)
      assert(addFiles.size === 1, "files should be combined")
      assert(addFiles.forall(_.deletionVector === null))
      checkAnswer(
        sql(s"SELECT * FROM delta.`$path`"),
        (1 to 98).toDF())
    }
  }

  test("Purge DVs") {
    val targetDf = spark.range(0, 100, 1, numPartitions = 5).toDF()
    withTempDeltaTable(targetDf) { (_, log) =>
      val path = log.dataPath.toString

      sql(s"DELETE FROM delta.`$path` WHERE id IN (0, 99)")
      assert(log.update().allFiles.filter(_.deletionVector != null).count() === 2)

      // First purge
      executePurge(path)
      val (addFiles, _) = getFileActionsInLastVersion(log)
      assert(addFiles.size === 1) // two files are combined
      assert(addFiles.forall(_.deletionVector === null))
      checkAnswer(
        sql(s"SELECT * FROM delta.`$path`"),
        (1 to 98).toDF())

      // Second purge is a noop
      val versionBefore = log.update().version
      executePurge(path)
      val versionAfter = log.update().version
      assert(versionBefore === versionAfter)
    }
  }

  test("Purge a non-DV table is a noop") {
    val targetDf = spark.range(0, 100, 1, numPartitions = 5).toDF()
    withTempDeltaTable(targetDf, enableDVs = false) { (_, log) =>
      val versionBefore = log.update().version
      executePurge(log.dataPath.toString)
      val versionAfter = log.update().version
      assert(versionBefore === versionAfter)
    }
  }

  test("Purge some partitions of a table with DV") {
    val targetDf = spark.range(0, 100, 1, numPartitions = 1)
      .withColumn("part", col("id") % 4)
      .toDF()
    withTempDeltaTable(targetDf, partitionBy = Seq("part")) { (_, log) =>
      val path = log.dataPath
      // Delete one row from each partition
      sql(s"DELETE FROM delta.`$path` WHERE id IN (48, 49, 50, 51)")
      val (addFiles1, _) = getFileActionsInLastVersion(log)
      assert(addFiles1.size === 4)
      assert(addFiles1.forall(_.deletionVector !== null))
      // PURGE two partitions
      sql(s"REORG TABLE delta.`$path` WHERE part IN (0, 2) APPLY (PURGE)")
      val (addFiles2, _) = getFileActionsInLastVersion(log)
      assert(addFiles2.size === 2)
      assert(addFiles2.forall(_.deletionVector === null))
    }
  }
}
