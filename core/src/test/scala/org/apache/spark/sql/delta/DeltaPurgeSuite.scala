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

import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

class DeltaPurgeSuite extends QueryTest
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

  testWithDVs("Purge DVs will combine small files") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      val log = DeltaLog.forTable(spark, path)
      spark
        .range(0, 100, 1, numPartitions = 5)
        .write
        .format("delta")
        .save(path)
      sql(s"DELETE FROM delta.`$path` WHERE id IN (0, 99)")
      assert(log.update().allFiles.filter(_.deletionVector != null).count() === 2)
      executePurge(path)
      val (addFiles, _) = getFileActionsInLastVersion(log)
      assert(addFiles.forall(_.deletionVector === null))
      checkAnswer(
        sql(s"SELECT * FROM delta.`$path`"),
        (1 to 98).toDF())
    }
  }

  testWithDVs("Purge DVs") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      val log = DeltaLog.forTable(spark, path)
      spark
        .range(0, 100, 1, numPartitions = 5)
        .write
        .format("delta")
        .save(path)
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
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      val log = DeltaLog.forTable(spark, path)
      spark
        .range(0, 100, 1, numPartitions = 5)
        .write
        .format("delta")
        .save(path)
      val versionBefore = log.update().version
      executePurge(path)
      val versionAfter = log.update().version
      assert(versionBefore === versionAfter)
    }
  }
}
