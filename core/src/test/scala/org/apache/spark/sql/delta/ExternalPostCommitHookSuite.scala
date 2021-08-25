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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.actions.Action
import org.apache.spark.sql.delta.hooks.PostCommitHook
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

class ExternalPostCommitHookSuite extends DeltaGenerateSymlinkManifestSuiteBase
  with DeltaSQLCommandTest {

  import testImplicits._

  test("generate manifest with postCommitHookClass") {
    withTempDir { tablePath =>
      tablePath.delete()
      withSQLConf(
        "spark.databricks.delta.properties.defaults.postCommitHookClass" ->
          classOf[ExternalPostCommitHook].getName) {
        val numPartitions = 7
        spark.range(100).repartition(numPartitions)
          .write.format("delta").mode("overwrite").save(tablePath.toString)

        assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = numPartitions)
      }
    }
  }
}

class ExternalPostCommitHook extends PostCommitHook {
  /** A user friendly name for the hook for error reporting purposes. */
  override val name: String = "ExternalPostCommitHook"

  /** Executes the hook. */
  override def run(
    spark: SparkSession,
    txn: OptimisticTransactionImpl,
    committedActions: Seq[Action]): Unit = {

    // Create a Delta table and call the scala api for generating manifest files
    spark.sql(s"GENERATE symlink_ForMat_Manifest FOR TABLE delta.`${txn.deltaLog.dataPath}`")
  }
}
