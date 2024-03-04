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

package org.apache.spark.sql.delta.managedcommit

import java.io.File

import org.apache.spark.sql.delta.DeltaConfigs.{MANAGED_COMMIT_OWNER_CONF, MANAGED_COMMIT_OWNER_NAME}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.storage.LogStore
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaSQLTestUtils
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkConf
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession

class ManagedCommitSuite
    extends QueryTest
    with DeltaSQLTestUtils
    with SharedSparkSession
    with DeltaSQLCommandTest
    with ManagedCommitTestUtils {

  import testImplicits._

  override def sparkConf: SparkConf = {
    // Make sure all new tables in tests use tracking-in-memory commit store by default.
    super.sparkConf
      .set(MANAGED_COMMIT_OWNER_NAME.defaultTablePropertyKey, "tracking-in-memory")
      .set(MANAGED_COMMIT_OWNER_CONF.defaultTablePropertyKey, JsonUtils.toJson(Map()))
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    CommitStoreProvider.clearNonDefaultBuilders()
  }

  test("Optimistic Transaction errors out if 0th commit is not backfilled") {
    val commitStoreName = "nobackfilling-commit-store"
    object NoBackfillingCommitStoreBuilder extends CommitStoreBuilder {

      override def name: String = commitStoreName

      override def build(conf: Map[String, String]): CommitStore =
        new InMemoryCommitStore(batchSize = 5) {
          override def commit(
              logStore: LogStore,
              hadoopConf: Configuration,
              tablePath: Path,
              commitVersion: Long,
              actions: Iterator[String],
              updatedActions: UpdatedActions): CommitResponse = {
            val uuidFile =
              FileNames.uuidDeltaFile(new Path(tablePath, "_delta_log"), commitVersion)
            logStore.write(uuidFile, actions, overwrite = false, hadoopConf)
            val uuidFileStatus = uuidFile.getFileSystem(hadoopConf).getFileStatus(uuidFile)
            val commitTime = uuidFileStatus.getModificationTime
            commitImpl(logStore, hadoopConf, tablePath, commitVersion, uuidFileStatus, commitTime)

            CommitResponse(Commit(commitVersion, uuidFileStatus, commitTime))
          }
        }
    }

    CommitStoreProvider.registerBuilder(NoBackfillingCommitStoreBuilder)
    withSQLConf(MANAGED_COMMIT_OWNER_NAME.defaultTablePropertyKey -> commitStoreName) {
      withTempDir { tempDir =>
        val tablePath = tempDir.getAbsolutePath
        val ex = intercept[IllegalStateException] {
          Seq(1).toDF.write.format("delta").save(tablePath)
        }
        assert(ex.getMessage.contains(s"Expected 0th commit to be written" +
          s" to file:$tablePath/_delta_log/00000000000000000000.json"))
      }
    }
  }

  test("basic write") {
    CommitStoreProvider.registerBuilder(TrackingInMemoryCommitStoreBuilder(batchSize = 2))
    withTempDir { tempDir =>
      val tablePath = tempDir.getAbsolutePath
      Seq(1).toDF.write.format("delta").mode("overwrite").save(tablePath) // version 0
      Seq(2).toDF.write.format("delta").mode("overwrite").save(tablePath) // version 1
      Seq(3).toDF.write.format("delta").mode("append").save(tablePath) // version 2

      val log = DeltaLog.forTable(spark, tablePath)
      val commitsDir = new File(FileNames.commitDirPath(log.logPath).toUri)
      val unbackfilledCommitVersions =
        commitsDir
          .listFiles()
          .filterNot(f => f.getName.startsWith(".") && f.getName.endsWith(".crc"))
          .map(_.getAbsolutePath)
          .sortBy(path => path).map { commitPath =>
            assert(FileNames.isDeltaFile(new Path(commitPath)))
            FileNames.deltaVersion(new Path(commitPath))
          }
      assert(unbackfilledCommitVersions === Array(0, 1, 2))
      checkAnswer(sql(s"SELECT * FROM delta.`$tablePath`"), Seq(Row(2), Row(3)))
    }
  }
}
