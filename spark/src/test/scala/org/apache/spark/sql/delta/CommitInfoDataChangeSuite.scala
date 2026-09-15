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

import org.apache.spark.sql.delta.actions.{Action, AddCDCFile, AddFile, CommitInfo, Metadata, Protocol, SetTransaction}
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests that the `dataChange` recorded in a commit's [[CommitInfo]] matches the file actions the
 * commit actually contains, for the normal transaction path and commitLarge path.
 */
class CommitInfoDataChangeSuite
  extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest
  with DeltaTableProvider {

  private def addFile(path: String, dataChange: Boolean): AddFile =
    AddFile(path, Map.empty, size = 1L, modificationTime = 1L, dataChange = dataChange)

  /** Reads the `dataChange` recorded in the [[CommitInfo]] of the given commit. */
  private def commitInfoDataChangeAt(deltaLog: DeltaLog, version: Long): Option[Boolean] =
    deltaLog
      .getChanges(version)
      .collectFirst { case (`version`, actions) => actions }
      .getOrElse(fail(s"no commit at version $version"))
      .collectFirst { case ci: CommitInfo => ci }
      .getOrElse(fail(s"no CommitInfo at version $version"))
      .dataChange

  private def latestCommitInfoDataChange(path: String): Option[Boolean] = {
    val deltaLog = DeltaLog.forTable(spark, path)
    commitInfoDataChangeAt(deltaLog, deltaLog.update().version)
  }

  test("dataChangeFromActions is false without file actions") {
    assert(!CommitInfo.dataChangeFromActions(Nil))
    val nonFileActions: Seq[Action] =
      Seq(Metadata(), Protocol(1, 2), SetTransaction("app", 1L, None))
    assert(!CommitInfo.dataChangeFromActions(nonFileActions))
  }

  test("dataChangeFromActions follows the file actions") {
    assert(CommitInfo.dataChangeFromActions(Seq(addFile("a", dataChange = true))))
    assert(!CommitInfo.dataChangeFromActions(Seq(addFile("a", dataChange = false))))

    val rearrange = addFile("a", dataChange = false)
    val dataChanging = addFile("b", dataChange = true)
    assert(CommitInfo.dataChangeFromActions(Seq(rearrange, dataChanging)))
    assert(CommitInfo.dataChangeFromActions(Seq(dataChanging, rearrange)))

    assert(CommitInfo.dataChangeFromActions(Seq(dataChanging.remove)))
    val rearrangeRemove = rearrange.removeWithTimestamp(dataChange = false)
    assert(!CommitInfo.dataChangeFromActions(Seq(rearrangeRemove)))
  }

  test("dataChangeFromActions ignores change data files") {
    // AddCDCFile always carries dataChange = false, so a commit of only CDC files does not change
    // the table's data.
    val cdc = AddCDCFile("cdc", Map.empty, size = 1L)
    assert(!cdc.dataChange)
    assert(!CommitInfo.dataChangeFromActions(Seq(cdc)))
    assert(CommitInfo.dataChangeFromActions(Seq(cdc, addFile("a", dataChange = true))))
  }

  test("append records dataChange = true") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(5).write.format(writeFormat).save(path)
      assert(latestCommitInfoDataChange(path).contains(true))

      spark.range(5, 10).write.format(writeFormat).mode("append").save(path)
      assert(latestCommitInfoDataChange(path).contains(true))
    }
  }

  test("DELETE records dataChange = true") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(10).write.format(writeFormat).save(path)
      sql(s"DELETE FROM $tableProvider.`$path` WHERE id < 5")
      assert(latestCommitInfoDataChange(path).contains(true))
    }
  }

  test("metadata-only commit records dataChange = false") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(5).write.format(writeFormat).save(path)
      sql(s"ALTER TABLE $tableProvider.`$path` SET TBLPROPERTIES ('someKey' = 'someValue')")
      assert(latestCommitInfoDataChange(path).contains(false))
    }
  }

  test("OPTIMIZE records dataChange = false") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(5).repartition(1).write.format(writeFormat).save(path)
      spark.range(5, 10).repartition(1).write.format(writeFormat).mode("append").save(path)
      val deltaLog = DeltaLog.forTable(spark, path)
      val versionBeforeOptimize = deltaLog.update().version

      sql(s"OPTIMIZE $tableProvider.`$path`")

      val optimizeVersion = deltaLog.update().version
      assert(optimizeVersion > versionBeforeOptimize, "OPTIMIZE did not commit")
      assert(commitInfoDataChangeAt(deltaLog, optimizeVersion).contains(false))
    }
  }

  test("a rearrange-only commit records dataChange = false") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(5).write.format(writeFormat).save(path)
      val deltaLog = DeltaLog.forTable(spark, path)
      val file = deltaLog.update().allFiles.head()

      deltaLog.startTransaction().commit(
        Seq(file.removeWithTimestamp(dataChange = false)),
        DeltaOperations.ManualUpdate)

      assert(commitInfoDataChangeAt(deltaLog, deltaLog.update().version).contains(false))
    }
  }

  test("the dataChange write option records dataChange = false") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(10).repartition(2).write.format(writeFormat).save(path)
      val deltaLog = DeltaLog.forTable(spark, path)
      val versionBeforeCompaction = deltaLog.update().version

      // The option is read as `DeltaOptions.rearrangeOnly`, which makes `WriteIntoDelta` stamp
      // `dataChange = !rearrangeOnly` on every file action it hands to `txn.commit`. The commit
      // then derives the CommitInfo summary from those actions.
      spark.read.format(writeFormat).load(path)
        .repartition(1)
        .write.format(writeFormat).mode("overwrite")
        .option("dataChange", "false")
        .save(path)

      val compactionVersion = deltaLog.update().version
      assert(compactionVersion > versionBeforeCompaction, "the compaction did not commit")
      assert(commitInfoDataChangeAt(deltaLog, compactionVersion).contains(false))
    }
  }

  test("an overwrite without the dataChange write option records dataChange = true") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(10).write.format(writeFormat).save(path)
      val deltaLog = DeltaLog.forTable(spark, path)
      val versionBeforeOverwrite = deltaLog.update().version

      spark.range(10, 20).write.format(writeFormat).mode("overwrite").save(path)

      val overwriteVersion = deltaLog.update().version
      assert(overwriteVersion > versionBeforeOverwrite, "the overwrite did not commit")
      assert(commitInfoDataChangeAt(deltaLog, overwriteVersion).contains(true))
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // commitLarge
  ///////////////////////////////////////////////////////////////////////////

  test("CONVERT TO DELTA records dataChange = true") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      spark.range(5).write.format("parquet").save(path)
      sql(s"CONVERT TO DELTA parquet.`$path`")
      assert(latestCommitInfoDataChange(path).contains(true))
    }
  }


  test("SHALLOW CLONE records dataChange = true") {
    withTempDir { sourceDir =>
      withTempDir { targetDir =>
        val sourcePath = sourceDir.getCanonicalPath
        val targetPath = targetDir.getCanonicalPath
        spark.range(5).write.format(writeFormat).save(sourcePath)

        sql(s"CREATE OR REPLACE TABLE $tableProvider.`$targetPath` " +
          s"SHALLOW CLONE $tableProvider.`$sourcePath`")

        assert(latestCommitInfoDataChange(targetPath).contains(true))
      }
    }
  }
}
