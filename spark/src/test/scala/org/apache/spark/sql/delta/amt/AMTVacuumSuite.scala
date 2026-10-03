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

package org.apache.spark.sql.delta.amt

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.delta.{AdaptiveMetadataTableFeature, DeltaLog}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.commands.VacuumCommand
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaTestImplicits.DeltaTableV2ObjectTestHelper

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.TableIdentifier

trait AMTVacuumTests extends AMTCheckpointTestBase {

  private def dataFilesOnDisk(deltaLog: DeltaLog): Set[String] = {
    val fs = deltaLog.dataPath.getFileSystem(deltaLog.newDeltaHadoopConf())
    val files = ArrayBuffer.empty[String]
    val it = fs.listFiles(deltaLog.dataPath, true)
    while (it.hasNext) {
      val path = it.next().getPath.toString
      val isDataFile = path.endsWith(".parquet") && path.contains("part-") &&
        !path.contains("/_delta_log/") && !path.contains("/metadata/")
      if (isDataFile) {
        files += path
      }
    }
    files.toSet
  }

  /** Physical files on disk under the table's AMT manifest directory (`metadata/`). */
  private def metadataFilesOnDisk(deltaLog: DeltaLog): Set[String] = {
    val fs = deltaLog.dataPath.getFileSystem(deltaLog.newDeltaHadoopConf())
    val files = ArrayBuffer.empty[String]
    val it = fs.listFiles(deltaLog.dataPath, true)
    while (it.hasNext) {
      val path = it.next().getPath.toString
      if (path.contains("/metadata/")) {
        files += path
      }
    }
    files.toSet
  }

  /** Overwrite the whole table with `numRows` rows, one data file per row (a single commit). */
  private def overwriteRowsAsSeparateFiles(tableName: String, numRows: Int, startId: Int): Unit = {
    withSQLConf(
        "spark.sql.files.maxRecordsPerFile" -> "1",
        DeltaSQLConf.DELTA_OPTIMIZE_WRITE_ENABLED.key -> "false") {
      sql(
        s"INSERT OVERWRITE $tableName " +
          s"SELECT CAST(id AS INT) FROM range($startId, ${startId + numRows})")
    }
  }

  /** Exact physical data-file paths on disk after seeding, split into removed vs live. */
  private case class SeededFiles(removed: Set[String], active: Set[String], latestVersion: Long) {
    /** All data files sitting on disk after seeding (removed-but-unvacuumed plus live). */
    def all: Set[String] = removed ++ active
  }

  /**
   * Seeds a removal history spread over many commits: one initial append followed by `rounds`
   * overwrite commits, each of which removes the previous round's files (a `RemoveFile` in every
   * overwrite commit) and adds `filesPerCommit` new ones. So there are `rounds` distinct commits
   * that each removed files.
   *
   * Returns the exact set of physical data-file paths, split into the files those commits removed
   * (still on disk, not yet vacuumed) and the files still live in the latest snapshot. Captured by
   * snapshotting the disk just before the final overwrite (everything present then becomes removed
   * once the final overwrite replaces it) and diffing against the disk after it.
   */
  private def seedAndCaptureFiles(
      tableName: String, rounds: Int, filesPerCommit: Int = 3): SeededFiles = {
    require(rounds >= 1, "need at least one overwrite to remove the appended files")
    val deltaLog = deltaLogForName(tableName)
    appendRowsAsSeparateFiles(tableName, numFiles = filesPerCommit, startId = 0)
    (1 until rounds).foreach { round =>
      overwriteRowsAsSeparateFiles(tableName, numRows = filesPerCommit, startId = round * 1000)
    }
    // Everything on disk just before the final overwrite is non-live after it (the overwrite
    // replaces the whole table), so this is exactly the removed-but-unvacuumed set.
    val removed = dataFilesOnDisk(deltaLog)
    overwriteRowsAsSeparateFiles(tableName, numRows = filesPerCommit, startId = rounds * 1000)
    val active = dataFilesOnDisk(deltaLog) -- removed
    SeededFiles(removed, active, deltaLog.update().version)
  }

  test("AMT table protects files removed within the retention window") {
    val tableName = "amt_vacuum_protect"
    withTable(tableName) {
      createAMTTable(tableName)
      val deltaLog = deltaLogForName(tableName)

      // An initial append plus 5 overwrites, each removing the previous round's files.
      val rounds = 5
      val seeded = seedAndCaptureFiles(tableName, rounds)
      // Each append/overwrite is its own commit (an AMT overwrite may even span more than one), so
      // the removal history is spread over many commits that commit traversal must read.
      assert(seeded.latestVersion >= rounds + 1,
        s"expected at least ${rounds + 1} commits, got ${seeded.latestVersion}")

      // Precondition: the removed files and the live files are all physically on disk.
      assert(seeded.removed.nonEmpty, "expected removed-but-unvacuumed files to make the test real")
      assert(dataFilesOnDisk(deltaLog) === seeded.all,
        "expected every removed and live data file on disk before vacuum")

      // VACUUM with default retention: the removals just happened, so they are within the window
      // and every removed file must be protected -- the on-disk set must be unchanged.
      val deltaTable = DeltaTableV2(spark, TableIdentifier(tableName))
      VacuumCommand.gc(spark, deltaTable, dryRun = false)

      assert(dataFilesOnDisk(deltaLog) === seeded.all,
        "exactly the removed and live files must remain: nothing within retention may be deleted")
    }
  }

  test("AMT table deletes files removed outside the retention window") {
    val tableName = "amt_vacuum_delete"
    withTable(tableName) {
      createAMTTable(tableName)
      val deltaLog = deltaLogForName(tableName)

      val rounds = 5
      val seeded = seedAndCaptureFiles(tableName, rounds)
      // Each append/overwrite is its own commit (an AMT overwrite may even span more than one), so
      // the removal history is spread over many commits that commit traversal must read.
      assert(seeded.latestVersion >= rounds + 1,
        s"expected at least ${rounds + 1} commits, got ${seeded.latestVersion}")

      assert(seeded.removed.nonEmpty, "expected removed-but-unvacuumed files to make the test real")
      assert(dataFilesOnDisk(deltaLog) === seeded.all,
        "expected every removed and live data file on disk before vacuum")

      // RETAIN 0 HOURS puts every removal outside the window; disable the safety check so the
      // sub-default retention is allowed.
      val deltaTable = DeltaTableV2(spark, TableIdentifier(tableName))
      withSQLConf(DeltaSQLConf.DELTA_VACUUM_RETENTION_CHECK_ENABLED.key -> "false") {
        VacuumCommand.gc(spark, deltaTable, retentionHours = Some(0), dryRun = false)
      }

      // Exactly the removed files are deleted; the live files are kept.
      val remaining = dataFilesOnDisk(deltaLog)
      assert(remaining === seeded.active,
        "exactly the removed files must be deleted, leaving only the live files")
      assert(seeded.removed.intersect(remaining).isEmpty,
        "no file removed outside the retention window may survive")
    }
  }

  test("AMT table protects a long history of removals accumulated over many commits") {
    val tableName = "amt_vacuum_history"
    withTable(tableName) {
      createAMTTable(tableName)
      val deltaLog = deltaLogForName(tableName)

      // A longer commit range, so commit traversal must read removals from many distinct commits.
      val rounds = 10
      val seeded = seedAndCaptureFiles(tableName, rounds)
      // Each append/overwrite is its own commit (an AMT overwrite may even span more than one), so
      // the removal history is spread over many commits that commit traversal must read.
      assert(seeded.latestVersion >= rounds + 1,
        s"expected at least ${rounds + 1} commits, got ${seeded.latestVersion}")


      assert(seeded.removed.nonEmpty, "expected removed files from many commits on disk")
      assert(dataFilesOnDisk(deltaLog) === seeded.all,
        "expected every removed and live data file on disk before vacuum")

      val deltaTable = DeltaTableV2(spark, TableIdentifier(tableName))
      VacuumCommand.gc(spark, deltaTable, dryRun = false)

      assert(dataFilesOnDisk(deltaLog) === seeded.all,
        "the full history of removals within the retention window must be protected exactly")
    }
  }

  test("VACUUM never deletes AMT manifest files under metadata/") {
    val tableName = "amt_vacuum_metadata_untouched"
    withTable(tableName) {
      createAMTTable(tableName)
      val deltaLog = deltaLogForName(tableName)

      // Several commits so a real manifest tree (multiple root/leaf files) exists under metadata/.
      seedAndCaptureFiles(tableName, rounds = 5)

      val metadataBefore = metadataFilesOnDisk(deltaLog)
      assert(metadataBefore.nonEmpty, "expected AMT manifest files under metadata/ before vacuum")

      val deltaTable = DeltaTableV2(spark, TableIdentifier(tableName))
      withSQLConf(DeltaSQLConf.DELTA_VACUUM_RETENTION_CHECK_ENABLED.key -> "false") {
        VacuumCommand.gc(spark, deltaTable, retentionHours = Some(0), dryRun = false)
      }

      // VACUUM may add a manifest as part of its own commit, but it must delete none: every
      // manifest file present before must still be present after. (The regression this guards
      // against deleted the live root, which would then be missing here.)
      assert(metadataBefore.subsetOf(metadataFilesOnDisk(deltaLog)),
        "VACUUM must not delete any AMT manifest file under metadata/")
    }
  }
}

/** Runs [[AMTVacuumTests]] with AMT manifests deferred to follow-up checkpoint commits. */
class AMTVacuumSuite extends AMTVacuumTests

/**
 * Runs [[AMTVacuumTests]] with every commit writing its AMT manifest tree inline
 * (amt.largeCommitActionsCountThresholdForInlineManifestCommit = 1), so the removes ride in the
 * same commit as the manifest rather than in a deferred checkpoint commit.
 */
class AMTVacuumInlineManifestSuite extends AMTVacuumTests {
  override protected def sparkConf: SparkConf =
    super.sparkConf.set(
      DeltaSQLConf.AMT_LARGE_COMMIT_ACTIONS_COUNT_THRESHOLD_FOR_INLINE_MANIFEST_COMMIT.key, "1")
}
