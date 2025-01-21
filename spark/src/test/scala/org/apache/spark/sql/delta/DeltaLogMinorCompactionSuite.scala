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

import org.apache.spark.sql.delta.DeltaOperations.ManualUpdate
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.coordinatedcommits.CoordinatedCommitsBaseSuite
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaSQLTestUtils
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.{DeltaCommitFileProvider, FileNames}
import org.apache.hadoop.fs.Path

import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.test.SharedSparkSession

// scalastyle:off: removeFile
class DeltaLogMinorCompactionSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest
  with DeltaSQLTestUtils
  with CoordinatedCommitsBaseSuite {

  /** Helper method to do minor compaction of [[DeltaLog]] from [startVersion, endVersion] */
  private def minorCompactDeltaLog(
      tablePath: String,
      startVersion: Long,
      endVersion: Long): Unit = {
    val deltaLog = DeltaLog.forTable(spark, tablePath)
    deltaLog.update().tableCommitCoordinatorClientOpt.foreach { tableCommitCoordinatorClient =>
      tableCommitCoordinatorClient.backfillToVersion(endVersion)
    }
    val logReplay = new InMemoryLogReplay(
      minFileRetentionTimestamp = 0,
      minSetTransactionRetentionTimestamp = None)
    val hadoopConf = deltaLog.newDeltaHadoopConf()

    (startVersion to endVersion).foreach { versionToRead =>
      val file = FileNames.unsafeDeltaFile(deltaLog.logPath, versionToRead)
      val actionsIterator = deltaLog.store.readAsIterator(file, hadoopConf).map(Action.fromJson)
      logReplay.append(versionToRead, actionsIterator)
    }
    deltaLog.store.write(
      path = FileNames.compactedDeltaFile(deltaLog.logPath, startVersion, endVersion),
      actions = logReplay.checkpoint.map(_.json).toIterator,
      overwrite = true,
      hadoopConf = hadoopConf)
  }

  // Helper method to validate a commit.
  protected def validateCommit(
      log: DeltaLog,
      version: Long,
      numAdds: Int = 0,
      numRemoves: Int = 0,
      numMetadata: Int = 0): Unit = {
    assert(log.update().version === version)
    val filePath = DeltaCommitFileProvider(log.update()).deltaFile(version)
    val actions = log.store.read(filePath, log.newDeltaHadoopConf()).map(Action.fromJson)
    assert(actions.head.isInstanceOf[CommitInfo])
    assert(actions.tail.count(_.isInstanceOf[AddFile]) === numAdds)
    assert(actions.tail.count(_.isInstanceOf[RemoveFile]) === numRemoves)
    assert(actions.tail.count(_.isInstanceOf[Metadata]) === numMetadata)
  }

  // Helper method to validate a compacted delta.
  private def validateCompactedDelta(
      log: DeltaLog,
      filePath: Path,
      expectedCompactedDelta: CompactedDelta): Unit = {
    val actions = log.store.read(filePath, log.newDeltaHadoopConf()).map(Action.fromJson)
    val observedCompactedDelta = CompactedDelta(
      versionWindow = FileNames.compactedDeltaVersions(filePath),
      numAdds = actions.count(_.isInstanceOf[AddFile]),
      numRemoves = actions.count(_.isInstanceOf[RemoveFile]),
      numMetadata = actions.count(_.isInstanceOf[Metadata])
    )
    assert(expectedCompactedDelta === observedCompactedDelta)
  }


  case class CompactedDelta(
      versionWindow: (Long, Long),
      numAdds: Int = 0,
      numRemoves: Int = 0,
      numMetadata: Int = 0)

  def createTestAddFile(
      path: String = "foo",
      partitionValues: Map[String, String] = Map.empty,
      size: Long = 1L,
      modificationTime: Long = 1L,
      dataChange: Boolean = true,
      stats: String = "{\"numRecords\": 1}"): AddFile = {
    AddFile(path, partitionValues, size, modificationTime, dataChange, stats)
  }

  def generateData(tableDir: String, checkpoints: Set[Int]): Unit = {
    val files = (1 to 21).map( index => createTestAddFile(s"f${index}"))
    // commit version 0 - AddFile: 4
    val deltaLog = DeltaLog.forTable(spark, tableDir)
    import org.apache.spark.sql.delta.test.DeltaTestImplicits._
    val metadata = Metadata()
    val tableMetadata = metadata.copy(
      configuration = DeltaConfigs.mergeGlobalConfigs(conf, metadata.configuration))
    deltaLog.startTransaction().commitManually(
      files(1), files(2), files(3), files(4), tableMetadata)
    validateCommit(deltaLog, version = 0, numAdds = 4, numRemoves = 0, numMetadata = 1)
    if (checkpoints.contains(0)) deltaLog.checkpoint()
    // commit version 1 - AddFile: 1
    deltaLog.startTransaction().commit(files(5) :: Nil, ManualUpdate)
    validateCommit(deltaLog, version = 1, numAdds = 1, numRemoves = 0)
    if (checkpoints.contains(1)) deltaLog.checkpoint()
    // commit version 2 - RemoveFile: 1, AddFile: 1
    deltaLog.startTransaction().commit(Seq(files(5).remove, files(6)), ManualUpdate)
    validateCommit(deltaLog, version = 2, numAdds = 1, numRemoves = 1)
    if (checkpoints.contains(2)) deltaLog.checkpoint()
    // commit version 3 - empty commit
    deltaLog.startTransaction().commit(Seq(), ManualUpdate)
    validateCommit(deltaLog, version = 3, numAdds = 0, numRemoves = 0)
    if (checkpoints.contains(3)) deltaLog.checkpoint()
    // commit version 4 - empty commit
    deltaLog.startTransaction().commit(Seq(), ManualUpdate)
    validateCommit(deltaLog, version = 4, numAdds = 0, numRemoves = 0)
    if (checkpoints.contains(4)) deltaLog.checkpoint()
    // commit version 5 - AddFile: 1, RemoveFile: 5
    deltaLog.startTransaction().commit(
      (1 to 4).map(i => files(i).remove) ++ Seq(files(6).remove, files(7)),
      ManualUpdate)
    validateCommit(deltaLog, version = 5, numAdds = 1, numRemoves = 5)
    if (checkpoints.contains(5)) deltaLog.checkpoint()
    // commit version 6 - AddFile: 10, RemoveFile: 0
    deltaLog.startTransaction().commit((8 to 17).map(i => files(i)), ManualUpdate)
    validateCommit(deltaLog, version = 6, numAdds = 10, numRemoves = 0)
    if (checkpoints.contains(6)) deltaLog.checkpoint()
    // commit version 7 - AddFile: 2, RemoveFile: 6
    deltaLog.startTransaction().commit(
      (10 to 15).map(i => files(i).remove) ++ Seq(files(18), files(19)),
      ManualUpdate)
    validateCommit(deltaLog, version = 7, numAdds = 2, numRemoves = 6)
    if (checkpoints.contains(7)) deltaLog.checkpoint()
    // commit version 8 - Metadata: 1
    deltaLog.startTransaction().commit(Seq(deltaLog.unsafeVolatileSnapshot.metadata), ManualUpdate)
    validateCommit(deltaLog, version = 8, numMetadata = 1)
    if (checkpoints.contains(8)) deltaLog.checkpoint()
    // commit version 9 - AddFile: 7
    deltaLog.startTransaction().commit(
      Seq(files(16), files(17), files(18), files(19), files(7), files(8), files(9))
        .map(af => af.copy(dataChange = false)),
      ManualUpdate)
    validateCommit(deltaLog, version = 9, numAdds = 7)
    if (checkpoints.contains(9)) deltaLog.checkpoint()
    // commit version 10 - AddFiles: 1
    deltaLog.startTransaction().commit(files(20) :: Nil, ManualUpdate)
    validateCommit(deltaLog, version = 10, numAdds = 1, numRemoves = 0)
  }

  /**
   * This test creates a Delta table with 11 commits (0, 1, ..., 10) and also creates compacted
   * deltas based on the provided `compactionRange` tuples.
   *
   * At the end, we create a Snapshot and see if the Snapshot is initialized properly using the
   * right compacted delta files instead of regular delta files. We also compare the
   * `computeState`, `stateDF`, `allFiles` of this compacted delta backed Snapshot against a
   * regular Snapshot backed by single delta files.
   */
  def testSnapshotCreation(
      compactionWindows: Seq[(Long, Long)],
      checkpoints: Set[Int] = Set.empty,
      postSetupFunc: Option[(DeltaLog => Unit)] = None,
      expectedCompactedDeltas: Seq[CompactedDelta],
      expectedDeltas: Seq[Long],
      expectedCheckpoint: Long = -1L,
      expectError: Boolean = false,
      additionalConfs: Seq[(String, String)] = Seq.empty): Unit = {

    val confs = Seq(
      DeltaSQLConf.DELTALOG_MINOR_COMPACTION_USE_FOR_READS.key -> "true",
      DeltaSQLConf.DELTA_SKIP_RECORDING_EMPTY_COMMITS.key -> "false",
      // Set CHECKPOINT_INTERVAL to high number so that we could checkpoint whenever we need as per
      // test setup.
      DeltaConfigs.CHECKPOINT_INTERVAL.defaultTablePropertyKey -> "1000"
    ) ++ additionalConfs

    withSQLConf(confs: _*) {
      withTempDir { tmpDir =>
        val tableDir = tmpDir.getAbsolutePath
        generateData(tableDir, checkpoints)
        val deltaLog = DeltaLog.forTable(spark, tableDir)
        compactionWindows.foreach { case (startV, endV) =>
          minorCompactDeltaLog(tableDir, startV, endV)
        }

        // Setup complete - run post setup function
        postSetupFunc.foreach(_.apply(deltaLog))

        DeltaLog.clearCache()
        if (expectError) {
          intercept[DeltaIllegalStateException] {
            DeltaLog.forTable(spark, tableDir).unsafeVolatileSnapshot
          }
          return
        }
        val snapshot1 = DeltaLog.forTable(spark, tableDir).unsafeVolatileSnapshot
        val (compactedDeltas1, deltas1) =
          snapshot1.logSegment.deltas.map(_.getPath).partition(FileNames.isCompactedDeltaFile)
        assert(compactedDeltas1.size === expectedCompactedDeltas.size)
        compactedDeltas1.sorted
          .zip(expectedCompactedDeltas.sortBy(_.versionWindow))
          .foreach { case (compactedDeltaPath, expectedCompactedDelta) =>
            validateCompactedDelta(deltaLog, compactedDeltaPath, expectedCompactedDelta)
          }
        assert(deltas1.sorted.map(FileNames.deltaVersion) === expectedDeltas)
        assert(snapshot1.logSegment.checkpointProvider.version === expectedCheckpoint)

        // Disable the conf and create a new Snapshot. The new snapshot should not use the comoacted
        // deltas.
        withSQLConf(DeltaSQLConf.DELTALOG_MINOR_COMPACTION_USE_FOR_READS.key -> "false") {
          DeltaLog.clearCache()
          val snapshot2 = DeltaLog.forTable(spark, tableDir).unsafeVolatileSnapshot
          val (compactedDeltas2, _) =
            snapshot2.logSegment.deltas.map(_.getPath).partition(FileNames.isCompactedDeltaFile)
          assert(compactedDeltas2.isEmpty)

          // Compare checksum, state reconstruction result of these 2 different snapshots.
          assert(snapshot2.computeChecksum === snapshot1.computeChecksum)
          checkAnswer(snapshot2.stateDF, snapshot1.stateDF)
          checkAnswer(snapshot2.allFiles.toDF(), snapshot1.allFiles.toDF())
        }
      }
    }
  }

  ///////////////////////
  // Without Checkpoints
  //////////////////////

  test("smallest interval is chosen first for Snapshot creation") {
    testSnapshotCreation(
      compactionWindows = Seq((1, 3), (2, 3), (3, 8)),
      expectedCompactedDeltas = Seq(CompactedDelta((1, 3), numAdds = 1, numRemoves = 1)),
      expectedDeltas = Seq(0, 4, 5, 6, 7, 8, 9, 10)
    )
  }

  test("Snapshot backed by single compacted delta") {
    testSnapshotCreation(
      compactionWindows = Seq((0, 10)),
      expectedCompactedDeltas =
        Seq(CompactedDelta((0, 10), numAdds = 8, numRemoves = 12, numMetadata = 1)),
      expectedDeltas = Seq()
    )
  }

  test("empty compacted delta, compacted delta covers the beginning part") {
    testSnapshotCreation(
      compactionWindows = Seq((0, 2), (3, 4), (4, 5)),
      expectedCompactedDeltas = Seq(
        CompactedDelta((0, 2), numAdds = 5, numRemoves = 1, numMetadata = 1),
        CompactedDelta((3, 4), numAdds = 0, numRemoves = 0) // empty compacted delta
      ),
      expectedDeltas = Seq(5, 6, 7, 8, 9, 10)
    )
  }

  test("compacted delta covers the end part of LogSegment") {
    testSnapshotCreation(
      compactionWindows = Seq((7, 10), (8, 10)),
      expectedCompactedDeltas = Seq(
        CompactedDelta((7, 10), numAdds = 8, numRemoves = 6, numMetadata = 1)
      ),
      expectedDeltas = Seq(0, 1, 2, 3, 4, 5, 6)
    )
  }

  test("multiple compacted delta covers full LogSegment") {
    testSnapshotCreation(
      compactionWindows = Seq((0, 2), (3, 5), (6, 8), (9, 10)),
      expectedCompactedDeltas = Seq(
        CompactedDelta((0, 2), numAdds = 5, numRemoves = 1, numMetadata = 1),
        CompactedDelta((3, 5), numAdds = 1, numRemoves = 5, numMetadata = 0),
        CompactedDelta((6, 8), numAdds = 6, numRemoves = 6, numMetadata = 1),
        CompactedDelta((9, 10), numAdds = 8, numRemoves = 0, numMetadata = 0)
      ),
      expectedDeltas = Seq()
    )
  }


  ///////////////////////
  // With Checkpoints
  //////////////////////

  test("smallest interval after last checkpoint is chosen for Snapshot creation") {
    testSnapshotCreation(
      compactionWindows = Seq((1, 3), (2, 3), (3, 8), (4, 9), (3, 10)),
      checkpoints = Set(0, 2),
      expectedCompactedDeltas =
        Seq(CompactedDelta((3, 8), numAdds = 7, numRemoves = 11, numMetadata = 1)),
      expectedDeltas = Seq(9, 10),
      expectedCheckpoint = 2,
      // Disable DELTA_CHECKPOINT_V2_ENABLED conf so that we don't forcefully checkpoint at
      // commit version 8 where we change `delta.dataSkippingNumIndexedCols` to 0.
      additionalConfs =
        Seq(DeltaConfigs.CHECKPOINT_WRITE_STATS_AS_STRUCT.defaultTablePropertyKey -> "false")
    )
  }

  test("Snapshot backed by single compacted delta after LAST_CHECKPOINT") {
    testSnapshotCreation(
      compactionWindows = Seq((0, 10), (5, 10)),
      checkpoints = Set(2, 4),
      expectedCompactedDeltas =
        Seq(CompactedDelta((5, 10), numAdds = 8, numRemoves = 11, numMetadata = 1)),
      expectedDeltas = Seq(),
      expectedCheckpoint = 4,
      // Disable DELTA_CHECKPOINT_V2_ENABLED conf so that we don't forcefully checkpoint at
      // commit version 8 where we change `delta.dataSkippingNumIndexedCols` to 0.
      additionalConfs =
        Seq(DeltaConfigs.CHECKPOINT_WRITE_STATS_AS_STRUCT.defaultTablePropertyKey -> "false")

    )
  }

  test("empty compacted delta, compacted delta covers the beginning part after LAST_CHECKPOINT") {
    testSnapshotCreation(
      compactionWindows = Seq((1, 2), (3, 4), (4, 5)),
      checkpoints = Set(0),
      expectedCompactedDeltas = Seq(
        CompactedDelta((1, 2), numAdds = 1, numRemoves = 1),
        CompactedDelta((3, 4), numAdds = 0, numRemoves = 0) // empty compacted delta
      ),
      expectedDeltas = Seq(5, 6, 7, 8, 9, 10),
      expectedCheckpoint = 0,
      // Disable DELTA_CHECKPOINT_V2_ENABLED conf so that we don't forcefully checkpoint at
      // commit version 8 where we change `delta.dataSkippingNumIndexedCols` to 0.
      additionalConfs =
        Seq(DeltaConfigs.CHECKPOINT_WRITE_STATS_AS_STRUCT.defaultTablePropertyKey -> "false")
    )
  }

  test("compacted delta covers the end part of LogSegment (with Checkpoint)") {
    testSnapshotCreation(
      compactionWindows = Seq((7, 10), (8, 10)),
      checkpoints = Set(0, 2, 5),
      expectedCompactedDeltas = Seq(
        CompactedDelta((7, 10), numAdds = 8, numRemoves = 6, numMetadata = 1)
      ),
      expectedDeltas = Seq(6),
      expectedCheckpoint = 5,
      // Disable DELTA_CHECKPOINT_V2_ENABLED conf so that we don't forcefully checkpoint at
      // commit version 8 where we change `delta.dataSkippingNumIndexedCols` to 0.
      additionalConfs =
        Seq(DeltaConfigs.CHECKPOINT_WRITE_STATS_AS_STRUCT.defaultTablePropertyKey -> "false")
    )
  }

  test("multiple compacted delta covers full LogSegment (with Checkpoint)") {
    testSnapshotCreation(
      compactionWindows = Seq((0, 2), (3, 5), (3, 6), (9, 10)),
      checkpoints = Set(0, 2),
      expectedCompactedDeltas = Seq(
        CompactedDelta((3, 5), numAdds = 1, numRemoves = 5, numMetadata = 0),
        CompactedDelta((9, 10), numAdds = 8, numRemoves = 0, numMetadata = 0)
      ),
      expectedDeltas = Seq(6, 7, 8),
      expectedCheckpoint = 2,
      // Disable DELTA_CHECKPOINT_V2_ENABLED conf so that we don't forcefully checkpoint at
      // commit version 8 where we change `delta.dataSkippingNumIndexedCols` to 0.
      additionalConfs =
        Seq(DeltaConfigs.CHECKPOINT_WRITE_STATS_AS_STRUCT.defaultTablePropertyKey -> "false")
    )
  }

  /////////////////////////////////////////////////////
  // negative scenarios where deltaLog is manipulated
  /////////////////////////////////////////////////////

  test("when compacted delta is available till version 11 but actual delta files are" +
      " till version 10") {
    testSnapshotCreation(
      compactionWindows = Seq((0, 2), (3, 5), (3, 6), (9, 10)),
      checkpoints = Set(0, 2),
      postSetupFunc = Some(
        (deltaLog: DeltaLog) => {
          val logPath = deltaLog.logPath
          val fromName = FileNames.compactedDeltaFile(logPath, fromVersion = 9, toVersion = 10)
          val toName = FileNames.compactedDeltaFile(logPath, fromVersion = 9, toVersion = 11)
          logPath.getFileSystem(deltaLog.newDeltaHadoopConf()).rename(fromName, toName)
        }
      ),
      expectedCompactedDeltas = Seq(
        CompactedDelta((3, 5), numAdds = 1, numRemoves = 5, numMetadata = 0)
      ),
      expectedDeltas = Seq(6, 7, 8, 9, 10),
      expectedCheckpoint = 2,
      // Disable DELTA_CHECKPOINT_V2_ENABLED conf so that we don't forcefully checkpoint at
      // commit version 8 where we change `delta.dataSkippingNumIndexedCols` to 0.
      additionalConfs =
        Seq(DeltaConfigs.CHECKPOINT_WRITE_STATS_AS_STRUCT.defaultTablePropertyKey -> "false")
    )
  }


  test("compacted deltas should not be used when there are holes in deltas") {
    testSnapshotCreation(
      compactionWindows = Seq((0, 2), (3, 5), (3, 6)),
      checkpoints = Set(0, 2),
      postSetupFunc = Some(
        (deltaLog: DeltaLog) => {
          val logPath = deltaLog.logPath
          val deltaFileToDelete = FileNames.unsafeDeltaFile(logPath, version = 4)
          logPath.getFileSystem(deltaLog.newDeltaHadoopConf()).delete(deltaFileToDelete, true)
        }
      ),
      expectError = true,
      expectedCompactedDeltas = Seq(),
      expectedDeltas = Seq(),
      expectedCheckpoint = -1L,
      // Disable DELTA_CHECKPOINT_V2_ENABLED conf so that we don't forcefully checkpoint at
      // commit version 8 where we change `delta.dataSkippingNumIndexedCols` to 0.
      additionalConfs =
        Seq(DeltaConfigs.CHECKPOINT_WRITE_STATS_AS_STRUCT.defaultTablePropertyKey -> "false")
    )
  }
}

class DeltaLogMinorCompactionWithCoordinatedCommitsBatch1Suite
  extends DeltaLogMinorCompactionSuite {
  override val coordinatedCommitsBackfillBatchSize: Option[Int] = Some(1)
}

class DeltaLogMinorCompactionWithCoordinatedCommitsBatch2Suite
  extends DeltaLogMinorCompactionSuite {
  override val coordinatedCommitsBackfillBatchSize: Option[Int] = Some(2)
}

class DeltaLogMinorCompactionWithCoordinatedCommitsBatch100Suite
    extends DeltaLogMinorCompactionSuite {
  override val coordinatedCommitsBackfillBatchSize: Option[Int] = Some(100)
}
