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

import com.databricks.spark.util.Log4jUsageLogger
import org.apache.spark.sql.delta.CheckpointInstance.Format
import org.apache.spark.sql.delta.DeltaTestUtils.BOOLEAN_DOMAIN
import org.apache.spark.sql.delta.coordinatedcommits.CoordinatedCommitsBaseSuite
import org.apache.spark.sql.delta.storage.LocalLogStore
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

class FindLastCompleteCheckpointSuite
  extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest
  with CoordinatedCommitsBaseSuite {

  protected override def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.delta.logStore.class", classOf[CustomListingLogStore].getName)
  }

  private def pathToFileStatus(path: Path, length: Long = 20): FileStatus = {
    SerializableFileStatus(path.toString, length, isDir = true, modificationTime = 0L).toFileStatus
  }

  private def commitFiles(logPath: Path, versions: Seq[Long]): Seq[FileStatus] = {
    versions.map { version => pathToFileStatus(FileNames.unsafeDeltaFile(logPath, version)) }
  }

  private def singleCheckpointFiles(
      logPath: Path, versions: Seq[Long],
      length: Long = 20): Seq[FileStatus] = {
    versions.map { v => pathToFileStatus(FileNames.checkpointFileSingular(logPath, v), length) }
  }

  private def multipartCheckpointFiles(
      logPath: Path,
      versions: Seq[Long],
      numParts: Int,
      length: Long = 20): Seq[FileStatus] = {
    versions.flatMap { version =>
      FileNames.checkpointFileWithParts(logPath, version, numParts).map(pathToFileStatus(_, length))
    }
  }

  private def checksumFiles(logPath: Path, versions: Seq[Long]): Seq[FileStatus] = {
    versions.map { version => pathToFileStatus(FileNames.checksumFile(logPath, version)) }
  }

  def getLastCompleteCheckpointUsageLog(f: => Unit): Map[String, String] = {
    val usageRecords = Log4jUsageLogger.track {
      f
    }
    val opType = "delta.findLastCompleteCheckpointBefore"
    val records = usageRecords.filter { r =>
      r.tags.get("opType").contains(opType) || r.opType.map(_.typeName).contains(opType)
    }
    assert(records.size === 1)
    JsonUtils.fromJson[Map[String, String]](records.head.blob)
  }

  test("findLastCompleteCheckpoint without any argument") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(spark, dir.getAbsolutePath)
      val logPath = log.logPath
      val logStore = log.store.asInstanceOf[CustomListingLogStore]

      // Case-1: Multiple checkpoint exists in table dir
      logStore.customListingResult = Some(
        commitFiles(logPath, 0L to 3000) ++
          singleCheckpointFiles(logPath, Seq(100, 200, 1000, 2000))
      )
      val eventData1 = getLastCompleteCheckpointUsageLog {
        assert(log.findLastCompleteCheckpointBefore().contains(CheckpointInstance(version = 2000)))
      }
      assert(!eventData1.contains("iterations"))
      assert(logStore.listFromCount == 1)
      assert(logStore.elementsConsumedFromListFromIter == 3005)
      logStore.reset()

      // Case-2: No checkpoint exists in table dir
      logStore.customListingResult = Some(commitFiles(logPath, 0L to 3000))
      val eventData2 = getLastCompleteCheckpointUsageLog {
        assert(log.findLastCompleteCheckpointBefore().isEmpty)
      }
      assert(!eventData2.contains("iterations"))
      assert(logStore.listFromCount == 1)
      assert(logStore.elementsConsumedFromListFromIter == 3001)
      logStore.reset()

      // Case-3: Multiple checkpoints for same version exists in table dir
      logStore.customListingResult = Some(
        commitFiles(logPath, 0L to 3000) ++
          singleCheckpointFiles(logPath, Seq(100, 200, 1000, 2000)) ++
          multipartCheckpointFiles(logPath, Seq(300, 2000), numParts = 4)
      )
      val eventData3 = getLastCompleteCheckpointUsageLog {
        assert(log.findLastCompleteCheckpointBefore().contains(
          CheckpointInstance(version = 2000, Format.WITH_PARTS, numParts = Some(4))))
      }
      assert(!eventData2.contains("iterations"))
      assert(logStore.listFromCount == 1)
      assert(logStore.elementsConsumedFromListFromIter == 3013)
      logStore.reset()
    }
  }

  test("findLastCompleteCheckpoint with an upperBound which exists") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(spark, dir.getAbsolutePath)
      val logPath = log.logPath
      val logStore = log.store.asInstanceOf[CustomListingLogStore]
      logStore.reset()

      // Case-1: The upperBound exists and it should not be returned
      logStore.customListingResult = Some(
        commitFiles(logPath, 0L to 3000) ++
          singleCheckpointFiles(logPath, Seq(100, 200, 1000, 2000))
      )
      val eventData1 = getLastCompleteCheckpointUsageLog {
        assert(
          log.findLastCompleteCheckpointBefore(Some(CheckpointInstance(version = 2000)))
            .contains(CheckpointInstance(version = 1000)))
      }
      assert(logStore.listFromCount == 1)
      assert(logStore.elementsConsumedFromListFromIter == 1002 + 2) // commits + checkpoint
      assert(eventData1("iterations") == "1")
      assert(eventData1("numFilesScanned") == "1004")
      logStore.reset()

      // Case-2: The exact upperBound (a multi-part checkpoint) doesn't exist but another single
      // part checkpoint for same version exists.
      logStore.customListingResult = Some(
        commitFiles(logPath, 0L to 3000) ++
          singleCheckpointFiles(logPath, Seq(100, 200, 1000, 2000))
      )
      var sentinelCheckpoint =
        CheckpointInstance(version = 2000, Format.WITH_PARTS, numParts = Some(4))
      val eventData2 = getLastCompleteCheckpointUsageLog {
        assert(log.findLastCompleteCheckpointBefore(Some(sentinelCheckpoint))
          .contains(CheckpointInstance(version = 2000)))
      }
      assert(logStore.listFromCount == 1)
      assert(logStore.elementsConsumedFromListFromIter == 1002 + 2) // commits + checkpoint
      assert(eventData2("iterations") == "1")
      assert(eventData2("numFilesScanned") == "1004")
      logStore.reset()

      // Case-3: The last complete checkpoint doesn't exist in last 1000 elements and needs
      // multiple iterations.
      logStore.customListingResult = Some(
        commitFiles(logPath, 0L to 2500) ++
          singleCheckpointFiles(logPath, Seq(100, 150))
      )
      val eventData3 = getLastCompleteCheckpointUsageLog {
        assert(
          log.findLastCompleteCheckpointBefore(2200).contains(CheckpointInstance(version = 150)))
      }
      assert(logStore.listFromCount == 3)
      // the first listing will consume 1000 elements from 1200 to 2201 => 1002 commits
      // the second listing will consume 1000 elements from 200 to 1201 => 1002 commits
      // the third listing will consume 501 elements from 0 to 201 => 202 commits + 2 checkpoints
      assert(logStore.elementsConsumedFromListFromIter == 2208) // commits + checkpoint
      assert(eventData3("iterations") == "3")
      assert(eventData3("numFilesScanned") == "2208")
      logStore.reset()
    }
  }

  for (passSentinelInstance <- BOOLEAN_DOMAIN)
  test("findLastCompleteCheckpoint ignores 0B files " +
      s"[passSentinelInstance: $passSentinelInstance]") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(spark, dir.getAbsolutePath)
      val logPath = log.logPath
      val logStore = log.store.asInstanceOf[CustomListingLogStore]
      logStore.reset()

      val lastCommitVersion = 1400
      val sentinelInstance =
        if (passSentinelInstance) Some(CheckpointInstance(version = 1200)) else None
      val expectedListCount = if (passSentinelInstance) 2 else 1
      def getExpectedFileCount(filesPerCheckpoint: Int): Int = {
        if (passSentinelInstance) {
          // commits and checkpoints from 200 to 1201 => 1002 + 1-checkpoint
          // commits and checkpoints from 0 to 201 => 202 + 2-checkpoint
          1204 + 3 * filesPerCheckpoint
        } else {
          val totalCommits = lastCommitVersion + 1 // commit starts from 0
          totalCommits + 2 * filesPerCheckpoint
        }
      }

      // Case-1: `findLastCompleteCheckpointBefore` invoked without upperBound, with 0B single part
      // checkpoint.
      logStore.customListingResult = Some(
        commitFiles(logPath, 0L to lastCommitVersion) ++
        singleCheckpointFiles(logPath, Seq(100), length = 20) ++
        singleCheckpointFiles(logPath, Seq(200), length = 0))
      val eventData1 = getLastCompleteCheckpointUsageLog {
        assert(
          log.findLastCompleteCheckpointBefore(sentinelInstance)
            .contains(CheckpointInstance(version = 100)))
      }
      assert(logStore.listFromCount == expectedListCount)
      assert(logStore.elementsConsumedFromListFromIter ===
        getExpectedFileCount(filesPerCheckpoint = 1))
      if (passSentinelInstance) {
        assert(eventData1("iterations") == expectedListCount.toString)
        assert(eventData1("numFilesScanned") ==
          getExpectedFileCount(filesPerCheckpoint = 1).toString)
      } else {
        assert(Seq("iterations", "numFilesScanned").forall(!eventData1.contains(_)))
      }

      logStore.reset()

      // Case-2: `findLastCompleteCheckpointBefore` invoked with upperBound, with a multi-part
      // checkpoint having one of the part as 0B.
      val badCheckpointV200 = {
        val checkpointV200 = multipartCheckpointFiles(logPath, Seq(200), numParts = 4)
        SerializableFileStatus.fromStatus(checkpointV200.head).copy(length = 0).toFileStatus +:
          checkpointV200.tail
      }
      logStore.customListingResult = Some(
        commitFiles(logPath, 0L to lastCommitVersion) ++
        multipartCheckpointFiles(logPath, Seq(100), numParts = 4) ++
        badCheckpointV200
      )
      val eventData2 = getLastCompleteCheckpointUsageLog {
        assert(log.findLastCompleteCheckpointBefore(sentinelInstance)
          .contains(CheckpointInstance(version = 100, Format.WITH_PARTS, numParts = Some(4))))
      }
      if (passSentinelInstance) {
        assert(eventData2("iterations") == expectedListCount.toString)
        assert(eventData2("numFilesScanned") ==
          getExpectedFileCount(filesPerCheckpoint = 4).toString)
      } else {
        assert(Seq("iterations", "numFilesScanned").forall(!eventData2.contains(_)))
      }
      assert(logStore.listFromCount == expectedListCount)
      assert(logStore.elementsConsumedFromListFromIter ===
        getExpectedFileCount(filesPerCheckpoint = 4))
      logStore.reset()
    }
  }

  for (passSentinelInstance <- BOOLEAN_DOMAIN)
  test("findLastCompleteCheckpoint ignores incomplete multi-part checkpoint " +
      s"[passSentinelInstance: $passSentinelInstance]") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(spark, dir.getAbsolutePath)
      val logPath = log.logPath
      val logStore = log.store.asInstanceOf[CustomListingLogStore]
      logStore.reset()

      val lastCommitVersion = 1400
      val sentinelInstance =
        if (passSentinelInstance) Some(CheckpointInstance(version = 1200)) else None
      val expectedListCount = if (passSentinelInstance) 2 else 1

      def getExpectedFileCount(fileInCheckpointV200: Int, filesInCheckpointV100: Int): Int = {
        if (passSentinelInstance) {
          // commits and checkpoints from 200 to 1201 => 1002 + 1-checkpoint
          // commits and checkpoints from 0 to 201 => 202 + 2-checkpoint
          1204 + (fileInCheckpointV200 + fileInCheckpointV200 + filesInCheckpointV100)
        } else {
          val totalCommits = lastCommitVersion + 1 // commit starts from 0
          totalCommits + (fileInCheckpointV200 + filesInCheckpointV100)
        }
      }

      // Case-1: `findLastCompleteCheckpointBefore` invoked, with 0B single part checkpoint.
      logStore.customListingResult = Some(
        commitFiles(logPath, 0L to lastCommitVersion) ++
          multipartCheckpointFiles(logPath, Seq(100), numParts = 4, length = 20) ++
          multipartCheckpointFiles(logPath, Seq(200), numParts = 4, length = 20).take(3))
      val eventData1 = getLastCompleteCheckpointUsageLog {
        assert(
          log.findLastCompleteCheckpointBefore(sentinelInstance)
            .contains(CheckpointInstance(100, Format.WITH_PARTS, numParts = Some(4))))
      }
      assert(logStore.listFromCount == expectedListCount)
      assert(logStore.elementsConsumedFromListFromIter ===
        getExpectedFileCount(fileInCheckpointV200 = 3, filesInCheckpointV100 = 4))
      if (passSentinelInstance) {
        assert(eventData1("iterations") == expectedListCount.toString)
        assert(eventData1("numFilesScanned") ==
          getExpectedFileCount(fileInCheckpointV200 = 3, filesInCheckpointV100 = 4).toString)
      } else {
        assert(Seq("iterations", "numFilesScanned").forall(!eventData1.contains(_)))
      }

      logStore.reset()
    }
  }

  test("findLastCompleteCheckpoint with CheckpointInstance.MAX value") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(spark, dir.getAbsolutePath)
      val logPath = log.logPath
      val logStore = log.store.asInstanceOf[CustomListingLogStore]
      logStore.reset()

      logStore.customListingResult = Some(
        commitFiles(logPath, 0L to 3000) ++
          singleCheckpointFiles(logPath, Seq(100, 200, 1000, 1200))
      )
      val eventData = getLastCompleteCheckpointUsageLog {
        assert(
          log.findLastCompleteCheckpointBefore(Some(CheckpointInstance.MaxValue))
            .contains(CheckpointInstance(version = 1200)))
      }
      assert(!eventData.contains("iterations"))
      assert(!eventData.contains("upperBoundVersion"))
      assert(eventData("totalTimeTakenMs").toLong > 0)
      assert(logStore.listFromCount == 1)
      assert(logStore.elementsConsumedFromListFromIter == 3001 + 4) // commits + checkpoint
      logStore.reset()
    }
  }
}

/**
 * A custom log store that allows to provide custom listing results. This is useful to test
 * `DeltaLog.findLastCompleteCheckpointBefore` method.
 */
class CustomListingLogStore(
  sparkConf: SparkConf,
  hadoopConf: Configuration) extends LocalLogStore(sparkConf, hadoopConf) {

  var listFromCount = 0
  var elementsConsumedFromListFromIter = 0
  // The custom listing result that will be returned by `listFrom` method. If this is None, then
  // the default listing result from the actual filesystem will be returned.
  var customListingResult: Option[Seq[FileStatus]] = None

  override def listFrom(path: Path, hadoopConf: Configuration): Iterator[FileStatus] = {
    customListingResult.map { results =>
      listFromCount += 1
      results
        .sortBy(_.getPath)
        .dropWhile(_.getPath.toString < path.toString)
        .toIterator
        .map { file =>
          elementsConsumedFromListFromIter += 1
          file
        }
    }.getOrElse(super.listFrom(path, hadoopConf))
  }

  def reset(): Unit = {
    listFromCount = 0
    elementsConsumedFromListFromIter = 0
    customListingResult = None
  }
}
