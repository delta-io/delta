/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.test

import org.apache.spark.sql.delta.{CheckpointProtectionTableFeature, DeletionVectorsTableFeature, DeltaConfigs, DeltaLog, DeltaRuntimeException, DeltaSourceDeletionVectorsSuite}
import org.apache.spark.sql.delta.sources.{DeltaSourceMetadataTrackingLog, DeltaSQLConf, PersistedMetadata}
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.Row

/**
 * Test suite that runs DeltaSourceDeletionVectorsSuite using the V2 connector
 * (V2_ENABLE_MODE=STRICT).
 */
class DeltaV2SourceDeletionVectorsSuite
  extends DeltaSourceDeletionVectorsSuite with V2ForceTest {

  override protected def useDsv2: Boolean = true

  override protected def executeDml(sqlText: String): Unit = executeInV1Mode(sqlText)

  import testImplicits._

  private def createTableWithoutDeletionVectors(path: String): Unit = {
    Seq(1, 2, 3).toDF("value").coalesce(1).write.format("delta")
      .option(DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key, "false").save(path)
  }

  private def enableDeletionVectorsForTable(path: String): Unit = {
    executeDml(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES " +
      "('delta.enableDeletionVectors' = 'true')")
  }

  private def commitDeletionVector(path: String, deletedValue: Int = 1): Unit = {
    executeDml(s"DELETE FROM delta.`$path` WHERE value = $deletedValue")
    val deltaLog = DeltaLog.forTable(spark, path)
    assert(getFilesWithDeletionVectors(deltaLog).nonEmpty)
  }

  private def fastDropDeletionVectors(path: String): Unit = {
    withSQLConf(
        DeltaSQLConf.FAST_DROP_FEATURE_ENABLED.key -> "true",
        DeltaSQLConf.FAST_DROP_FEATURE_GENERATE_DV_TOMBSTONES.key -> "true") {
      executeDml(s"ALTER TABLE delta.`$path` DROP FEATURE deletionVectors")
    }
    val deltaLog = DeltaLog.forTable(spark, path)
    val snapshot = deltaLog.update()
    assert(!snapshot.protocol.isFeatureSupported(DeletionVectorsTableFeature))
    assert(snapshot.protocol.isFeatureSupported(CheckpointProtectionTableFeature))
    assert(getFilesWithDeletionVectors(deltaLog).isEmpty)
  }

  private def commitDeletionVectorAndDropFeature(path: String): Unit = {
    commitDeletionVector(path)
    fastDropDeletionVectors(path)
  }

  private def commitDeletionVectorAppendAndDropFeature(path: String, deletedValue: Int): Unit = {
    commitDeletionVector(path, deletedValue)
    // With maxFilesPerTrigger=1, this append separates the DV commit from the drop.
    Seq(4).toDF("value").write.format("delta").mode("append").save(path)
    fastDropDeletionVectors(path)
  }

  private def expectStreamFailure(errorClass: String): StreamAction = {
    ExpectFailure[DeltaRuntimeException] { error =>
      assert(error.asInstanceOf[DeltaRuntimeException].getErrorClass == errorClass)
    }
  }

  private def checkRowsAfterFailure(expectedValues: Int*): StreamAction = {
    CheckAnswer { (rows: Seq[Row]) =>
      assert(rows.map(_.getInt(0)).sorted == expectedValues.sorted)
      ()
    }
  }

  test(
    "historical deletion vectors after a committed DV batch recover with new tracking") {
    withTempDirs { (inputDir, checkpointDir, _) =>
      val path = inputDir.getCanonicalPath
      val checkpoint = checkpointDir.getCanonicalPath
      val schemaLocation = new Path(checkpoint, "_schema_location").toString
      createTableWithoutDeletionVectors(path)
      enableDeletionVectorsForTable(path)
      commitDeletionVector(path)
      val options = Map("ignoreChanges" -> "true", "maxFilesPerTrigger" -> "1")

      // Commit a batch that reads DV files, without schema tracking.
      testStream(loadStreamWithOptions(path, options))(
        StartStream(checkpointLocation = checkpoint),
        ProcessAllAvailable(),
        CheckAnswer(2, 3),
        StopStream)

      commitDeletionVectorAppendAndDropFeature(path, deletedValue = 2)

      // The latest snapshot has dropped DVs, but the next unread commit still needs DV filtering.
      testStream(loadStreamWithOptions(path, options))(
        StartStream(checkpointLocation = checkpoint),
        expectStreamFailure("DELTA_STREAMING_DELETION_VECTORS_REQUIRE_SCHEMA_TRACKING"),
        checkRowsAfterFailure())

      val trackedOptions = options + ("schemaTrackingLocation" -> schemaLocation)

      // New tracking recovers the historical protocol and requires a restart.
      testStream(loadStreamWithOptions(path, trackedOptions))(
        StartStream(checkpointLocation = checkpoint),
        expectStreamFailure("DELTA_STREAMING_METADATA_EVOLUTION"),
        checkRowsAfterFailure())

      // Read the surviving row and the append, then stop at the property change disabling DVs.
      testStream(loadStreamWithOptions(path, trackedOptions))(
        StartStream(checkpointLocation = checkpoint),
        expectStreamFailure("DELTA_STREAMING_METADATA_EVOLUTION"),
        checkRowsAfterFailure(3, 4))

      // The next restart records the protocol drop before any post-drop data is read.
      testStream(loadStreamWithOptions(path, trackedOptions))(
        StartStream(checkpointLocation = checkpoint),
        expectStreamFailure("DELTA_STREAMING_METADATA_EVOLUTION"),
        checkRowsAfterFailure())

      Seq(5).toDF("value").write.format("delta").mode("append").save(path)
      testStream(loadStreamWithOptions(path, trackedOptions))(
        StartStream(checkpointLocation = checkpoint),
        ProcessAllAvailable(),
        CheckAnswer(5),
        StopStream)
    }
  }

  test(
    "historical deletion vectors fail before recording offsets and recover with new tracking") {
    withTempDirs { (inputDir, checkpointDir, _) =>
      val path = inputDir.getCanonicalPath
      val checkpoint = checkpointDir.getCanonicalPath
      val schemaLocation = new Path(checkpoint, "_schema_location").toString
      createTableWithoutDeletionVectors(path)
      val options = Map("ignoreChanges" -> "true")

      // Stop without tracking before activation.
      testStream(loadStreamWithOptions(path, options))(
        StartStream(checkpointLocation = checkpoint),
        ProcessAllAvailable(),
        CheckAnswer(1, 2, 3),
        StopStream)

      enableDeletionVectorsForTable(path)
      commitDeletionVectorAndDropFeature(path)

      // Reject the DV files before recording a batch across activation and drop.
      testStream(loadStreamWithOptions(path, options))(
        StartStream(checkpointLocation = checkpoint),
        expectStreamFailure("DELTA_STREAMING_DELETION_VECTORS_REQUIRE_SCHEMA_TRACKING"),
        AssertOnQuery { query =>
          assert(query.offsetLog.getLatestBatchId() == query.commitLog.getLatestBatchId())
          true
        },
        checkRowsAfterFailure())

      val trackedOptions = options + ("schemaTrackingLocation" -> schemaLocation)

      // Initialize tracking with the historical metadata; this first requires a restart.
      testStream(loadStreamWithOptions(path, trackedOptions))(
        StartStream(checkpointLocation = checkpoint),
        expectStreamFailure("DELTA_STREAMING_METADATA_EVOLUTION"),
        checkRowsAfterFailure())

      // With no pending batch, tracking can now stop at activation before it plans the DV files.
      testStream(loadStreamWithOptions(path, trackedOptions))(
        StartStream(checkpointLocation = checkpoint),
        expectStreamFailure("DELTA_STREAMING_METADATA_EVOLUTION"),
        checkRowsAfterFailure())

      // Read the surviving rows, then persist each change that disables and drops DVs.
      testStream(loadStreamWithOptions(path, trackedOptions))(
        StartStream(checkpointLocation = checkpoint),
        expectStreamFailure("DELTA_STREAMING_METADATA_EVOLUTION"),
        checkRowsAfterFailure(2, 3))
      testStream(loadStreamWithOptions(path, trackedOptions))(
        StartStream(checkpointLocation = checkpoint),
        expectStreamFailure("DELTA_STREAMING_METADATA_EVOLUTION"),
        checkRowsAfterFailure())

      Seq(4).toDF("value").write.format("delta").mode("append").save(path)
      testStream(loadStreamWithOptions(path, trackedOptions))(
        StartStream(checkpointLocation = checkpoint),
        ProcessAllAvailable(),
        CheckAnswer(4),
        StopStream)
    }
  }

  test("mid-stream deletion vectors skip change commits without restart") {
    withTempDir { inputDir =>
      val path = inputDir.getCanonicalPath
      createTableWithoutDeletionVectors(path)
      val deltaLog = DeltaLog.forTable(spark, path)
      assert(!deltaLog.update().protocol.isFeatureSupported(DeletionVectorsTableFeature))
      val stream = loadStreamWithOptions(path, Map("skipChangeCommits" -> "true"))

      testStream(stream)(
        AssertOnQuery { query =>
          query.processAllAvailable()
          true
        },
        CheckAnswer(1, 2, 3),
        Assert {
          enableDeletionVectorsForTable(path)
          executeDml(s"DELETE FROM delta.`$path` WHERE value = 1")
          assert(getFilesWithDeletionVectors(deltaLog).nonEmpty)
          Seq(4).toDF("value").write.format("delta").mode("append").save(path)
        },
        AssertOnQuery { query =>
          query.processAllAvailable()
          true
        },
        CheckAnswer(1, 2, 3, 4),
        StopStream)
    }
  }

  test("schema tracking persists mid-stream deletion vector protocol before restart") {
    withTempDirs { (inputDir, checkpointDir, _) =>
      val path = inputDir.getCanonicalPath
      val checkpoint = checkpointDir.getCanonicalPath
      val schemaLocation = new Path(checkpoint, "_schema_location").toString
      createTableWithoutDeletionVectors(path)
      val deltaLog = DeltaLog.forTable(spark, path)
      val initialSnapshot = deltaLog.update()
      assert(!initialSnapshot.protocol.isFeatureSupported(DeletionVectorsTableFeature))
      val originalFile = initialSnapshot.allFiles.collect().head.path
      var activationVersion = -1L
      val stream = loadStreamWithOptions(path, Map(
        "ignoreChanges" -> "true",
        "schemaTrackingLocation" -> schemaLocation))

      def trackedMetadata: PersistedMetadata =
        DeltaSourceMetadataTrackingLog.create(
          spark, schemaLocation, initialSnapshot.metadata.id, path, parameters = Map.empty)
          .getCurrentTrackedMetadata.get

      testStream(stream)(
        StartStream(checkpointLocation = checkpoint),
        ProcessAllAvailable(),
        CheckAnswer(1, 2, 3),
        Assert {
          assert(trackedMetadata.deltaCommitVersion == initialSnapshot.version)
          assert(trackedMetadata.protocol.contains(initialSnapshot.protocol))
          enableDeletionVectorsForTable(path)
          activationVersion = deltaLog.update().version
          executeDml(s"DELETE FROM delta.`$path` WHERE value = 1")
          val filesWithDVs = getFilesWithDeletionVectors(deltaLog)
          assert(filesWithDVs.size == 1)
          assert(filesWithDVs.head.path == originalFile)
          assert(filesWithDVs.head.deletionVector.cardinality == 1L)
          ()
        },
        ExpectFailure[DeltaRuntimeException] { error =>
          assert(error.asInstanceOf[DeltaRuntimeException].getErrorClass ==
            "DELTA_STREAMING_METADATA_EVOLUTION")
        },
        Assert {
          val metadata = trackedMetadata
          assert(metadata.deltaCommitVersion == activationVersion)
          assert(metadata.protocol.get.isFeatureSupported(DeletionVectorsTableFeature))
          assert(metadata.dataSchema == initialSnapshot.schema)
          Seq(4).toDF("value").write.format("delta").mode("append").save(path)
        },
        CheckAnswer { (rows: Seq[Row]) =>
          assert(rows.map(_.getInt(0)).sorted == Seq(1, 2, 3))
          ()
        })

      val restartedStream = loadStreamWithOptions(path, Map(
        "ignoreChanges" -> "true",
        "schemaTrackingLocation" -> schemaLocation))
      testStream(restartedStream)(
        StartStream(checkpointLocation = checkpoint),
        ProcessAllAvailable(),
        CheckAnswer(2, 3, 4),
        Assert {
          assert(trackedMetadata.deltaCommitVersion == activationVersion)
          ()
        },
        StopStream)
    }
  }

  for (option <- Seq("ignoreChanges", "ignoreFileDeletion"))
  test(s"mid-stream deletion vectors require restart with the same checkpoint ($option)") {
    withTempDirs { (inputDir, checkpointDir, _) =>
      val path = inputDir.getCanonicalPath
      createTableWithoutDeletionVectors(path)
      val deltaLog = DeltaLog.forTable(spark, path)
      val initialSnapshot = deltaLog.update()
      assert(!initialSnapshot.protocol.isFeatureSupported(DeletionVectorsTableFeature))
      val originalFile = initialSnapshot.allFiles.collect().head.path
      var committedBatchBeforeDelete: Option[Long] = None
      val stream = loadStreamWithOptions(path, Map(option -> "true"))

      testStream(stream)(
        StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
        ProcessAllAvailable(),
        CheckAnswer(1, 2, 3),
        Assert {
          enableDeletionVectorsForTable(path)
          assert(deltaLog.update().protocol.isFeatureSupported(DeletionVectorsTableFeature))
          ()
        },
        ProcessAllAvailable(),
        CheckAnswer(1, 2, 3),
        AssertOnQuery { query =>
          committedBatchBeforeDelete = query.commitLog.getLatestBatchId()
          true
        },
        Assert {
          executeDml(s"DELETE FROM delta.`$path` WHERE value = 1")
          val filesWithDVs = getFilesWithDeletionVectors(deltaLog)
          assert(filesWithDVs.size == 1)
          assert(filesWithDVs.head.path == originalFile)
          assert(filesWithDVs.head.deletionVector.cardinality == 1L)
          ()
        },
        ExpectFailure[DeltaRuntimeException] { error =>
          checkError(
            error.asInstanceOf[DeltaRuntimeException],
            "DELTA_STREAMING_DELETION_VECTORS_REQUIRE_RESTART",
            "55000",
            Map.empty[String, String])
        },
        AssertOnQuery { query =>
          assert(query.commitLog.getLatestBatchId() == committedBatchBeforeDelete)
          assert(query.offsetLog.getLatestBatchId() == committedBatchBeforeDelete)
          true
        },
        CheckAnswer { (rows: Seq[Row]) =>
          assert(rows.map(_.getInt(0)).sorted == Seq(1, 2, 3))
          ()
        })

      Seq(4).toDF("value").write.format("delta").mode("append").save(path)
      val restartedStream = loadStreamWithOptions(path, Map(option -> "true"))
      testStream(restartedStream)(
        StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
        ProcessAllAvailable(),
        CheckAnswer(2, 3, 4),
        StopStream)
    }
  }

  test("historical deletion vectors reject an already planned batch") {
    withTempDirs { (inputDir, checkpointDir, _) =>
      val path = inputDir.getCanonicalPath
      val checkpoint = checkpointDir.getCanonicalPath
      createTableWithoutDeletionVectors(path)
      enableDeletionVectorsForTable(path)
      var lastBatch = -1L

      testStream(loadStreamWithOptions(path, Map("ignoreChanges" -> "true")))(
        StartStream(checkpointLocation = checkpoint),
        ProcessAllAvailable(),
        CheckAnswer(1, 2, 3),
        Assert {
          commitDeletionVector(path)
        },
        ProcessAllAvailable(),
        CheckAnswer(1, 2, 2, 3, 3),
        AssertOnQuery { query =>
          lastBatch = query.commitLog.getLatestBatchId().get
          true
        },
        StopStream)

      // Keep the offset but remove its commit to replay this batch without latestOffset.
      val commitFile = new Path(checkpoint, s"commits/$lastBatch")
      val deltaLog = DeltaLog.forTable(spark, path)
      assert(commitFile.getFileSystem(deltaLog.newDeltaHadoopConf()).delete(commitFile, false))
      fastDropDeletionVectors(path)

      testStream(loadStreamWithOptions(path, Map("ignoreChanges" -> "true")))(
        StartStream(checkpointLocation = checkpoint),
        ExpectFailure[DeltaRuntimeException] { error =>
          checkError(
            error.asInstanceOf[DeltaRuntimeException],
            "DELTA_STREAMING_DELETION_VECTORS_REQUIRE_SCHEMA_TRACKING",
            "55000",
            Map.empty[String, String])
        },
        AssertOnQuery { query =>
          assert(query.offsetLog.getLatestBatchId().contains(lastBatch))
          assert(!query.commitLog.getLatestBatchId().contains(lastBatch))
          true
        },
        checkRowsAfterFailure())
    }
  }

  test("mid-stream deletion vectors respect excluded paths") {
    withTempDir { inputDir =>
      val path = inputDir.getCanonicalPath
      createTableWithoutDeletionVectors(path)
      val deltaLog = DeltaLog.forTable(spark, path)
      val excludedFile = deltaLog.update().allFiles.collect().head.path
      val options = Map(
        "ignoreChanges" -> "true",
        "excludeRegex" -> java.util.regex.Pattern.quote(excludedFile))

      testStream(loadStreamWithOptions(path, options))(
        ProcessAllAvailable(),
        CheckAnswer(),
        Assert {
          enableDeletionVectorsForTable(path)
          commitDeletionVector(path)
          Seq(4).toDF("value").write.format("delta").mode("append").save(path)
        },
        ProcessAllAvailable(),
        CheckAnswer(4),
        StopStream)
    }
  }

  override protected def shouldPassTests: Set[String] = Set(
    "historical deletion vectors reject an already planned batch",
    "mid-stream deletion vectors respect excluded paths",
    "historical deletion vectors after a committed DV batch recover with new tracking",
    "historical deletion vectors fail before recording offsets and recover with new tracking",
    "mid-stream deletion vectors require restart with the same checkpoint (ignoreChanges)",
    "mid-stream deletion vectors require restart with the same checkpoint (ignoreFileDeletion)",
    "schema tracking persists mid-stream deletion vector protocol before restart",
    "mid-stream deletion vectors skip change commits without restart",
    "allow to delete files before starting a streaming query",
    "allow to delete files before staring a streaming query without checkpoint",
    "multiple deletion vectors per file with initial snapshot",
    "deleting files fails query if ignoreDeletes = false",
    "deleting files when ignoreChanges = true doesn't fail the query",
    "allow to delete files after staring a streaming query when ignoreDeletes is true",
    "allow to delete files after staring a streaming query when ignoreFileDeletion is true",
    "updating the source table causes failure when ignoreChanges = false - using DELETE",
    "allow to update the source table when ignoreChanges = true - using DELETE",
    "updating source table when ignoreDeletes = true fails the query - using DELETE",
    "subsequent DML commands are processed correctly in a batch - DELETE->DELETE - List()",
    "subsequent DML commands are processed correctly in a batch - DELETE->DELETE" +
      " - List((ignoreDeletes,true))",
    "subsequent DML commands are processed correctly in a batch - DELETE->DELETE" +
      " - List((skipChangeCommits,true))",
    "subsequent DML commands are processed correctly in a batch - DELETE->DELETE" +
      " - List((ignoreChanges,true))",
    "subsequent DML commands are processed correctly in a batch - INSERT->DELETE - List()",
    "subsequent DML commands are processed correctly in a batch - INSERT->DELETE" +
      " - List((ignoreDeletes,true))",
    "subsequent DML commands are processed correctly in a batch - INSERT->DELETE" +
      " - List((skipChangeCommits,true))",
    "subsequent DML commands are processed correctly in a batch - INSERT->DELETE" +
      " - List((ignoreChanges,true))",
    "multiple deletion vectors per file - List((ignoreChanges,true))",
    "multiple deletion vectors per file - List((ignoreFileDeletion,true))",
    "streaming read with nulls and deletion vectors does not NPE",
    "streaming read with variant column and deletion vectors does not ClassCastException",
    "variant column and deletion vectors preserve payload identity",
    "array column and deletion vectors preserve element identity"
  )

  override protected def shouldFailTests: Set[String] = Set.empty
}
