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

import org.apache.spark.sql.delta.{DeletionVectorsTableFeature, DeltaCDCStreamDeletionVectorMixin, DeltaCDCStreamSuite, DeltaConfigs, DeltaLog, DeltaOperations, DeltaRuntimeException}
import org.apache.spark.sql.delta.actions.AddCDCFile
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.Row

/**
 * Test suite that runs DeltaCDCStreamSuite using the V2 connector (V2_ENABLE_MODE=STRICT).
 */
class DeltaV2CDCStreamSuite extends DeltaCDCStreamSuite with V2ForceTest {

  override protected def useDsv2: Boolean = true

  override protected def enableCDF(path: String): Unit = {
    val deltaLog = DeltaLog.forTable(spark, path)
    val metadata = deltaLog.update().metadata
    deltaLog.startTransaction().commit(
      metadata.copy(configuration =
        metadata.configuration + (DeltaConfigs.CHANGE_DATA_FEED.key -> "true")) :: Nil,
      DeltaOperations.SetTableProperties(
        Map(DeltaConfigs.CHANGE_DATA_FEED.key -> "true")))
  }

  // V2 doesn't support write ops; run DELETE in V1 mode.
  override protected def executeDeleteSql(sqlText: String): Unit = executeInV1Mode(sqlText)

  override protected def executeDml(sqlText: String): Unit = executeInV1Mode(sqlText)

  override protected def shouldPassTests: Set[String] = Set(
    // ========== Core CDC streaming tests ==========
    "no startingVersion should result fetch the entire snapshot",
    "CDC initial snapshot should end at base index of next version",
    "startingVersion = latest",
    "user provided startingVersion",
    "user provided startingTimestamp",
    "startingVersion and startingTimestamp are both set",
    "cdc streams should respect checkpoint",
    "cdc streams with noop merge",
    "streams updating latest offset with readChangeFeed=true",
    "streams updating latest offset with readChangeFeed=false",

    // ========== File action variant tests ==========
    "cdc streams should be able to get offset when there only RemoveFiles",
    "cdc streams should work starting from RemoveFile",
    "cdc streams should work starting from AddCDCFile",
    "double delete-only on the same file",

    // ========== Rate limiting tests ==========
    "rateLimit - maxFilesPerTrigger - overall",
    "rateLimit - maxBytesPerTrigger - overall",
    "rateLimit - maxFilesPerTrigger - starting from initial snapshot",
    "rateLimit - maxBytesPerTrigger - starting from initial snapshot",
    "rateLimit - maxFilesPerTrigger - should not deadlock",
    "rateLimit - maxBytesPerTrigger - should not deadlock",
    "maxFilesPerTrigger - 2 successive AddCDCFile commits",
    "maxFilesPerTrigger - batch reject stops iteration to prevent data loss",
    "maxFilesPerTrigger with Trigger.AvailableNow respects read limits",

    // ========== Misc tests ==========
    "check starting[Version/Timestamp] > latest version without error",
    "excludeRegex works with cdc",
    "excludeRegex on cdcPath should not return Add/RemoveFiles",
    "schema check for cdc stream",
    "should not attempt to read a non exist version",

    // ========== Option B refactor + RT-rejection coverage ==========
    "CDC stream supports column pruning of data columns",
    "CDC stream rejects reading row tracking metadata fields",

    // ========== CDC combination coverage ==========
    "CDC stream on partitioned table strips partition and CDC columns correctly",
    "CDC stream on column-mapped table passes through correctly",

    // ========== filterFiles END-marker offset-roll ==========
    "CDF stream rolls the offset past a change-data commit (keeps END marker)"
  )

  override protected def shouldFailTests: Set[String] = Set(
    // === Error message format differs in V2 (missing [DELTA_VERSION_NOT_FOUND] prefix) ===
    "starting[Version/Timestamp] > latest version",
    // === V2 wraps DeltaAnalysisException in RuntimeException, so the test's
    // === ExpectFailure[DeltaAnalysisException] cause-class check fails.
    "startingVersion before CDF was enabled rejects with change-data-not-recorded"
  )
}

/**
 * Runs DeltaCDCStreamSuite with DVs enabled using the V2 connector (V2_ENABLE_MODE=STRICT).
 */
class DeltaV2CDCStreamDeletionVectorSuite
    extends DeltaV2CDCStreamSuite with DeltaCDCStreamDeletionVectorMixin {

  import testImplicits._

  test(
    "CDC mid-stream deletion vectors require restart with the same checkpoint") {
    withTempDirs { (inputDir, checkpointDir, _) =>
      val path = inputDir.getCanonicalPath
      spark.range(1, 4).coalesce(1).write.format("delta")
        .option(DeltaConfigs.CHANGE_DATA_FEED.key, "true")
        .option(DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key, "false").save(path)
      val deltaLog = DeltaLog.forTable(spark, path)
      val initialSnapshot = deltaLog.update()
      assert(!initialSnapshot.protocol.isFeatureSupported(DeletionVectorsTableFeature))
      val originalFile = initialSnapshot.allFiles.collect().head.path
      val initialRows = Seq((1L, "insert", 0L), (2L, "insert", 0L), (3L, "insert", 0L))
      val stream = loadCDCStream(path).drop(CDCReader.CDC_COMMIT_TIMESTAMP)

      testStream(stream)(
        StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
        ProcessAllAvailable(),
        CheckAnswer(initialRows: _*),
        Assert {
          executeDml(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES " +
            "('delta.enableDeletionVectors' = 'true')")
          assert(deltaLog.update().protocol.isFeatureSupported(DeletionVectorsTableFeature))
          ()
        },
        ProcessAllAvailable(),
        CheckAnswer(initialRows: _*),
        Assert {
          executeDeleteSql(s"DELETE FROM delta.`$path` WHERE id = 1")
          val filesWithDVs = getFilesWithDeletionVectors(deltaLog)
          assert(filesWithDVs.size == 1)
          assert(filesWithDVs.head.path == originalFile)
          assert(filesWithDVs.head.deletionVector.cardinality == 1L)
          assert(!deltaLog.getChanges(2L).next()._2.exists(_.isInstanceOf[AddCDCFile]))
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
          assert(query.offsetLog.getLatestBatchId() == query.commitLog.getLatestBatchId())
          true
        },
        CheckAnswer { (rows: Seq[Row]) =>
          val events = rows.map(row => (row.getLong(0), row.getString(1), row.getLong(2)))
          assert(events.sortBy(_._1) == initialRows)
          ()
        })

      Seq(4L).toDF("id").write.format("delta").mode("append").save(path)
      val restartedStream = loadCDCStream(path).drop(CDCReader.CDC_COMMIT_TIMESTAMP)
      testStream(restartedStream)(
        StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
        ProcessAllAvailable(),
        CheckAnswer((1L, "delete", 2L), (4L, "insert", 3L)),
        StopStream)
    }
  }

  test(
    "CDC historical deletion vectors after committed activation require schema tracking") {
    withSQLConf(
        DeltaSQLConf.FAST_DROP_FEATURE_ENABLED.key -> "true",
        DeltaSQLConf.FAST_DROP_FEATURE_GENERATE_DV_TOMBSTONES.key -> "true") {
      withTempDirs { (inputDir, checkpointDir, _) =>
        val path = inputDir.getCanonicalPath
        val checkpoint = checkpointDir.getCanonicalPath
        spark.range(1, 4).coalesce(1).write.format("delta")
          .option(DeltaConfigs.CHANGE_DATA_FEED.key, "true")
          .option(DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key, "false").save(path)
        val deltaLog = DeltaLog.forTable(spark, path)
        val options = Map("maxFilesPerTrigger" -> "1")

        testStream(loadCDCStream(path, options).drop(CDCReader.CDC_COMMIT_TIMESTAMP))(
          StartStream(checkpointLocation = checkpoint),
          ProcessAllAvailable(),
          CheckAnswer((1L, "insert", 0L), (2L, "insert", 0L), (3L, "insert", 0L)),
          Assert {
            executeDml(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES " +
              "('delta.enableDeletionVectors' = 'true')")
          },
          ProcessAllAvailable(),
          StopStream)

        executeDeleteSql(s"DELETE FROM delta.`$path` WHERE id = 1")
        val deleteVersion = deltaLog.update().version
        assert(getFilesWithDeletionVectors(deltaLog).nonEmpty)
        assert(!deltaLog.getChanges(deleteVersion).next()._2.exists(_.isInstanceOf[AddCDCFile]))
        // Read the inferred DELETE and the append before the metadata barriers for the drop.
        Seq(4L).toDF("id").coalesce(1).write.format("delta").mode("append").save(path)
        val appendVersion = deltaLog.update().version
        executeDml(s"ALTER TABLE delta.`$path` DROP FEATURE deletionVectors")
        assert(!deltaLog.update().protocol.isFeatureSupported(DeletionVectorsTableFeature))

        testStream(loadCDCStream(path, options).drop(CDCReader.CDC_COMMIT_TIMESTAMP))(
          StartStream(checkpointLocation = checkpoint),
          ExpectFailure[DeltaRuntimeException] { error =>
            checkError(
              error.asInstanceOf[DeltaRuntimeException],
              "DELTA_STREAMING_DELETION_VECTORS_REQUIRE_SCHEMA_TRACKING",
              "55000",
              Map.empty[String, String])
          },
          CheckAnswer { (rows: Seq[Row]) =>
            assert(rows.isEmpty)
            ()
          })

        val trackedOptions = options +
          ("schemaTrackingLocation" -> new Path(checkpoint, "_schema_location").toString)
        // Persist the historical protocol, then restart to use it.
        testStream(loadCDCStream(path, trackedOptions).drop(CDCReader.CDC_COMMIT_TIMESTAMP))(
          StartStream(checkpointLocation = checkpoint),
          ExpectFailure[DeltaRuntimeException] { error =>
            assert(error.asInstanceOf[DeltaRuntimeException].getErrorClass ==
              "DELTA_STREAMING_METADATA_EVOLUTION")
          })

        // Read the inferred DELETE and the append, then stop at the property change disabling DVs.
        testStream(loadCDCStream(path, trackedOptions).drop(CDCReader.CDC_COMMIT_TIMESTAMP))(
          StartStream(checkpointLocation = checkpoint),
          ExpectFailure[DeltaRuntimeException] { error =>
            assert(error.asInstanceOf[DeltaRuntimeException].getErrorClass ==
              "DELTA_STREAMING_METADATA_EVOLUTION")
          },
          CheckAnswer { (rows: Seq[Row]) =>
            val events = rows.map(row => (row.getLong(0), row.getString(1), row.getLong(2)))
            assert(events.sortBy(_._1) == Seq(
              (1L, "delete", deleteVersion), (4L, "insert", appendVersion)))
            ()
          })
      }
    }
  }

  test(
    "CDC mid-stream deletion vectors do not require restart for explicit change files") {
    withTempDir { inputDir =>
      val path = inputDir.getCanonicalPath
      spark.range(1, 4).coalesce(1).write.format("delta")
        .option(DeltaConfigs.CHANGE_DATA_FEED.key, "true")
        .option(DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key, "false").save(path)
      val deltaLog = DeltaLog.forTable(spark, path)
      assert(!deltaLog.update().protocol.isFeatureSupported(DeletionVectorsTableFeature))
      val stream = loadCDCStream(path).drop(CDCReader.CDC_COMMIT_TIMESTAMP)

      testStream(stream)(
        ProcessAllAvailable(),
        CheckAnswer((1L, "insert", 0L), (2L, "insert", 0L), (3L, "insert", 0L)),
        Assert {
          executeDml(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES " +
            "('delta.enableDeletionVectors' = 'true')")
          assert(deltaLog.update().protocol.isFeatureSupported(DeletionVectorsTableFeature))
          ()
        },
        ProcessAllAvailable(),
        Assert {
          executeDml(s"UPDATE delta.`$path` SET id = 10 WHERE id = 1")
          assert(deltaLog.getChanges(2L).next()._2.exists(_.isInstanceOf[AddCDCFile]))
          assert(getFilesWithDeletionVectors(deltaLog).nonEmpty)
          ()
        },
        ProcessAllAvailable(),
        CheckAnswer(
          (1L, "insert", 0L), (2L, "insert", 0L), (3L, "insert", 0L),
          (1L, "update_preimage", 2L), (10L, "update_postimage", 2L)),
        Assert {
          Seq(4L).toDF("id").write.format("delta").mode("append").save(path)
        },
        ProcessAllAvailable(),
        CheckAnswer(
          (1L, "insert", 0L), (2L, "insert", 0L), (3L, "insert", 0L),
          (1L, "update_preimage", 2L), (10L, "update_postimage", 2L), (4L, "insert", 3L)),
        StopStream)
    }
  }

  test("CDC historical deletion vectors reject an already planned batch") {
    withTempDirs { (inputDir, checkpointDir, _) =>
      val path = inputDir.getCanonicalPath
      val checkpoint = checkpointDir.getCanonicalPath
      spark.range(1, 4).coalesce(1).write.format("delta")
        .option(DeltaConfigs.CHANGE_DATA_FEED.key, "true")
        .option(DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key, "true").save(path)
      val deltaLog = DeltaLog.forTable(spark, path)
      var lastBatch = -1L

      testStream(loadCDCStream(path).drop(CDCReader.CDC_COMMIT_TIMESTAMP))(
        StartStream(checkpointLocation = checkpoint),
        ProcessAllAvailable(),
        CheckAnswer((1L, "insert", 0L), (2L, "insert", 0L), (3L, "insert", 0L)),
        Assert {
          executeDeleteSql(s"DELETE FROM delta.`$path` WHERE id = 1")
          assert(getFilesWithDeletionVectors(deltaLog).nonEmpty)
          assert(!deltaLog.getChanges(1L).next()._2.exists(_.isInstanceOf[AddCDCFile]))
          ()
        },
        ProcessAllAvailable(),
        CheckAnswer(
          (1L, "insert", 0L), (2L, "insert", 0L), (3L, "insert", 0L),
          (1L, "delete", 1L)),
        AssertOnQuery { query =>
          lastBatch = query.commitLog.getLatestBatchId().get
          true
        },
        StopStream)

      // Replay the recorded batch after the latest protocol no longer advertises DVs.
      val commitFile = new Path(checkpoint, s"commits/$lastBatch")
      assert(commitFile.getFileSystem(deltaLog.newDeltaHadoopConf()).delete(commitFile, false))
      withSQLConf(
          DeltaSQLConf.FAST_DROP_FEATURE_ENABLED.key -> "true",
          DeltaSQLConf.FAST_DROP_FEATURE_GENERATE_DV_TOMBSTONES.key -> "true") {
        executeDml(s"ALTER TABLE delta.`$path` DROP FEATURE deletionVectors")
      }
      assert(!deltaLog.update().protocol.isFeatureSupported(DeletionVectorsTableFeature))

      testStream(loadCDCStream(path).drop(CDCReader.CDC_COMMIT_TIMESTAMP))(
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
        CheckAnswer { (rows: Seq[Row]) =>
          assert(rows.isEmpty)
          ()
        })
    }
  }

  override protected def shouldPassTests: Set[String] =
    super.shouldPassTests ++
      Set(
        "CDC historical deletion vectors reject an already planned batch",
        "CDC historical deletion vectors after committed activation require schema tracking",
        "CDC mid-stream deletion vectors require restart with the same checkpoint",
        "CDC mid-stream deletion vectors do not require restart for explicit change files",
        "CDC DV-diff: only deleted rows appear, not surviving rows",
        "CDC DV-diff: second delete emits only newly deleted rows, not prior deletions",
        "CDC DV-diff: partitioned table includes correct partition values",
        "CDC DV-diff: delete spanning multiple files produces one DV pair per file",
        "CDC DV-diff: multi-file commit admitted all-or-nothing under maxFilesPerTrigger",
        "CDC DV-diff: maxBytesPerTrigger rate limits across DV-diff commits"
      )
}
