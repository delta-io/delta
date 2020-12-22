/*
 * Copyright (2020) The Delta Lake Project Authors.
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

import java.io.{File, FileNotFoundException}

import scala.language.postfixOps

import org.apache.spark.sql.delta.DeltaOperations.Truncate
import org.apache.spark.sql.delta.DeltaTestUtils.OptimisticTxnTestHelper
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}
import org.apache.hadoop.fs.Path

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}
import org.apache.spark.util.Utils

// scalastyle:off: removeFile
class DeltaLogSuite extends QueryTest
  with SharedSparkSession  with SQLTestUtils {

  protected val testOp = Truncate()

  testQuietly("checkpoint") {
    val tempDir = Utils.createTempDir()
    val log1 = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))

    (1 to 15).foreach { i =>
      val txn = log1.startTransaction()
      val file = AddFile(i.toString, Map.empty, 1, 1, true) :: Nil
      val delete: Seq[Action] = if (i > 1) {
        RemoveFile(i - 1 toString, Some(System.currentTimeMillis()), true) :: Nil
      } else {
        Nil
      }
      txn.commitManually(delete ++ file: _*)
    }

    DeltaLog.clearCache()
    val log2 = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
    assert(log2.snapshot.version == log1.snapshot.version)
    assert(log2.snapshot.allFiles.count == 1)
  }

  testQuietly("SC-8078: update deleted directory") {
    withTempDir { dir =>
      val path = new Path(dir.getCanonicalPath)
      val log = DeltaLog.forTable(spark, path)

      // Commit data so the in-memory state isn't consistent with an empty log.
      val txn = log.startTransaction()
      val files = (1 to 10).map(f => AddFile(f.toString, Map.empty, 1, 1, true))
      txn.commitManually(files: _*)
      log.checkpoint()

      val fs = path.getFileSystem(spark.sessionState.newHadoopConf())
      fs.delete(path, true)

      val snapshot = log.update()
      assert(snapshot.version === -1)
    }
  }

  testQuietly("update should pick up checkpoints") {
    withTempDir { tempDir =>
      val log = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
      val checkpointInterval = log.checkpointInterval
      for (f <- 0 until (checkpointInterval * 2)) {
        val txn = log.startTransaction()
        txn.commitManually(AddFile(f.toString, Map.empty, 1, 1, true))
      }

      def collectReservoirStateRDD(rdd: RDD[_]): Seq[RDD[_]] = {
        if (rdd.name != null && rdd.name.startsWith("Delta Table State")) {
          Seq(rdd) ++ rdd.dependencies.flatMap(d => collectReservoirStateRDD(d.rdd))
        } else {
          rdd.dependencies.flatMap(d => collectReservoirStateRDD(d.rdd))
        }
      }

      val numOfStateRDDs = collectReservoirStateRDD(log.snapshot.state.rdd).size
      assert(numOfStateRDDs >= 1, "collectReservoirStateRDD may not work properly")
      assert(numOfStateRDDs < checkpointInterval)
    }
  }

  test("update shouldn't pick up delta files earlier than checkpoint") {
    val tempDir = Utils.createTempDir()
    val log1 = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))

    (1 to 5).foreach { i =>
      val txn = log1.startTransaction()
      val file = AddFile(i.toString, Map.empty, 1, 1, true) :: Nil
      val delete: Seq[Action] = if (i > 1) {
        RemoveFile(i - 1 toString, Some(System.currentTimeMillis()), true) :: Nil
      } else {
        Nil
      }
      txn.commitManually(delete ++ file: _*)
    }

    DeltaLog.clearCache()
    val log2 = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))

    (6 to 15).foreach { i =>
      val txn = log1.startTransaction()
      val file = AddFile(i.toString, Map.empty, 1, 1, true) :: Nil
      val delete: Seq[Action] = if (i > 1) {
        RemoveFile(i - 1 toString, Some(System.currentTimeMillis()), true) :: Nil
      } else {
        Nil
      }
      txn.commitManually(delete ++ file: _*)
    }

    // Since log2 is a separate instance, it shouldn't be updated to version 15
    assert(log2.snapshot.version == 4)
    val updateLog2 = log2.update()
    assert(updateLog2.version == log1.snapshot.version, "Did not update to correct version")

    val deltas = log2.snapshot.logSegment.deltas
    assert(deltas.length === 4, "Expected 4 files starting at version 11 to 14")
    val versions = deltas.map(f => FileNames.deltaVersion(f.getPath)).sorted
    assert(versions === Seq[Long](11, 12, 13, 14), "Received the wrong files for update")
  }

  testQuietly("ActionLog cache should use the normalized path as key") {
    withTempDir { tempDir =>
      val dir = tempDir.getAbsolutePath.stripSuffix("/")
      assert(dir.startsWith("/"))
      val fs = new Path("/").getFileSystem(spark.sessionState.newHadoopConf())
      val samePaths = Seq(
        new Path(dir + "/foo"),
        new Path(dir + "/foo/"),
        new Path(fs.getScheme + ":" + dir + "/foo"),
        new Path(fs.getScheme + "://" + dir + "/foo")
      )
      val logs = samePaths.map(DeltaLog.forTable(spark, _))
      logs.foreach { log =>
        assert(log eq logs.head)
      }
    }
  }

  testQuietly("handle corrupted '_last_checkpoint' file") {
    withTempDir { tempDir =>
      val log = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))

      val checkpointInterval = log.checkpointInterval
      for (f <- 0 to checkpointInterval) {
        val txn = log.startTransaction()
        txn.commitManually(AddFile(f.toString, Map.empty, 1, 1, true))
      }
      assert(log.lastCheckpoint.isDefined)

      val lastCheckpoint = log.lastCheckpoint.get

      // Create an empty "_last_checkpoint" (corrupted)
      val fs = log.LAST_CHECKPOINT.getFileSystem(spark.sessionState.newHadoopConf)
      fs.create(log.LAST_CHECKPOINT, true /* overwrite */).close()

      // Create a new DeltaLog
      DeltaLog.clearCache()
      val log2 = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
      // Make sure we create a new DeltaLog in order to test the loading logic.
      assert(log ne log2)

      // We should get the same metadata even if "_last_checkpoint" is corrupted.
      assert(CheckpointInstance(log2.lastCheckpoint.get) === CheckpointInstance(lastCheckpoint))
    }
  }

  testQuietly("paths should be canonicalized") {
    Seq("file:", "file://").foreach { scheme =>
      withTempDir { dir =>
        val log = DeltaLog.forTable(spark, dir)
        assert(new File(log.logPath.toUri).mkdirs())
        val path = "/some/unqualified/absolute/path"
        val add = AddFile(
          path, Map.empty, 100L, 10L, dataChange = true)
        val rm = RemoveFile(
          s"$scheme$path", Some(200L), dataChange = false)

        log.store.write(
          FileNames.deltaFile(log.logPath, 0L),
          Iterator(Protocol(), Metadata(), add).map(a => JsonUtils.toJson(a.wrap)))
        log.store.write(
          FileNames.deltaFile(log.logPath, 1L),
          Iterator(JsonUtils.toJson(rm.wrap)))

        assert(log.update().version === 1)
        assert(log.snapshot.numOfFiles === 0)
      }
    }
  }

  testQuietly("paths should be canonicalized - special characters") {
    Seq("file:", "file://").foreach { scheme =>
      withTempDir { dir =>
        val log = DeltaLog.forTable(spark, dir)
        assert(new File(log.logPath.toUri).mkdirs())
        val path = new Path("/some/unqualified/with space/p@#h").toUri.toString
        val add = AddFile(
          path, Map.empty, 100L, 10L, dataChange = true)
        val rm = RemoveFile(
          s"$scheme$path", Some(200L), dataChange = false)

        log.store.write(
          FileNames.deltaFile(log.logPath, 0L),
          Iterator(Protocol(), Metadata(), add).map(a => JsonUtils.toJson(a.wrap)))
        log.store.write(
          FileNames.deltaFile(log.logPath, 1L),
          Iterator(JsonUtils.toJson(rm.wrap)))

        assert(log.update().version === 1)
        assert(log.snapshot.numOfFiles === 0)
      }
    }
  }

  test("Reject read from Delta if no path is passed") {
    val e = intercept[IllegalArgumentException](spark.read.format("delta").load()).getMessage
    assert(e.contains("'path' is not specified"))
  }

  test("do not relativize paths in RemoveFiles") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(spark, dir)
      assert(new File(log.logPath.toUri).mkdirs())
      val path = new File(dir, "a/b/c").getCanonicalPath
      val rm = RemoveFile(path, Some(System.currentTimeMillis()), dataChange = true)
      log.startTransaction().commitManually(rm)

      val committedRemove = log.update(stalenessAcceptable = false).tombstones.collect()
      assert(committedRemove.head.path === s"file://$path")
    }
  }

  test("delete and re-add the same file in different transactions") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(spark, dir)
      assert(new File(log.logPath.toUri).mkdirs())

      val add1 = AddFile("foo", Map.empty, 1L, System.currentTimeMillis(), dataChange = true)
      log.startTransaction().commitManually(add1)

      val rm = add1.remove
      log.startTransaction().commit(rm :: Nil, DeltaOperations.ManualUpdate)

      val add2 = AddFile("foo", Map.empty, 1L, System.currentTimeMillis(), dataChange = true)
      log.startTransaction().commit(add2 :: Nil, DeltaOperations.ManualUpdate)

      // Add a new transaction to replay logs using the previous snapshot. If it contained
      // AddFile("foo") and RemoveFile("foo"), "foo" would get removed and fail this test.
      val otherAdd = AddFile("bar", Map.empty, 1L, System.currentTimeMillis(), dataChange = true)
      log.startTransaction().commit(otherAdd :: Nil, DeltaOperations.ManualUpdate)

      assert(log.update().allFiles.collect().find(_.path == "foo")
        // `dataChange` is set to `false` after replaying logs.
        === Some(add2.copy(dataChange = false)))
    }
  }

  test("error - versions not contiguous") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(spark, dir)
      assert(new File(log.logPath.toUri).mkdirs())

      val metadata = Metadata()
      val add1 = AddFile("foo", Map.empty, 1L, System.currentTimeMillis(), dataChange = true)
      log.startTransaction().commit(metadata :: add1 :: Nil, DeltaOperations.ManualUpdate)

      val add2 = AddFile("foo", Map.empty, 1L, System.currentTimeMillis(), dataChange = true)
      log.startTransaction().commit(add2 :: Nil, DeltaOperations.ManualUpdate)

      val add3 = AddFile("foo", Map.empty, 1L, System.currentTimeMillis(), dataChange = true)
      log.startTransaction().commit(add3 :: Nil, DeltaOperations.ManualUpdate)

      new File(new Path(log.logPath, "00000000000000000001.json").toUri).delete()

      DeltaLog.clearCache()
      val ex = intercept[IllegalStateException] {
        DeltaLog.forTable(spark, dir)
      }
      assert(ex.getMessage === "Versions (Vector(0, 2)) are not contiguous.")
    }
  }

  Seq("protocol", "metadata").foreach { action =>
    test(s"state reconstruction without $action should fail") {
      withTempDir { tempDir =>
        val log = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
        assert(new File(log.logPath.toUri).mkdirs())
        val selectedAction = if (action == "metadata") {
          Protocol()
        } else {
          Metadata()
        }
        val file = AddFile("abc", Map.empty, 1, 1, true)
        log.store.write(
          FileNames.deltaFile(log.logPath, 0L),
          Iterator(selectedAction, file).map(a => JsonUtils.toJson(a.wrap)))
        withSQLConf(DeltaSQLConf.DELTA_STATE_RECONSTRUCTION_VALIDATION_ENABLED.key -> "true") {
          val e = intercept[IllegalStateException] {
            log.update()
          }
          assert(e.getMessage === DeltaErrors.actionNotFoundException(action, 0).getMessage)
        }
        // Disable the validation check
        withSQLConf(DeltaSQLConf.DELTA_STATE_RECONSTRUCTION_VALIDATION_ENABLED.key -> "false") {
          assert(log.update().version === 0L)
        }
      }
    }
  }

  Seq("protocol", "metadata").foreach { action =>
    testQuietly(s"state reconstruction from checkpoint with missing $action should fail") {
      withTempDir { tempDir =>
        import testImplicits._
        val log = DeltaLog.forTable(spark, tempDir)
        val checkpointInterval = log.checkpointInterval
        // Create a checkpoint regularly
        for (f <- 0 to checkpointInterval) {
          val txn = log.startTransaction()
          if (f == 0) {
            txn.commitManually(AddFile(f.toString, Map.empty, 1, 1, true))
          } else {
            txn.commit(Seq(AddFile(f.toString, Map.empty, 1, 1, true)), testOp)
          }
        }

        {
          // Create an incomplete checkpoint without the action and overwrite the
          // original checkpoint
          val checkpointPath = FileNames.checkpointFileSingular(log.logPath, log.snapshot.version)
          withTempDir { tmpCheckpoint =>
            val takeAction = if (action == "metadata") {
              "protocol"
            } else {
              "metadata"
            }
            val corruptedCheckpointData = spark.read.schema(SingleAction.encoder.schema)
              .parquet(checkpointPath.toString)
              .where(s"add is not null or $takeAction is not null")
              .as[SingleAction].collect()

            // Keep the add files and also filter by the additional condition
            corruptedCheckpointData.toSeq.toDS().coalesce(1).write
              .mode("overwrite").parquet(tmpCheckpoint.toString)
            val writtenCheckpoint =
              tmpCheckpoint.listFiles().toSeq.filter(_.getName.startsWith("part")).head
            val checkpointFile = new File(checkpointPath.toUri)
            new File(log.logPath.toUri).listFiles().toSeq.foreach { file =>
              if (file.getName.startsWith(".0")) {
                // we need to delete checksum files, otherwise trying to replace our incomplete
                // checkpoint file fails due to the LocalFileSystem's checksum checks.
                require(file.delete(), "Failed to delete checksum file")
              }
            }
            require(checkpointFile.delete(), "Failed to delete old checkpoint")
            require(writtenCheckpoint.renameTo(checkpointFile),
              "Failed to rename corrupt checkpoint")
          }
        }

        DeltaLog.clearCache()

        // Verify if the state reconstruction from the checkpoint fails.
        withSQLConf(DeltaSQLConf.DELTA_STATE_RECONSTRUCTION_VALIDATION_ENABLED.key -> "true") {
          val e = intercept[IllegalStateException] {
            DeltaLog.forTable(spark, tempDir).update()
          }
          assert(e.getMessage === DeltaErrors.actionNotFoundException(action, 10).getMessage)
        }

        // Disable state reconstruction validation and try again
        withSQLConf(DeltaSQLConf.DELTA_STATE_RECONSTRUCTION_VALIDATION_ENABLED.key -> "false") {
          assert(DeltaLog.forTable(spark, tempDir).update().version === 10)
        }
      }
    }
  }

  test("deleting and recreating a directory should cause the snapshot to be recomputed") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(10).write.format("delta").mode("append").save(path)
      spark.range(10, 20).write.format("delta").mode("append").save(path)
      val deltaLog = DeltaLog.forTable(spark, path)
      deltaLog.checkpoint()
      spark.range(20, 30).write.format("delta").mode("append").save(path)

      // Store these for later usage
      val actions = deltaLog.snapshot.state.collect()
      val commitTimestamp = deltaLog.snapshot.logSegment.lastCommitTimestamp

      checkAnswer(
        spark.read.format("delta").load(path),
        spark.range(30).toDF()
      )

      // Now let's delete the last version
      deltaLog.store
        .listFrom(FileNames.checksumFile(deltaLog.logPath, deltaLog.snapshot.version))
        .foreach(f => deltaLog.fs.delete(f.getPath, false))

      // Should show up to 20
      checkAnswer(
        spark.read.format("delta").load(path),
        spark.range(20).toDF()
      )

      // Now let's delete the checkpoint and json file for version 1. We will try to list from
      // version 1, but since we can't find anything, we should start listing from version 0
      deltaLog.store
        .listFrom(FileNames.checkpointFileSingular(deltaLog.logPath, 1))
        .foreach(f => deltaLog.fs.delete(f.getPath, false))

      checkAnswer(
        spark.read.format("delta").load(path),
        spark.range(10).toDF()
      )

      // Now let's delete that commit as well, and write a new first version
      deltaLog.store
        .listFrom(FileNames.deltaFile(deltaLog.logPath, 0))
        .foreach(f => deltaLog.fs.delete(f.getPath, false))

      assert(deltaLog.snapshot.version === 0)

      deltaLog.store.write(
        FileNames.deltaFile(deltaLog.logPath, 0),
        actions.map(_.unwrap.json).iterator,
        overwrite = false)

      // To avoid flakiness, we manually set the modification timestamp of the file to a later
      // second
      new File(FileNames.deltaFile(deltaLog.logPath, 0).toUri)
        .setLastModified(commitTimestamp + 5000)

      checkAnswer(
        spark.read.format("delta").load(path),
        spark.range(30).toDF()
      )
    }
  }
}
