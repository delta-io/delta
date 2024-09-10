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

import java.io.{BufferedReader, File, InputStreamReader, IOException}
import java.nio.charset.StandardCharsets
import java.util.{Locale, Optional}

import scala.collection.JavaConverters._
import scala.language.postfixOps

import org.apache.spark.sql.delta.DeltaOperations.Truncate
import org.apache.spark.sql.delta.DeltaTestUtils.createTestAddFile
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.coordinatedcommits.{CommitCoordinatorProvider, CoordinatedCommitsBaseSuite, InMemoryCommitCoordinator, TrackingCommitCoordinatorClient}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaSQLTestUtils
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.delta.storage.commit.TableDescriptor
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.FsPermission

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.JsonToStructs
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

// scalastyle:off: removeFile
class DeltaLogSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest
  with CoordinatedCommitsBaseSuite
  with DeltaCheckpointTestUtils
  with DeltaSQLTestUtils {


  protected val testOp = Truncate()

  testDifferentCheckpoints("checkpoint", quiet = true) { (_, _) =>
    val tempDir = Utils.createTempDir()
    val log1 = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))

    (1 to 15).foreach { i =>
      val txn = log1.startTransaction()
      val file = createTestAddFile(encodedPath = i.toString) :: Nil
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

  testDifferentCheckpoints("update deleted directory", quiet = true) { (_, _) =>
    withTempDir { dir =>
      val path = new Path(dir.getCanonicalPath)
      val log = DeltaLog.forTable(spark, path)

      // Commit data so the in-memory state isn't consistent with an empty log.
      val txn = log.startTransaction()
      val files = (1 to 10).map(f => createTestAddFile(encodedPath = f.toString))
      txn.commitManually(files: _*)
      log.checkpoint()

      val fs = path.getFileSystem(log.newDeltaHadoopConf())
      fs.delete(path, true)

      val snapshot = log.update()
      assert(snapshot.version === -1)
    }
  }

  testDifferentCheckpoints(
      "checkpoint write should use the correct Hadoop configuration") { (_, _) =>
    withTempDir { dir =>
      withSQLConf(
          "fs.AbstractFileSystem.fake.impl" -> classOf[FakeAbstractFileSystem].getName,
          "fs.fake.impl" -> classOf[FakeFileSystem].getName,
          "fs.fake.impl.disable.cache" -> "true") {
        val path = s"fake://${dir.getCanonicalPath}"
        val log = DeltaLog.forTable(spark, path)
        val txn = log.startTransaction()
        txn.commitManually(createTestAddFile())
        log.checkpoint()
      }
    }
  }

  testDifferentCheckpoints("update should pick up checkpoints", quiet = true) { (_, _) =>
    withTempDir { tempDir =>
      val log = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
      val checkpointInterval = log.checkpointInterval()
      for (f <- 0 until (checkpointInterval * 2)) {
        val txn = log.startTransaction()
        txn.commitManually(createTestAddFile(encodedPath = f.toString))
      }

      def collectReservoirStateRDD(rdd: RDD[_]): Seq[RDD[_]] = {
        if (rdd.name != null && rdd.name.startsWith("Delta Table State")) {
          Seq(rdd) ++ rdd.dependencies.flatMap(d => collectReservoirStateRDD(d.rdd))
        } else {
          rdd.dependencies.flatMap(d => collectReservoirStateRDD(d.rdd))
        }
      }

      val numOfStateRDDs = collectReservoirStateRDD(log.snapshot.stateDS.rdd).size
      assert(numOfStateRDDs >= 1, "collectReservoirStateRDD may not work properly")
      assert(numOfStateRDDs < checkpointInterval)
    }
  }

  testDifferentCheckpoints(
      "update shouldn't pick up delta files earlier than checkpoint") { (_, _) =>
    val tempDir = Utils.createTempDir()
    val log1 = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))

    (1 to 5).foreach { i =>
      val txn = log1.startTransaction()
      val file = if (i > 1) {
        createTestAddFile(encodedPath = i.toString) :: Nil
      } else {
        Metadata(configuration = Map(DeltaConfigs.CHECKPOINT_INTERVAL.key -> "10")) :: Nil
      }
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
      val file = createTestAddFile(encodedPath = i.toString) :: Nil
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
    val versions = deltas.map(FileNames.deltaVersion).sorted
    assert(versions === Seq[Long](11, 12, 13, 14), "Received the wrong files for update")
  }

  testQuietly("ActionLog cache should use the normalized path as key") {
    withTempDir { tempDir =>
      val dir = tempDir.getAbsolutePath.stripSuffix("/")
      assert(dir.startsWith("/"))
      // scalastyle:off deltahadoopconfiguration
      val fs = new Path("/").getFileSystem(spark.sessionState.newHadoopConf())
      // scalastyle:on deltahadoopconfiguration
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

  testDifferentCheckpoints(
      "handle corrupted '_last_checkpoint' file", quiet = true) { (checkpointPolicy, format) =>
    withTempDir { tempDir =>
      val log = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))

      val checkpointInterval = log.checkpointInterval()
      for (f <- 0 to checkpointInterval) {
        val txn = log.startTransaction()
        txn.commitManually(createTestAddFile(encodedPath = f.toString))
      }
      val lastCheckpointOpt = log.readLastCheckpointFile()
      assert(lastCheckpointOpt.isDefined)
      val lastCheckpoint = lastCheckpointOpt.get
      import  CheckpointInstance.Format._
      val expectedCheckpointFormat = if (checkpointPolicy == CheckpointPolicy.V2) V2 else SINGLE
      assert(CheckpointInstance(lastCheckpoint).format === expectedCheckpointFormat)

      // Create an empty "_last_checkpoint" (corrupted)
      val fs = log.LAST_CHECKPOINT.getFileSystem(log.newDeltaHadoopConf())
      fs.create(log.LAST_CHECKPOINT, true /* overwrite */).close()

      // Create a new DeltaLog
      DeltaLog.clearCache()
      val log2 = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
      // Make sure we create a new DeltaLog in order to test the loading logic.
      assert(log ne log2)

      // We should get the same metadata even if "_last_checkpoint" is corrupted.
      assert(CheckpointInstance(log2.readLastCheckpointFile().get) ===
        CheckpointInstance(lastCheckpoint.version, SINGLE))
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
          FileNames.unsafeDeltaFile(log.logPath, 0L),
          Iterator(Action.supportedProtocolVersion(), Metadata(), add)
            .map(a => JsonUtils.toJson(a.wrap)),
          overwrite = false,
          log.newDeltaHadoopConf())
        log.store.write(
          FileNames.unsafeDeltaFile(log.logPath, 1L),
          Iterator(JsonUtils.toJson(rm.wrap)),
          overwrite = false,
          log.newDeltaHadoopConf())

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
          FileNames.unsafeDeltaFile(log.logPath, 0L),
          Iterator(Action.supportedProtocolVersion(), Metadata(), add)
            .map(a => JsonUtils.toJson(a.wrap)),
          overwrite = false,
          log.newDeltaHadoopConf())
        log.store.write(
          FileNames.unsafeDeltaFile(log.logPath, 1L),
          Iterator(JsonUtils.toJson(rm.wrap)),
          overwrite = false,
          log.newDeltaHadoopConf())

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
      val path = new File(dir, "a/b%c/d").toURI.toString
      val rm = RemoveFile(path, Some(System.currentTimeMillis()), dataChange = true)
      log.startTransaction().commitManually(rm)

      val committedRemove = log.update(stalenessAcceptable = false).tombstones.collect().head
      assert(committedRemove.path === path)
    }
  }

  test("delete and re-add the same file in different transactions") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(spark, dir)
      assert(new File(log.logPath.toUri).mkdirs())

      val add1 = createTestAddFile(modificationTime = System.currentTimeMillis())
      log.startTransaction().commitManually(add1)

      val rm = add1.remove
      log.startTransaction().commit(rm :: Nil, DeltaOperations.ManualUpdate)

      val add2 = createTestAddFile(modificationTime = System.currentTimeMillis())
      log.startTransaction().commit(add2 :: Nil, DeltaOperations.ManualUpdate)

      // Add a new transaction to replay logs using the previous snapshot. If it contained
      // AddFile("foo") and RemoveFile("foo"), "foo" would get removed and fail this test.
      val otherAdd =
        createTestAddFile(encodedPath = "bar", modificationTime = System.currentTimeMillis())
      log.startTransaction().commit(otherAdd :: Nil, DeltaOperations.ManualUpdate)

      assert(log.update().allFiles.collect().find(_.path == "foo")
        // `dataChange` is set to `false` after replaying logs.
        === Some(add2.copy(
          dataChange = false, baseRowId = Some(1), defaultRowCommitVersion = Some(2))))
    }
  }

  test("error - versions not contiguous") {
    withTempDir { dir =>
      val staleLog = DeltaLog.forTable(spark, dir)
      DeltaLog.clearCache()

      val log = DeltaLog.forTable(spark, dir)
      assert(new File(log.logPath.toUri).mkdirs())

      val metadata = Metadata()
      val add1 = AddFile("foo", Map.empty, 1L, System.currentTimeMillis(), dataChange = true)
      log.startTransaction().commit(metadata :: add1 :: Nil, DeltaOperations.ManualUpdate)

      val add2 = AddFile("bar", Map.empty, 1L, System.currentTimeMillis(), dataChange = true)
      log.startTransaction().commit(add2 :: Nil, DeltaOperations.ManualUpdate)

      val add3 = AddFile("baz", Map.empty, 1L, System.currentTimeMillis(), dataChange = true)
      log.startTransaction().commit(add3 :: Nil, DeltaOperations.ManualUpdate)

      new File(new Path(log.logPath, "00000000000000000001.json").toUri).delete()

      val ex = intercept[IllegalStateException] {
        staleLog.update()
      }
      assert(ex.getMessage.contains("Versions (0, 2) are not contiguous."))
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
          FileNames.unsafeDeltaFile(log.logPath, 0L),
          Iterator(selectedAction, file).map(a => JsonUtils.toJson(a.wrap)),
          overwrite = false,
          log.newDeltaHadoopConf())
        val e = intercept[IllegalStateException] {
          log.update()
        }
        assert(e.getMessage === DeltaErrors.actionNotFoundException(action, 0).getMessage)
      }
    }
  }

  Seq("protocol", "metadata").foreach { action =>
    testDifferentCheckpoints(s"state reconstruction from checkpoint with" +
        s" missing $action should fail", quiet = true) { (_, _) =>
      withTempDir { tempDir =>
        import testImplicits._
        val staleLog = DeltaLog.forTable(spark, tempDir)
        DeltaLog.clearCache()

        val log = DeltaLog.forTable(spark, tempDir)
        assert (staleLog != log)
        val checkpointInterval = log.checkpointInterval()
        // Create a checkpoint regularly
        for (f <- 0 to checkpointInterval) {
          val txn = log.startTransaction()
          val addFile = createTestAddFile(encodedPath = f.toString)
          if (f == 0) {
            txn.commitManually(addFile)
          } else {
            txn.commit(Seq(addFile), testOp)
          }
        }

        {
          // Create an incomplete checkpoint without the action and overwrite the
          // original checkpoint
          val checkpointPathOpt =
            log.listFrom(log.snapshot.version).find(FileNames.isCheckpointFile).map(_.getPath)
          assert(checkpointPathOpt.nonEmpty)
          assert(FileNames.checkpointVersion(checkpointPathOpt.get) === log.snapshot.version)
          val checkpointPath = checkpointPathOpt.get
          def removeActionFromParquetCheckpoint(tmpCheckpoint: File): Unit = {
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
          if (checkpointPath.getName.endsWith("json")) {
            val conf = log.newDeltaHadoopConf()
            val filteredActions = log.store
              .read(checkpointPath, log.newDeltaHadoopConf())
              .map(Action.fromJson)
              .filter {
                case _: Protocol => action != "protocol"
                case _: Metadata => action != "metadata"
                case _ => true
              }.map(_.json)
            log.store.write(checkpointPath, filteredActions.toIterator, overwrite = true, conf)
          } else {
            withTempDir { f => removeActionFromParquetCheckpoint(f) }
          }
        }

        // Verify if the state reconstruction from the checkpoint fails.
        val e = intercept[IllegalStateException] {
          staleLog.update()
        }
        assert(e.getMessage ===
          DeltaErrors.actionNotFoundException(action, checkpointInterval).getMessage)
      }
    }
  }

  testDifferentCheckpoints("deleting and recreating a directory should" +
      " cause the snapshot to be recomputed", quiet = true) { (_, _) =>
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(10).write.format("delta").mode("append").save(path)
      spark.range(10, 20).write.format("delta").mode("append").save(path)
      val deltaLog = DeltaLog.forTable(spark, path)
      deltaLog.checkpoint()
      spark.range(20, 30).write.format("delta").mode("append").save(path)

      // Store these for later usage
      val actions = deltaLog.snapshot.stateDS.collect()
      val commitTimestamp = deltaLog.snapshot.logSegment.lastCommitFileModificationTimestamp

      checkAnswer(
        spark.read.format("delta").load(path),
        spark.range(30).toDF()
      )

      val fs = deltaLog.logPath.getFileSystem(deltaLog.newDeltaHadoopConf())

      // Now let's delete the last version
      deltaLog.store
        .listFrom(
          FileNames.listingPrefix(deltaLog.logPath, deltaLog.snapshot.version),
          deltaLog.newDeltaHadoopConf())
        .filter(!_.getPath.getName.startsWith("_"))
        .foreach(f => fs.delete(f.getPath, true))
      if (coordinatedCommitsEnabledInTests) {
        // For Coordinated Commits table with a commit that is not backfilled, we can't use
        // 00000000002.json yet. Contact commit coordinator to get uuid file path to malform json
        // file.
        val oc = CommitCoordinatorProvider.getCommitCoordinatorClient(
          "tracking-in-memory", Map.empty[String, String], spark)
        val tableDesc =
          new TableDescriptor(deltaLog.logPath, Optional.empty(), Map.empty[String, String].asJava)
        val commitResponse = oc.getCommits(tableDesc, 2, null)
        if (!commitResponse.getCommits.isEmpty) {
          val path = commitResponse.getCommits.asScala.last.getFileStatus.getPath
          fs.delete(path, true)
        }
        // Also deletes it from in-memory commit coordinator.
        oc.asInstanceOf[TrackingCommitCoordinatorClient]
          .delegatingCommitCoordinatorClient
          .asInstanceOf[InMemoryCommitCoordinator]
          .removeCommitTestOnly(deltaLog.logPath, 2)
      }

      // Should show up to 20
      checkAnswer(
        spark.read.format("delta").load(path),
        spark.range(20).toDF()
      )

      // Now let's delete the checkpoint and json file for version 1. We will try to list from
      // version 1, but since we can't find anything, we should start listing from version 0
      deltaLog.store
        .listFrom(
          FileNames.listingPrefix(deltaLog.logPath, 1),
          deltaLog.newDeltaHadoopConf())
        .filter(!_.getPath.getName.startsWith("_"))
        .foreach(f => fs.delete(f.getPath, true))
      if (coordinatedCommitsEnabledInTests) {
        val oc = CommitCoordinatorProvider.getCommitCoordinatorClient(
          "tracking-in-memory",
          Map.empty[String, String],
          spark)
        oc.asInstanceOf[TrackingCommitCoordinatorClient]
          .delegatingCommitCoordinatorClient
          .asInstanceOf[InMemoryCommitCoordinator]
          .removeCommitTestOnly(deltaLog.logPath, 1)
      }
      checkAnswer(
        spark.read.format("delta").load(path),
        spark.range(10).toDF()
      )

      // Now let's delete that commit as well, and write a new first version
      deltaLog.listFrom(0)
        .filter(!_.getPath.getName.startsWith("_"))
        .foreach(f => fs.delete(f.getPath, false))

      assert(deltaLog.snapshot.version === 0)

      deltaLog.store.write(
        FileNames.unsafeDeltaFile(deltaLog.logPath, 0),
        actions.map(_.unwrap.json).iterator,
        overwrite = false,
        deltaLog.newDeltaHadoopConf())

      // To avoid flakiness, we manually set the modification timestamp of the file to a later
      // second
      new File(FileNames.unsafeDeltaFile(deltaLog.logPath, 0).toUri)
        .setLastModified(commitTimestamp + 5000)

      checkAnswer(
        spark.read.format("delta").load(path),
        spark.range(30).toDF()
      )
    }
  }

  test("forTableWithSnapshot should always return the latest snapshot") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(10).write.format("delta").mode("append").save(path)
      val deltaLog = DeltaLog.forTable(spark, path)
      assert(deltaLog.snapshot.version === 0)

      val (_, snapshot) = DeltaLog.withFreshSnapshot { _ =>
        // This update is necessary to advance the lastUpdatedTs beyond the start time of
        // withFreshSnapshot call.
        deltaLog.update()
        // Manually add a commit. However, the deltaLog should now be fresh enough
        // that we don't trigger another update, and thus don't find the commit.
        val add = AddFile(path, Map.empty, 100L, 10L, dataChange = true)
        deltaLog.store.write(
          FileNames.unsafeDeltaFile(deltaLog.logPath, 1L),
          Iterator(JsonUtils.toJson(add.wrap)),
          overwrite = false,
          deltaLog.newDeltaHadoopConf())
        deltaLog
      }
      assert(snapshot.version === 0)

      val deltaLog2 = DeltaLog.forTable(spark, path)
      assert(deltaLog2.snapshot.version === 0) // This shouldn't update
      val (_, snapshot2) = DeltaLog.forTableWithSnapshot(spark, path)
      assert(snapshot2.version === 1) // This should get the latest snapshot
    }
  }

  test("Delta log should handle malformed json") {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    def testJsonCommitParser(
        path: String, func: Map[String, Map[String, String]] => String): Unit = {
      spark.range(10).write.format("delta").mode("append").save(path)
      spark.range(1).write.format("delta").mode("append").save(path)

      val log = DeltaLog.forTable(spark, path)
      var commitFilePath = FileNames.unsafeDeltaFile(log.logPath, 1L)
      if (coordinatedCommitsEnabledInTests) {
        // For Coordinated Commits table with a commit that is not backfilled, we can't use
        // 00000000001.json yet. Contact commit coordinator to get uuid file path to malform json
        // file.
        val oc = CommitCoordinatorProvider.getCommitCoordinatorClient(
          "tracking-in-memory", Map.empty[String, String], spark)
        val tableDesc =
          new TableDescriptor(log.logPath, Optional.empty(), Map.empty[String, String].asJava)
        val commitResponse = oc.getCommits(tableDesc, 1, null)
        if (!commitResponse.getCommits.isEmpty) {
          commitFilePath = commitResponse.getCommits.asScala.head.getFileStatus.getPath
        }
      }
      val fs = log.logPath.getFileSystem(log.newDeltaHadoopConf())
      val stream = fs.open(commitFilePath)
      val reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))
      val commitInfo = reader.readLine() + "\n"
      val addFile = reader.readLine()
      stream.close()

      val map = mapper.readValue(addFile, classOf[Map[String, Map[String, String]]])
      val output = fs.create(commitFilePath, true)
      output.write(commitInfo.getBytes(StandardCharsets.UTF_8))
      output.write(func(map).getBytes(StandardCharsets.UTF_8))
      output.close()
      DeltaLog.clearCache()

      val parser = JsonToStructs(
        schema = Action.logSchema,
        options = DeltaLog.jsonCommitParseOption,
        child = null,
        timeZoneId = Some(spark.sessionState.conf.sessionLocalTimeZone))

      val it = log.store.readAsIterator(commitFilePath, log.newDeltaHadoopConf())
      try {
        it.foreach { json =>
          val utf8json = UTF8String.fromString(json)
          parser.nullSafeEval(utf8json).asInstanceOf[InternalRow]
        }
      } finally {
        it.close()
      }
    }

    // Parser should succeed when AddFile in json commit has missing fields
    withTempDir { dir =>
      testJsonCommitParser(dir.toString, (content: Map[String, Map[String, String]]) => {
        mapper.writeValueAsString(Map("add" -> content("add").-("path").-("size"))) + "\n"
      })
    }

    // Parser should succeed when AddFile in json commit has extra fields
    withTempDir { dir =>
      testJsonCommitParser(dir.toString, (content: Map[String, Map[String, String]]) => {
        mapper.writeValueAsString(Map("add" -> content("add"). +("random" -> "field"))) + "\n"
      })
    }

    // Parser should succeed when AddFile in json commit has mismatched schema
    withTempDir { dir =>
      val json = """{"x": 1, "y": 2, "z": [10, 20]}"""
      testJsonCommitParser(dir.toString, (content: Map[String, Map[String, String]]) => {
        mapper.writeValueAsString(Map("add" -> content("add").updated("path", json))) + "\n"
      })
    }

    // Parser should throw exception when AddFile is a bad json
    withTempDir { dir =>
      val e = intercept[Throwable] {
        testJsonCommitParser(dir.toString, (content: Map[String, Map[String, String]]) => {
          "bad json{{{"
        })
      }
      assert(e.getMessage.contains("FAILFAST"))
    }
  }

  test("DeltaLog cache size should honor config limit") {
    def assertCacheSize(expected: Long): Unit = {
      for (_ <- 1 to 6) {
        withTempDir(dir => {
          val path = dir.getCanonicalPath
          spark.range(10).write.format("delta").mode("append").save(path)
        })
      }
      assert(DeltaLog.cacheSize === expected)
    }
    DeltaLog.unsetCache()
    withSQLConf(DeltaSQLConf.DELTA_LOG_CACHE_SIZE.key -> "4") {
      assertCacheSize(4)
      DeltaLog.unsetCache()
      // the larger of SQLConf and env var is adopted
      try {
        System.getProperties.setProperty("delta.log.cacheSize", "5")
        assertCacheSize(5)
      } finally {
        System.getProperties.remove("delta.log.cacheSize")
      }
    }

    // assert timeconf returns correct value
    withSQLConf(DeltaSQLConf.DELTA_LOG_CACHE_RETENTION_MINUTES.key -> "100") {
      assert(spark.sessionState.conf.getConf(
        DeltaSQLConf.DELTA_LOG_CACHE_RETENTION_MINUTES) === 100)
    }
  }

  test("DeltaLog should create log directory when ensureLogDirectory is called") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      val log = DeltaLog.forTable(spark, new Path(path))
      log.createLogDirectoriesIfNotExists()

      val logPath = log.logPath
      val fs = logPath.getFileSystem(log.newDeltaHadoopConf())
      assert(fs.exists(logPath), "Log path should exist.")
      assert(fs.getFileStatus(logPath).isDirectory, "Log path should be a directory")
      val commitPath = FileNames.commitDirPath(logPath)
      assert(fs.exists(commitPath), "Commit path should exist.")
      assert(fs.getFileStatus(commitPath).isDirectory, "Commit path should be a directory")
    }
  }

  test("DeltaLog should throw exception when unable to create log directory " +
      "with filesystem IO Exception") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      val log = DeltaLog.forTable(spark, new Path(path))
      val fs = log.logPath.getFileSystem(log.newDeltaHadoopConf())

      // create a file in place of what should be the directory.
      // Attempting to create a child file/directory should fail and throw an IOException.
      fs.create(log.logPath)

      val e = intercept[DeltaIOException] {
        log.createLogDirectoriesIfNotExists()
      }
      checkError(e, "DELTA_CANNOT_CREATE_LOG_PATH")
      e.getCause match {
        case e: IOException =>
          assert(e.getMessage.contains("Parent path is not a directory"))
        case _ =>
          fail(s"Expected IOException, got ${e.getCause}")
      }
    }
  }

  test("DeltaFileProviderUtils.getDeltaFilesInVersionRange") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(0, 1).write.format("delta").mode("overwrite").save(path)
      spark.range(0, 1).write.format("delta").mode("overwrite").save(path)
      spark.range(0, 1).write.format("delta").mode("overwrite").save(path)
      spark.range(0, 1).write.format("delta").mode("overwrite").save(path)
      val log = DeltaLog.forTable(spark, new Path(path))
      val result = DeltaFileProviderUtils.getDeltaFilesInVersionRange(
        spark, log, startVersion = 1, endVersion = 3)
      assert(result.map(FileNames.getFileVersion) === Seq(1, 2, 3))
      val filesAreUnbackfilledArray = result.map(FileNames.isUnbackfilledDeltaFile)

      val (fileV1, fileV2, fileV3) = (result(0), result(1), result(2))
      assert(FileNames.getFileVersion(fileV1) === 1)
      assert(FileNames.getFileVersion(fileV2) === 2)
      assert(FileNames.getFileVersion(fileV3) === 3)

      val backfillInterval = coordinatedCommitsBackfillBatchSize.getOrElse(0L)
      if (backfillInterval == 0 || backfillInterval == 1) {
        assert(filesAreUnbackfilledArray === Seq(false, false, false))
      } else if (backfillInterval == 2) {
        assert(filesAreUnbackfilledArray === Seq(false, false, true))
      } else {
        assert(filesAreUnbackfilledArray === Seq(true, true, true))
      }
    }
  }

}

class CoordinatedCommitsBatchBackfill1DeltaLogSuite extends DeltaLogSuite {
  override def coordinatedCommitsBackfillBatchSize: Option[Int] = Some(1)
}

class CoordinatedCommitsBatchBackfill2DeltaLogSuite extends DeltaLogSuite {
  override def coordinatedCommitsBackfillBatchSize: Option[Int] = Some(2)
}

class CoordinatedCommitsBatchBackfill100DeltaLogSuite extends DeltaLogSuite {
  override def coordinatedCommitsBackfillBatchSize: Option[Int] = Some(100)
}
