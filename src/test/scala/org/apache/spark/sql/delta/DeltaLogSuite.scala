/*
 * Copyright 2019 Databricks, Inc.
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

import org.apache.spark.sql.delta.DeltaOperations.Truncate
import org.apache.spark.sql.delta.actions._
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
    val log1 = DeltaLog(spark, new Path(tempDir.getCanonicalPath))

    (1 to 15).foreach { i =>
      val txn = log1.startTransaction()
      val file = AddFile(i.toString, Map.empty, 1, 1, true) :: Nil
      val delete: Seq[Action] = if (i > 1) {
        RemoveFile(i - 1 toString, Some(System.currentTimeMillis()), true) :: Nil
      } else {
        Nil
      }
      txn.commit(delete ++ file, testOp)
    }

    DeltaLog.clearCache()
    val log2 = DeltaLog(spark, new Path(tempDir.getCanonicalPath))
    assert(log2.snapshot.version == log1.snapshot.version)
    assert(log2.snapshot.allFiles.count == 1)
  }

  testQuietly("SC-8078: update deleted directory") {
    withTempDir { dir =>
      val path = new Path(dir.getCanonicalPath)
      val log = DeltaLog(spark, path)

      // Commit data so the in-memory state isn't consistent with an empty log.
      val txn = log.startTransaction()
      val files = (1 to 10).map(f => AddFile(f.toString, Map.empty, 1, 1, true))
      txn.commit(files, testOp)
      log.checkpoint()

      val fs = path.getFileSystem(spark.sessionState.newHadoopConf())
      fs.delete(path, true)

      val thrown = intercept[FileNotFoundException] {
        log.update()
      }
      assert(thrown.getMessage().contains("No delta log found"))
    }
  }

  testQuietly("update should pick up checkpoints") {
    withTempDir { tempDir =>
      val log = DeltaLog(spark, new Path(tempDir.getCanonicalPath))
      val checkpointInterval = log.checkpointInterval
      for (f <- 0 until (checkpointInterval * 2)) {
        val txn = log.startTransaction()
        txn.commit(Seq(AddFile(f.toString, Map.empty, 1, 1, true)), testOp)
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
      val logs = samePaths.map(DeltaLog(spark, _))
      logs.foreach { log =>
        assert(log eq logs.head)
      }
    }
  }

  testQuietly("handle corrupted '_last_checkpoint' file") {
    withTempDir { tempDir =>
      val log = DeltaLog(spark, new Path(tempDir.getCanonicalPath))

      val checkpointInterval = log.checkpointInterval
      for (f <- 0 to checkpointInterval) {
        val txn = log.startTransaction()
        txn.commit(Seq(AddFile(f.toString, Map.empty, 1, 1, true)), testOp)
      }
      assert(log.lastCheckpoint.isDefined)

      val lastCheckpoint = log.lastCheckpoint.get

      // Create an empty "_last_checkpoint" (corrupted)
      val fs = log.LAST_CHECKPOINT.getFileSystem(spark.sessionState.newHadoopConf)
      fs.create(log.LAST_CHECKPOINT, true /* overwrite */).close()

      // Create a new DeltaLog
      DeltaLog.clearCache()
      val log2 = DeltaLog(spark, new Path(tempDir.getCanonicalPath))
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
          Iterator(JsonUtils.toJson(add.wrap)))
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
          Iterator(JsonUtils.toJson(add.wrap)))
        log.store.write(
          FileNames.deltaFile(log.logPath, 1L),
          Iterator(JsonUtils.toJson(rm.wrap)))

        assert(log.update().version === 1)
        assert(log.snapshot.numOfFiles === 0)
      }
    }
  }

  test("do not relativize paths in RemoveFiles") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(spark, dir)
      assert(new File(log.logPath.toUri).mkdirs())
      val path = new File(dir, "a/b/c").getCanonicalPath
      val rm = RemoveFile(path, Some(System.currentTimeMillis()), dataChange = true)
      log.startTransaction().commit(rm :: Nil, DeltaOperations.ManualUpdate)

      val committedRemove = log.update(stalenessAcceptable = false).tombstones.collect()
      assert(committedRemove.head.path === s"file://$path")
    }
  }

  test("delete and re-add the same file in different transactions") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(spark, dir)
      assert(new File(log.logPath.toUri).mkdirs())

      val add1 = AddFile("foo", Map.empty, 1L, System.currentTimeMillis(), dataChange = true)
      log.startTransaction().commit(add1 :: Nil, DeltaOperations.ManualUpdate)

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
}
