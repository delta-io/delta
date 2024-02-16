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

import java.io.{File, IOException}
import java.net.URI
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.DeltaTestUtils.createTestAddFile
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.storage._
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, FSDataOutputStream, Path, RawLocalFileSystem}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.{LocalSparkSession, QueryTest, SparkSession}
import org.apache.spark.sql.LocalSparkSession.withSparkSession
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

/////////////////////
// Base Test Suite //
/////////////////////

abstract class LogStoreSuiteBase extends QueryTest
  with LogStoreProvider
  with SharedSparkSession
  with DeltaSQLCommandTest {

  def logStoreClassName: String

  protected override def sparkConf = {
    super.sparkConf.set(logStoreClassConfKey, logStoreClassName)
  }

  // scalastyle:off deltahadoopconfiguration
  def sessionHadoopConf: Configuration = spark.sessionState.newHadoopConf
  // scalastyle:on deltahadoopconfiguration

  protected def testInitFromSparkConf(): Unit = {
    test("instantiation through SparkConf") {
      assert(spark.sparkContext.getConf.get(logStoreClassConfKey) == logStoreClassName)
      assert(LogStore(spark).getClass.getName == logStoreClassName)
    }
  }

  testInitFromSparkConf()

  protected def withTempLogDir(f: File => Unit): Unit = {
    val dir = Utils.createTempDir()
    val deltaLogDir = new File(dir, "_delta_log")
    deltaLogDir.mkdir()
    try f(deltaLogDir) finally {
      Utils.deleteRecursively(dir)
    }
  }

  test("read / write") {
    def assertNoLeakedCrcFiles(dir: File): Unit = {
      // crc file should not be leaked when origin file doesn't exist.
      // The implementation of Hadoop filesystem may filter out checksum file, so
      // listing files from local filesystem.
      val fileNames = dir.listFiles().toSeq.filter(p => p.isFile).map(p => p.getName)
      val crcFiles = fileNames.filter(n => n.startsWith(".") && n.endsWith(".crc"))
      val originFileNamesForExistingCrcFiles = crcFiles.map { name =>
        // remove first "." and last ".crc"
        name.substring(1, name.length - 4)
      }

      // Check all origin files exist for all crc files.
      assert(originFileNamesForExistingCrcFiles.toSet.subsetOf(fileNames.toSet),
        s"Some of origin files for crc files don't exist - crc files: $crcFiles / " +
          s"expected origin files: $originFileNamesForExistingCrcFiles / actual files: $fileNames")
    }

    def pathToFileStatus(path: Path): FileStatus =
      path.getFileSystem(sessionHadoopConf).getFileStatus(path)

    withTempLogDir { tempLogDir =>
      val store = createLogStore(spark)
      val deltas = Seq(0, 1)
        .map(i => new File(tempLogDir, i.toString)).map(_.toURI).map(new Path(_))
      store.write(deltas.head, Iterator("zero", "none"), overwrite = false, sessionHadoopConf)
      store.write(deltas(1), Iterator("one"), overwrite = false, sessionHadoopConf)

      // Test Path based read APIs
      assert(store.read(deltas.head, sessionHadoopConf) == Seq("zero", "none"))
      assert(store.readAsIterator(deltas.head, sessionHadoopConf).toSeq == Seq("zero", "none"))
      assert(store.read(deltas(1), sessionHadoopConf) == Seq("one"))
      assert(store.readAsIterator(deltas(1), sessionHadoopConf).toSeq == Seq("one"))
      // Test FileStatus based read APIs
      assert(store.read(pathToFileStatus(deltas.head), sessionHadoopConf) == Seq("zero", "none"))
      assert(store.readAsIterator(pathToFileStatus(deltas.head), sessionHadoopConf).toSeq ==
        Seq("zero", "none"))
      assert(store.read(pathToFileStatus(deltas(1)), sessionHadoopConf) == Seq("one"))
      assert(store.readAsIterator(pathToFileStatus(deltas(1)), sessionHadoopConf).toSeq ==
        Seq("one"))

      assertNoLeakedCrcFiles(tempLogDir)
    }

  }

  test("detects conflict") {
    withTempLogDir { tempLogDir =>
      val store = createLogStore(spark)
      val deltas = Seq(0, 1)
        .map(i => new File(tempLogDir, i.toString)).map(_.toURI).map(new Path(_))
      store.write(deltas.head, Iterator("zero"), overwrite = false, sessionHadoopConf)
      store.write(deltas(1), Iterator("one"), overwrite = false, sessionHadoopConf)

      intercept[java.nio.file.FileAlreadyExistsException] {
        store.write(deltas(1), Iterator("uno"), overwrite = false, sessionHadoopConf)
      }
    }

  }

  test("listFrom") {
    withTempLogDir { tempLogDir =>
      val store = createLogStore(spark)

      val deltas =
        Seq(0, 1, 2, 3, 4).map(i => new File(tempLogDir, i.toString)).map(_.toURI).map(new Path(_))
      store.write(deltas(1), Iterator("zero"), overwrite = false, sessionHadoopConf)
      store.write(deltas(2), Iterator("one"), overwrite = false, sessionHadoopConf)
      store.write(deltas(3), Iterator("two"), overwrite = false, sessionHadoopConf)

      assert(
        store.listFrom(deltas.head, sessionHadoopConf)
          .map(_.getPath.getName).toArray === Seq(1, 2, 3).map(_.toString))
      assert(
        store.listFrom(deltas(1), sessionHadoopConf)
          .map(_.getPath.getName).toArray === Seq(1, 2, 3).map(_.toString))
      assert(store.listFrom(deltas(2), sessionHadoopConf)
        .map(_.getPath.getName).toArray === Seq(2, 3).map(_.toString))
      assert(store.listFrom(deltas(3), sessionHadoopConf)
        .map(_.getPath.getName).toArray === Seq(3).map(_.toString))
      assert(store.listFrom(deltas(4), sessionHadoopConf).map(_.getPath.getName).toArray === Nil)
    }
  }

  test("simple log store test") {
    val tempDir = Utils.createTempDir()
    val log1 = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
    assert(log1.store.getClass.getName == logStoreClassName)

    val txn = log1.startTransaction()
    txn.commitManually(createTestAddFile())
    log1.checkpoint()

    DeltaLog.clearCache()
    val log2 = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
    assert(log2.store.getClass.getName == logStoreClassName)

    assert(log2.readLastCheckpointFile().map(_.version) === Some(0L))
    assert(log2.snapshot.allFiles.count == 1)
  }

  protected def testHadoopConf(expectedErrMsg: String, fsImplConfs: (String, String)*): Unit = {
    test("should pick up fs impl conf from session Hadoop configuration") {
      withTempDir { tempDir =>
        // scalastyle:off pathfromuri
        val path = new Path(new URI(s"fake://${tempDir.toURI.getRawPath}/1.json"))
        // scalastyle:on pathfromuri

        // Make sure it will fail without FakeFileSystem
        val e = intercept[IOException] {
          createLogStore(spark).listFrom(path, sessionHadoopConf)
        }
        assert(e.getMessage.matches(expectedErrMsg))
        withSQLConf(fsImplConfs: _*) {
          createLogStore(spark).listFrom(path, sessionHadoopConf)
        }
      }
    }
  }

  /**
   * Whether the log store being tested should use rename to write checkpoint or not. The following
   * test is using this method to verify the behavior of `checkpoint`.
   */
  protected def shouldUseRenameToWriteCheckpoint: Boolean

  test(
    "use isPartialWriteVisible to decide whether use rename") {
    withTempDir { tempDir =>
      import testImplicits._
      // Write 5 files to delta table
      (1 to 100).toDF().repartition(5).write.format("delta").save(tempDir.getCanonicalPath)
      withSQLConf(
          "fs.file.impl" -> classOf[TrackingRenameFileSystem].getName,
          "fs.file.impl.disable.cache" -> "true") {
        val deltaLog = DeltaLog.forTable(spark, tempDir.getCanonicalPath)
        TrackingRenameFileSystem.renameCounter.set(0)
        deltaLog.checkpoint()
        val expectedNumOfRename = if (shouldUseRenameToWriteCheckpoint) 1 else 0
        assert(TrackingRenameFileSystem.renameCounter.get() === expectedNumOfRename)

        withSQLConf(DeltaSQLConf.DELTA_CHECKPOINT_PART_SIZE.key -> "9") {
          // Write 5 more files to the delta table
          (1 to 100).toDF().repartition(5).write
            .format("delta").mode("append").save(tempDir.getCanonicalPath)
          // At this point table has total 10 files, which won't fit in 1 checkpoint part file (as
          // DELTA_CHECKPOINT_PART_SIZE is set to 9 in this test). So this will end up generating
          // 2 PART files.
          TrackingRenameFileSystem.renameCounter.set(0)
          deltaLog.checkpoint()
          val expectedNumOfRename = if (shouldUseRenameToWriteCheckpoint) 2 else 0
          assert(TrackingRenameFileSystem.renameCounter.get() === expectedNumOfRename)
        }
      }
    }
  }

  test("readAsIterator should be lazy") {
    withTempLogDir { tempLogDir =>
      val store = createLogStore(spark)
      val testFile = new File(tempLogDir, "readAsIterator").getCanonicalPath
      store.write(new Path(testFile), Iterator("foo", "bar"), overwrite = false, sessionHadoopConf)

      withSQLConf(
          "fs.fake.impl" -> classOf[FakeFileSystem].getName,
          "fs.fake.impl.disable.cache" -> "true") {
        val fsStats = FileSystem.getStatistics("fake", classOf[FakeFileSystem])
        fsStats.reset()
        val iter = store.readAsIterator(new Path(s"fake:///$testFile"), sessionHadoopConf)
        try {
          // We should not read any date when creating the iterator.
          assert(fsStats.getBytesRead == 0)
          assert(iter.toList == "foo" :: "bar" :: Nil)
          // Verify we are using the correct Statistics instance.
          assert(fsStats.getBytesRead == 8)
        } finally {
          iter.close()
        }
      }
    }
  }
}
