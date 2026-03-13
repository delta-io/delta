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

///////////////////////////
// Child-specific traits //
///////////////////////////

trait AzureLogStoreSuiteBase extends LogStoreSuiteBase {

  testHadoopConf(
    expectedErrMsg = ".*No FileSystem for scheme.*fake.*",
    "fs.fake.impl" -> classOf[FakeFileSystem].getName,
    "fs.fake.impl.disable.cache" -> "true")

  protected def shouldUseRenameToWriteCheckpoint: Boolean = true
}

trait HDFSLogStoreSuiteBase extends LogStoreSuiteBase {

  // HDFSLogStore is based on FileContext APIs and hence requires AbstractFileSystem-based
  // implementations.
  testHadoopConf(
    expectedErrMsg = ".*No FileSystem for scheme.*fake.*",
    "fs.fake.impl" -> classOf[FakeFileSystem].getName,
    "fs.fake.impl.disable.cache" -> "true")

  import testImplicits._

  test("writes on systems without AbstractFileSystem implemented") {
    withSQLConf("fs.fake.impl" -> classOf[FakeFileSystem].getName,
      "fs.fake.impl.disable.cache" -> "true") {
      val tempDir = Utils.createTempDir()
      // scalastyle:off pathfromuri
      val path = new Path(new URI(s"fake://${tempDir.toURI.getRawPath}/1.json"))
      // scalastyle:on pathfromuri
      val e = intercept[IOException] {
        createLogStore(spark)
          .write(path, Iterator("zero", "none"), overwrite = false, sessionHadoopConf)
      }
      assert(e.getMessage
        .contains("The error typically occurs when the default LogStore implementation"))
    }
  }

  test("reads should work on systems without AbstractFileSystem implemented") {
    withTempDir { tempDir =>
      val writtenFile = new File(tempDir, "1")
      val store = createLogStore(spark)
      store.write(
        new Path(writtenFile.getCanonicalPath),
        Iterator("zero", "none"),
        overwrite = false,
        sessionHadoopConf)
      withSQLConf("fs.fake.impl" -> classOf[FakeFileSystem].getName,
        "fs.fake.impl.disable.cache" -> "true") {
        val read = createLogStore(spark)
          .read(new Path("fake://" + writtenFile.getCanonicalPath), sessionHadoopConf)
        assert(read === ArrayBuffer("zero", "none"))
      }
    }
  }

  test(
    "No AbstractFileSystem - end to end test using data frame") {
    // Writes to the fake file system will fail
    withTempDir { tempDir =>
      val fakeFSLocation = s"fake://${tempDir.getCanonicalFile}"
      withSQLConf("fs.fake.impl" -> classOf[FakeFileSystem].getName,
        "fs.fake.impl.disable.cache" -> "true") {
        val e = intercept[IOException] {
          Seq(1, 2, 4).toDF().write.format("delta").save(fakeFSLocation)
        }
        assert(e.getMessage
          .contains("The error typically occurs when the default LogStore implementation"))
      }
    }
    // Reading files written by other systems will work.
    withTempDir { tempDir =>
      Seq(1, 2, 4).toDF().write.format("delta").save(tempDir.getAbsolutePath)
      withSQLConf("fs.fake.impl" -> classOf[FakeFileSystem].getName,
        "fs.fake.impl.disable.cache" -> "true") {
        val fakeFSLocation = s"fake://${tempDir.getCanonicalFile}"
        checkAnswer(spark.read.format("delta").load(fakeFSLocation), Seq(1, 2, 4).toDF())
      }
    }
  }

  test("if fc.rename() fails, it should throw java.nio.file.FileAlreadyExistsException") {
    withTempDir { tempDir =>
      withSQLConf(
        "fs.AbstractFileSystem.fake.impl" -> classOf[FailingRenameAbstractFileSystem].getName,
        "fs.fake.impl" -> classOf[FakeFileSystem].getName,
        "fs.fake.impl.disable.cache" -> "true") {
        val store = createLogStore(spark)
        val commit0 = new Path(s"fake://${tempDir.getCanonicalPath}/00000.json")

        intercept[java.nio.file.FileAlreadyExistsException] {
          store.write(commit0, Iterator("zero"), overwrite = false, sessionHadoopConf)
        }
      }
    }
  }

  test("Read after write consistency with msync") {
     withTempDir { tempDir =>
      val tsFSLocation = s"ts://${tempDir.getCanonicalFile}"
      // Use the file scheme so that it uses a different FileSystem cached object
      withSQLConf(
        ("fs.ts.impl", classOf[TimestampLocalFileSystem].getCanonicalName),
        ("fs.AbstractFileSystem.ts.impl",
          classOf[TimestampAbstractFileSystem].getCanonicalName)) {
        val store = createLogStore(spark)
        val path = new Path(tsFSLocation, "1.json")

        // Initialize the TimestampLocalFileSystem object which will be reused later due to the
        // FileSystem cache
        assert(store.listFrom(path, sessionHadoopConf).length == 0)

        store.write(path, Iterator("zero", "none"), overwrite = false, sessionHadoopConf)
        // Verify `msync` is called by checking whether `listFrom` returns the latest result.
        // Without the `msync` call, the TimestampLocalFileSystem would not see this file.
        assert(store.listFrom(path, sessionHadoopConf).length == 1)
      }
    }
  }

  protected def shouldUseRenameToWriteCheckpoint: Boolean = true
}

trait LocalLogStoreSuiteBase extends LogStoreSuiteBase {
  testHadoopConf(
    expectedErrMsg = ".*No FileSystem for scheme.*fake.*",
    "fs.fake.impl" -> classOf[FakeFileSystem].getName,
    "fs.fake.impl.disable.cache" -> "true")

  protected def shouldUseRenameToWriteCheckpoint: Boolean = true
}

trait GCSLogStoreSuiteBase extends LogStoreSuiteBase {

  testHadoopConf(
    expectedErrMsg = ".*No FileSystem for scheme.*fake.*",
    "fs.fake.impl" -> classOf[FakeFileSystem].getName,
    "fs.fake.impl.disable.cache" -> "true")

  protected def shouldUseRenameToWriteCheckpoint: Boolean = false

  test("gcs write should happen in a new thread") {
    withTempDir { tempDir =>
      // Use `FakeGCSFileSystemValidatingCommits` to verify we write in the correct thread.
      withSQLConf(
        "fs.gs.impl" -> classOf[FakeGCSFileSystemValidatingCommits].getName,
        "fs.gs.impl.disable.cache" -> "true") {
        val store = createLogStore(spark)
        store.write(
          new Path(s"gs://${tempDir.getCanonicalPath}", "1.json"),
          Iterator("foo"),
          overwrite = false,
          sessionHadoopConf)
      }
    }
  }

  test("handles precondition failure") {
    withTempDir { tempDir =>
      withSQLConf(
        "fs.gs.impl" -> classOf[FailingGCSFileSystem].getName,
        "fs.gs.impl.disable.cache" -> "true") {
        val store = createLogStore(spark)

        assertThrows[java.nio.file.FileAlreadyExistsException] {
          store.write(
            new Path(s"gs://${tempDir.getCanonicalPath}", "1.json"),
            Iterator("foo"),
            overwrite = false,
            sessionHadoopConf)
        }

        store.write(
          new Path(s"gs://${tempDir.getCanonicalPath}", "1.json"),
          Iterator("foo"),
          overwrite = true,
          sessionHadoopConf)
      }
    }
  }
}

////////////////////////////////
// File System Helper Classes //
////////////////////////////////

/** A fake file system to test whether GCSLogStore properly handles precondition failures. */
class FailingGCSFileSystem extends RawLocalFileSystem {
  override def getScheme: String = "gs"
  override def getUri: URI = URI.create("gs:/")

  override def create(f: Path, overwrite: Boolean): FSDataOutputStream = {
    throw new IOException("412 Precondition Failed");
  }
}

/**
 * A fake AbstractFileSystem to test whether session Hadoop configuration will be picked up.
 * This is a wrapper around [[FakeFileSystem]].
 */
class FakeAbstractFileSystem(uri: URI, conf: org.apache.hadoop.conf.Configuration)
  extends org.apache.hadoop.fs.DelegateToFileSystem(
    uri,
    new FakeFileSystem,
    conf,
    FakeFileSystem.scheme,
    false) {

  // Implementation copied from RawLocalFs
  import org.apache.hadoop.fs.local.LocalConfigKeys
  import org.apache.hadoop.fs._

  override def getUriDefaultPort(): Int = -1
  override def getServerDefaults(): FsServerDefaults = LocalConfigKeys.getServerDefaults
  override def isValidName(src: String): Boolean = true
}

/**
 * A file system allowing to track how many times `rename` is called.
 * `TrackingRenameFileSystem.numOfRename` should be reset to 0 before starting to trace.
 */
class TrackingRenameFileSystem extends RawLocalFileSystem {
  override def rename(src: Path, dst: Path): Boolean = {
    TrackingRenameFileSystem.renameCounter.incrementAndGet()
    super.rename(src, dst)
  }
}

object TrackingRenameFileSystem {
  val renameCounter = new AtomicInteger(0)
  def resetCounter(): Unit = renameCounter.set(0)
}

/**
 * A fake AbstractFileSystem to ensure FileSystem.renameInternal(), and thus FileContext.rename(),
 * fails. This will be used to test HDFSLogStore.writeInternal corner case.
 */
class FailingRenameAbstractFileSystem(uri: URI, conf: org.apache.hadoop.conf.Configuration)
  extends FakeAbstractFileSystem(uri, conf) {

  override def renameInternal(src: Path, dst: Path, overwrite: Boolean): Unit = {
    throw new org.apache.hadoop.fs.FileAlreadyExistsException(s"$dst path already exists")
  }
}

////////////////////////////////////////////////////////////////////
// Public LogStore (Java) suite tests from delta-storage artifact //
////////////////////////////////////////////////////////////////////

abstract class PublicLogStoreSuite extends LogStoreSuiteBase {

  protected val publicLogStoreClassName: String

  // The actual type of LogStore created will be LogStoreAdaptor.
  override val logStoreClassName: String = classOf[LogStoreAdaptor].getName

  protected override def sparkConf = {
    super.sparkConf.set(logStoreClassConfKey, publicLogStoreClassName)
  }

  protected override def testInitFromSparkConf(): Unit = {
    test("instantiation through SparkConf") {
      assert(spark.sparkContext.getConf.get(logStoreClassConfKey) == publicLogStoreClassName)
      assert(LogStore(spark).getClass.getName == logStoreClassName)
      assert(LogStore(spark).asInstanceOf[LogStoreAdaptor]
        .logStoreImpl.getClass.getName == publicLogStoreClassName)

    }
  }
}

class PublicHDFSLogStoreSuite extends PublicLogStoreSuite with HDFSLogStoreSuiteBase {
  override protected val publicLogStoreClassName: String =
    classOf[io.delta.storage.HDFSLogStore].getName
}

class PublicS3SingleDriverLogStoreSuite extends PublicLogStoreSuite
  with S3SingleDriverLogStoreSuiteBase {

  override protected val publicLogStoreClassName: String =
    classOf[io.delta.storage.S3SingleDriverLogStore].getName
}

class PublicAzureLogStoreSuite extends PublicLogStoreSuite with AzureLogStoreSuiteBase {
  override protected val publicLogStoreClassName: String =
    classOf[io.delta.storage.AzureLogStore].getName
}

class PublicLocalLogStoreSuite extends PublicLogStoreSuite with LocalLogStoreSuiteBase {
  override protected val publicLogStoreClassName: String =
    classOf[io.delta.storage.LocalLogStore].getName
}

class PublicGCSLogStoreSuite extends PublicLogStoreSuite with GCSLogStoreSuiteBase {
  override protected val publicLogStoreClassName: String =
    classOf[io.delta.storage.GCSLogStore].getName
}
