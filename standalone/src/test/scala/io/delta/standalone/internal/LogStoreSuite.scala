/*
 * Copyright (2020-present) The Delta Lake Project Authors.
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

package io.delta.standalone.internal

import java.io.File

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path, RawLocalFileSystem}
import org.scalatest.FunSuite

import io.delta.standalone.Operation
import io.delta.standalone.actions.{AddFile => AddFileJ, Metadata => MetadataJ}
import io.delta.standalone.data.{CloseableIterator => CloseableIteratorJ}
import io.delta.standalone.storage.LogStore

import io.delta.standalone.internal.sources.StandaloneHadoopConf
import io.delta.standalone.internal.storage.{AzureLogStore, HDFSLogStore, LocalLogStore, LogStoreProvider, S3SingleDriverLogStore}
import io.delta.standalone.internal.util.TestUtils._

abstract class LogStoreSuiteBase extends FunSuite with LogStoreProvider {

  def logStoreClassName: Option[String]

  def hadoopConf: Configuration = {
    val conf = new Configuration()
    if (logStoreClassName.isDefined) {
      conf.set(StandaloneHadoopConf.LOG_STORE_CLASS_KEY, logStoreClassName.get)
    }
    conf
  }

  /**
   * Whether the log store being tested should use rename to write checkpoint or not. The following
   * test is using this method to verify the behavior of `checkpoint`.
   */
  protected def shouldUseRenameToWriteCheckpoint: Boolean

  test("instantiation through HadoopConf") {
    val expectedClassName = logStoreClassName.getOrElse(LogStoreProvider.defaultLogStoreClassName)
    assert(createLogStore(hadoopConf).getClass.getName == expectedClassName)
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

    withTempDir { dir =>
      import io.delta.standalone.internal.util.Implicits._

      val store = createLogStore(hadoopConf)

      val deltas = Seq(0, 1).map(i => new File(dir, i.toString)).map(_.getCanonicalPath)
      store.write(new Path(deltas.head), Iterator("zero", "none").asJava, false, hadoopConf)
      store.write(new Path(deltas(1)), Iterator("one").asJava, false, hadoopConf)

      assert(store.read(new Path(deltas.head), hadoopConf).toArray sameElements
        Array("zero", "none"))
      assert(store.read(new Path(deltas(1)), hadoopConf).toArray sameElements Array("one"))

      assertNoLeakedCrcFiles(dir)
    }
  }

  test("detects conflict") {
    withTempDir { dir =>
      val store = createLogStore(hadoopConf)

      val deltas = Seq(0, 1).map(i => new File(dir, i.toString)).map(_.getCanonicalPath)
      store.write(new Path(deltas.head), Iterator("zero").asJava, false, hadoopConf)
      store.write(new Path(deltas(1)), Iterator("one").asJava, false, hadoopConf)

      intercept[java.nio.file.FileAlreadyExistsException] {
        store.write(new Path(deltas(1)), Iterator("uno").asJava, false, hadoopConf)
      }
    }
  }

  test("listFrom") {
    withTempDir { tempDir =>
      val logStore = createLogStore(hadoopConf)
      val deltas =
        Seq(0, 1, 2, 3, 4).map(i => new File(tempDir, i.toString)).map(_.toURI).map(new Path(_))
      logStore.write(deltas(1), Iterator("zero").asJava, false, hadoopConf)
      logStore.write(deltas(2), Iterator("one").asJava, false, hadoopConf)
      logStore.write(deltas(3), Iterator("two").asJava, false, hadoopConf)

      assert(logStore.listFrom(deltas.head, hadoopConf).asScala.map(_.getPath.getName)
        .filterNot(_ == "_delta_log").toArray === Seq(1, 2, 3).map(_.toString))
      assert(logStore.listFrom(deltas(1), hadoopConf).asScala.map(_.getPath.getName)
        .filterNot(_ == "_delta_log").toArray === Seq(1, 2, 3).map(_.toString))
      assert(logStore.listFrom(deltas(2), hadoopConf).asScala.map(_.getPath.getName)
        .filterNot(_ == "_delta_log").toArray === Seq(2, 3).map(_.toString))
      assert(logStore.listFrom(deltas(3), hadoopConf).asScala.map(_.getPath.getName)
        .filterNot(_ == "_delta_log").toArray === Seq(3).map(_.toString))
      assert(logStore.listFrom(deltas(4), hadoopConf).asScala.map(_.getPath.getName)
        .filterNot(_ == "_delta_log").toArray === Nil)
    }
  }

  test("use isPartialWriteVisible to decide whether use rename") {
    withTempDir { tempDir =>
      val conf = hadoopConf
      conf.set("fs.file.impl", classOf[TrackingRenameFileSystem].getName)
      conf.set("fs.file.impl.disable.cache", "true")

      val log = DeltaLogImpl.forTable(conf, tempDir.getCanonicalPath)
      val addFile = AddFileJ.builder("/path", Map.empty[String, String].asJava, 100L,
        System.currentTimeMillis(), true).build()
      val metadata = MetadataJ.builder().build()

      log.startTransaction().commit((metadata :: addFile :: Nil).asJava,
        new Operation(Operation.Name.MANUAL_UPDATE), "engineInfo")

      TrackingRenameFileSystem.numOfRename = 0

      log.checkpoint()

      val expectedNumOfRename = if (shouldUseRenameToWriteCheckpoint) 1 else 0
      assert(TrackingRenameFileSystem.numOfRename === expectedNumOfRename)
    }
  }
}

/**
 * A file system allowing to track how many times `rename` is called.
 * `TrackingRenameFileSystem.numOfRename` should be reset to 0 before starting to trace.
 */
class TrackingRenameFileSystem extends RawLocalFileSystem {
  override def rename(src: Path, dst: Path): Boolean = {
    TrackingRenameFileSystem.numOfRename += 1
    super.rename(src, dst)
  }
}

object TrackingRenameFileSystem {
  @volatile var numOfRename = 0
}

class HDFSLogStoreSuite extends LogStoreSuiteBase {
  override def logStoreClassName: Option[String] = Some(classOf[HDFSLogStore].getName)
  override protected def shouldUseRenameToWriteCheckpoint: Boolean = true
}

class AzureLogStoreSuite extends LogStoreSuiteBase {
  override def logStoreClassName: Option[String] = Some(classOf[AzureLogStore].getName)
  override protected def shouldUseRenameToWriteCheckpoint: Boolean = true
}

class S3SingleDriverLogStoreSuite extends LogStoreSuiteBase {
  override def logStoreClassName: Option[String] = Some(classOf[S3SingleDriverLogStore].getName)
  override protected def shouldUseRenameToWriteCheckpoint: Boolean = false
}

class LocalLogStoreSuite extends LogStoreSuiteBase {
  override def logStoreClassName: Option[String] = Some(classOf[LocalLogStore].getName)
  override protected def shouldUseRenameToWriteCheckpoint: Boolean = true
}

/**
 * Test not providing a LogStore classname, in which case [[LogStoreProvider]] will use
 * the default value.
 */
class DefaultLogStoreSuite extends LogStoreSuiteBase {
  override def logStoreClassName: Option[String] = None
  override protected def shouldUseRenameToWriteCheckpoint: Boolean = true
}

/**
 * Test having the user provide their own LogStore.
 */
class UserDefinedLogStoreSuite extends LogStoreSuiteBase {
  override def logStoreClassName: Option[String] = Some(classOf[UserDefinedLogStore].getName)

  // In [[UserDefinedLogStore]], we purposefully set isPartialWriteVisible to false, so this
  // should be false as well
  override protected def shouldUseRenameToWriteCheckpoint: Boolean = false
}

/**
 * Sample user-defined log store implementing [[LogStore]].
 */
class UserDefinedLogStore(override val initHadoopConf: Configuration)
  extends LogStore(initHadoopConf) {

  private val mockImpl = new HDFSLogStore(initHadoopConf)

  override def read(path: Path, hadoopConf: Configuration): CloseableIteratorJ[String] = {
    val iter = mockImpl.read(path, hadoopConf)
    new CloseableIteratorJ[String] {
      override def close(): Unit = iter.close()
      override def hasNext: Boolean = iter.hasNext
      override def next(): String = iter.next
    }
  }

  override def write(
      path: Path,
      actions: java.util.Iterator[String],
      overwrite: java.lang.Boolean,
      hadoopConf: Configuration): Unit = {
    mockImpl.write(path, actions, overwrite, hadoopConf)
  }

  override def listFrom(path: Path, hadoopConf: Configuration): java.util.Iterator[FileStatus] = {
    mockImpl.listFrom(path, hadoopConf)
  }

  override def resolvePathOnPhysicalStorage(path: Path, hadoopConf: Configuration): Path = {
    mockImpl.resolvePathOnPhysicalStorage(path, hadoopConf)
  }

  override def isPartialWriteVisible(path: Path, hadoopConf: Configuration): java.lang.Boolean = {
    // mockImpl.isPartialWriteVisible is true, but let's add some test diversity for better branch
    // coverage and return false instead
    false
  }
}
