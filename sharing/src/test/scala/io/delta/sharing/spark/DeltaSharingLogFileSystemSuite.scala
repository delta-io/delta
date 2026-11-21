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

package io.delta.sharing.spark

import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.{Callable, CountDownLatch, ExecutorService, TimeoutException, TimeUnit}

import java.util.concurrent.Executors

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.{SharedSparkContext, SparkEnv, SparkFunSuite}
import org.apache.spark.sql.delta.sharing.DeltaSharingTestSparkUtils
import org.apache.spark.storage.StorageLevel

class DeltaSharingLogFileSystemSuite extends SparkFunSuite with SharedSparkContext {
  import DeltaSharingLogFileSystem._

  var hadoopConf: Configuration = new Configuration

  var path: Path = null
  var fs: FileSystem = null
  override def beforeAll(): Unit = {
    super.beforeAll()
    conf.set(
      s"spark.hadoop.fs.${DeltaSharingLogFileSystem.SCHEME}.impl",
      classOf[DeltaSharingLogFileSystem].getName
    )
    hadoopConf = DeltaSharingTestSparkUtils.getHadoopConf(conf)

    path = encode(table1)
    fs = path.getFileSystem(hadoopConf)
  }

  // constants for testing.
  private val table1 = "table1"
  private val table2 = "table2"

  test("encode and decode") {
    assert(decode(encode(table1)) == table1)
  }

  test("file system should be cached") {
    assert(fs.isInstanceOf[DeltaSharingLogFileSystem])
    assert(fs eq path.getFileSystem(hadoopConf))

    assert(fs.getScheme == "delta-sharing-log")
    assert(fs.getWorkingDirectory == new Path("delta-sharing-log:/"))
  }

  test("unsupported functions") {
    intercept[UnsupportedOperationException] { fs.create(path) }
    intercept[UnsupportedOperationException] { fs.append(path) }
    intercept[UnsupportedOperationException] { fs.rename(path, new Path(path, "a")) }
    intercept[UnsupportedOperationException] { fs.delete(path, true) }
    intercept[UnsupportedOperationException] { fs.listStatusIterator(path) }
    intercept[UnsupportedOperationException] { fs.setWorkingDirectory(path) }
    intercept[UnsupportedOperationException] { fs.mkdirs(path) }
  }

  test("open works ok") {
    val content = "this is the content\nanother line\nthird line"
    SparkEnv.get.blockManager.putSingle[String](
      blockId = getDeltaSharingLogBlockId(path.toString),
      value = content,
      level = StorageLevel.MEMORY_AND_DISK_SER,
      tellMaster = true
    )
    assert(scala.io.Source.fromInputStream(fs.open(path)).mkString == content)
  }

  test("exists works ok") {
    val newPath = encode(table1)
    val fileAndSizeSeq = Seq[DeltaSharingLogFileStatus](
      DeltaSharingLogFileStatus("filea", 10, 100)
    )
    SparkEnv.get.blockManager.putIterator[DeltaSharingLogFileStatus](
      blockId = getDeltaSharingLogBlockId(newPath.toString),
      values = fileAndSizeSeq.toIterator,
      level = StorageLevel.MEMORY_AND_DISK_SER,
      tellMaster = true
    )

    assert(fs.exists(newPath))
    assert(!fs.exists(new Path(newPath, "A")))
  }

  test("listStatus works ok") {
    val newPath = encode(table2)
    val fileAndSizeSeq = Seq[DeltaSharingLogFileStatus](
      DeltaSharingLogFileStatus("file_a", 10, 100),
      DeltaSharingLogFileStatus("file_b", 20, 200)
    )
    SparkEnv.get.blockManager.putIterator[DeltaSharingLogFileStatus](
      blockId = getDeltaSharingLogBlockId(newPath.toString),
      values = fileAndSizeSeq.toIterator,
      level = StorageLevel.MEMORY_AND_DISK_SER,
      tellMaster = true
    )

    val files = fs.listStatus(newPath)
    assert(files.length == 2)
    assert(files(0).getPath == new Path("file_a"))
    assert(files(0).getLen == 10)
    assert(files(0).getModificationTime == 100)
    assert(files(1).getPath == new Path("file_b"))
    assert(files(1).getLen == 20)
    assert(files(1).getModificationTime == 200)

    intercept[java.io.FileNotFoundException] {
      fs.listStatus(new Path(newPath, "random"))
    }
  }

  private def snapshotLines(includeFile: Boolean): Seq[String] = {
    val metadata = Seq(TestUtils.protocolStr, TestUtils.metaDataStr)
    if (includeFile) metadata :+ TestUtils.getAddFileStr1(TestUtils.paths.head) else metadata
  }

  private def snapshotExecutor(threads: Int): ExecutorService = {
    Executors.newFixedThreadPool(threads)
  }

  private def snapshotDirectory(table: String): Path = new Path(encode(table), "_delta_log")

  private def readSnapshot(table: String): String = {
    val files = fs.listStatus(snapshotDirectory(table))
    assert(files.length == 1)
    val source = scala.io.Source.fromInputStream(fs.open(files.head.getPath), "UTF-8")
    try {
      val contents = source.mkString
      assert(files.head.getLen == contents.getBytes(UTF_8).length)
      contents
    } finally {
      source.close()
    }
  }

  test("immutable snapshots keep the first JSON and directory contents") {
    val table = "immutable-snapshot"
    constructLocalDeltaLogAtVersionZero(snapshotLines(includeFile = false), table,
      overwriteBlocks = false)
    val first = readSnapshot(table)

    constructLocalDeltaLogAtVersionZero(snapshotLines(includeFile = true), table,
      overwriteBlocks = false)
    assert(readSnapshot(table) == first)
  }

  test("a missing listing uses the existing immutable JSON size") {
    val table = "immutable-snapshot-missing-listing"
    constructLocalDeltaLogAtVersionZero(snapshotLines(includeFile = false), table,
      overwriteBlocks = false)
    val first = readSnapshot(table)
    SparkEnv.get.blockManager.removeBlock(
      getDeltaSharingLogBlockId(snapshotDirectory(table).toString), tellMaster = true)

    constructLocalDeltaLogAtVersionZero(snapshotLines(includeFile = true), table,
      overwriteBlocks = false)
    assert(readSnapshot(table) == first)
  }

  test("immutable snapshot construction completes while a reader holds the JSON block") {
    val table = "immutable-snapshot-active-reader"
    constructLocalDeltaLogAtVersionZero(snapshotLines(includeFile = true), table,
      overwriteBlocks = false)
    val jsonPath = fs.listStatus(snapshotDirectory(table)).head.getPath
    val reader = SparkEnv.get.blockManager
      .get[String](getDeltaSharingLogBlockId(jsonPath.toString)).get.data
    val executor = snapshotExecutor(1)
    try {
      val writer = executor.submit(new Callable[Unit] {
        override def call(): Unit = {
          constructLocalDeltaLogAtVersionZero(snapshotLines(includeFile = true), table,
            overwriteBlocks = false)
        }
      })
      writer.get(10, TimeUnit.SECONDS)
    } finally {
      while (reader.hasNext) reader.next()
      executor.shutdownNow()
      assert(executor.awaitTermination(10, TimeUnit.SECONDS))
    }
  }

  test("immutable snapshot publication waits for an in-flight JSON writer") {
    val table = "immutable-snapshot-active-writer"
    val directory = snapshotDirectory(table)
    val jsonPath = new Path(directory, "00000000000000000000.json")
    val entered = new CountDownLatch(1)
    val secondEntered = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val executor = snapshotExecutor(2)
    val contents = "stored JSON\n"
    try {
      val firstWriter = executor.submit(new Callable[Boolean] {
        override def call(): Boolean = {
          val values = Iterator(contents).map { line =>
            entered.countDown()
            assert(release.await(10, TimeUnit.SECONDS))
            line
          }
          SparkEnv.get.blockManager.putIterator(
            getDeltaSharingLogBlockId(jsonPath.toString), values,
            StorageLevel.MEMORY_AND_DISK_SER, tellMaster = true)
        }
      })
      assert(entered.await(10, TimeUnit.SECONDS))
      val secondWriter = executor.submit(new Callable[Unit] {
        override def call(): Unit = {
          secondEntered.countDown()
          constructLocalDeltaLogAtVersionZero(snapshotLines(includeFile = true), table,
            overwriteBlocks = false)
        }
      })
      // The JSON writer owns the write lock, so a directory entry cannot yet be published.
      assert(secondEntered.await(10, TimeUnit.SECONDS))
      intercept[TimeoutException] {
        secondWriter.get(200, TimeUnit.MILLISECONDS)
      }
      assert(!fs.exists(directory))
      release.countDown()
      assert(firstWriter.get(10, TimeUnit.SECONDS))
      secondWriter.get(10, TimeUnit.SECONDS)
      assert(readSnapshot(table) == contents)
    } finally {
      release.countDown()
      executor.shutdownNow()
      assert(executor.awaitTermination(10, TimeUnit.SECONDS))
    }
  }

  test("snapshot construction retains replacement behavior by default") {
    val table = "mutable-snapshot"
    constructLocalDeltaLogAtVersionZero(snapshotLines(includeFile = false), table)
    val first = readSnapshot(table)
    constructLocalDeltaLogAtVersionZero(snapshotLines(includeFile = true), table)
    val second = readSnapshot(table)
    assert(second != first)
    assert(second.contains("\"add\""))
  }
}
