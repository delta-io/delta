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

import org.apache.spark.sql.delta.sources.DeltaSQLConf
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

  private val ucTable = "system.lakeflow_pipeline_events_preview.pipeline_events"
  private val ucTablePath = s"uc-deltasharing://$ucTable#$ucTable"

  private def readerLogPath(readerId: String, customTablePath: String = ucTablePath): Path = {
    val readerPath = DeltaSharingUtils.getTablePathWithIdSuffix(customTablePath, readerId)
    new Path(encode(readerPath), "_delta_log")
  }

  private def withFlatBlockIdsEnabled[T](enabled: Option[Boolean])(f: => T): T = {
    val sparkConf = SparkEnv.get.conf
    val key = DeltaSQLConf.DELTA_SHARING_FLAT_LOG_BLOCK_IDS_ENABLED.key
    val previous = sparkConf.getOption(key)
    enabled match {
      case Some(value) => sparkConf.set(key, value.toString)
      case None => sparkConf.remove(key)
    }
    try {
      f
    } finally {
      previous match {
        case Some(value) => sparkConf.set(key, value)
        case None => sparkConf.remove(key)
      }
    }
  }

  test("encode and decode") {
    assert(decode(encode(table1)) == table1)
  }

  gridTest("disabled log block IDs preserve legacy paths")(
    Seq(None, Some(false))
  ) { enabled =>
    withFlatBlockIdsEnabled(enabled) {
      val logPath = readerLogPath("20261117_005400_1234abcd")
      val paths = Seq(logPath, new Path(logPath, "00000000000000000000.json"),
        new Path(logPath, "00000000000000000000.json_status"),
        new Path(logPath, "00000000000000000000.checkpoint.parquet"),
        new Path(logPath, "_last_checkpoint"))
      paths.foreach { path =>
        val id = getDeltaSharingLogBlockId(path.toString)
        assert(id.name == s"test_$path")
      }
    }
  }

  Seq(Some(true)).foreach { enabled =>
    test(s"delta log block IDs are flat, bounded, and scoped to one log: enabled=$enabled") {
      withFlatBlockIdsEnabled(enabled) {
        val logPaths = Seq(ucTablePath, s"${"a" * 64}.$ucTablePath").map { tablePath =>
          readerLogPath("20261117_005400_1234abcd", tablePath)
        }
        assert(s"test_${logPaths.last}/00000000000000000000.json".length > 255)
        val suffixes = Seq("listing", "00000000000000000000.json", "00000000000000000001.json",
          "00000000000000000001.json_status", "00000000000000000000.checkpoint.parquet",
          "00000000000000000000.checkpoint.0000000001.0000000002.parquet", "_last_checkpoint")
        val ids = logPaths.flatMap { logPath =>
          val prefix = getDeltaSharingLogBlockId(logPath.toString).name.stripSuffix("listing")
          assert(prefix.matches(s"${DELTA_SHARING_LOG_BLOCK_ID_PREFIX}[0-9a-f]{64}_"))
          suffixes.map { suffix =>
            val path = if (suffix == "listing") logPath else new Path(logPath, suffix)
            val id = getDeltaSharingLogBlockId(path.toString)
            assert(id == getDeltaSharingLogBlockId(path.toString))
            assert(id.name == prefix + suffix)
            assert(!id.name.contains("/"))
            assert(id.name.getBytes("UTF-8").length < 255)
            id
          }
        }
        assert(ids.distinct.size == ids.size)
      }
    }
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

  Seq(false, true).foreach { enabled =>
    test(s"delta log cleanup selects only matching versions and log: enabled=$enabled") {
      withFlatBlockIdsEnabled(Some(enabled)) {
        val logPath = readerLogPath("20261117_005400_1234abcd")
        val jsonPaths = (0 to 1).map(version => new Path(logPath, f"$version%020d.json"))
        val ids = jsonPaths.map(path => getDeltaSharingLogBlockId(path.toString))
        val listingId = getDeltaSharingLogBlockId(logPath.toString)
        val lastCheckpointId = getDeltaSharingLogBlockId(
          new Path(logPath, "_last_checkpoint").toString)
        val otherLog = readerLogPath("20261117_005401_5678abcd")
        val otherId = getDeltaSharingLogBlockId(
          new Path(otherLog, "00000000000000000000.json").toString)
        val blockManager = SparkEnv.get.blockManager
        if (!enabled) assert(ids.head.name == s"test_${jsonPaths.head}")

        try {
          (ids :+ otherId).foreach { id =>
            assert(blockManager.putSingle(id, "{}\n", StorageLevel.MEMORY_ONLY))
          }
          assert(blockManager.putIterator(listingId,
            jsonPaths.map(path => DeltaSharingLogFileStatus(path.toString, 3, 0)).iterator,
            StorageLevel.MEMORY_ONLY))

          tryToCleanUpPreviousBlocks(logPath.toString, maxVersion = 0)
          assert(blockManager.getStatus(ids.head).isEmpty)
          assert(blockManager.getStatus(ids.last).isDefined)
          assert(blockManager.getStatus(otherId).isDefined)
          assert(blockManager.getStatus(listingId).isDefined)
          assert(blockManager.getStatus(lastCheckpointId).isDefined)

          tryToCleanUpDeltaLog(logPath.toString)
          Seq(ids.last, listingId, lastCheckpointId).foreach { id =>
            assert(blockManager.getStatus(id).isEmpty)
          }
          assert(blockManager.getStatus(otherId).isDefined)
        } finally {
          (ids ++ Seq(listingId, lastCheckpointId, otherId)).foreach(blockManager.removeBlock(_))
        }
      }
    }
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
}
