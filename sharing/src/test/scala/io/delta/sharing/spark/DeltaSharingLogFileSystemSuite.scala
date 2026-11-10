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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.{SharedSparkContext, SparkEnv, SparkFunSuite}
import org.apache.spark.sql.delta.sharing.DeltaSharingTestSparkUtils
import org.apache.spark.storage.{BlockId, StorageLevel}

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


  test("delta log block IDs are flat, bounded, and scoped to one log") {
      Seq(
        "/path/to/profile.share#share.schema.table",
        "_delta_log_profile.share#share.schema.table")
        .foreach { tablePath =>
          val logPath = new Path(encode(tablePath), "_delta_log")
          val jsonPath = new Path(logPath, "00000000000000000000.json")
          val nextJsonPath = new Path(logPath, "00000000000000000001.json")
          val blockId = getDeltaSharingLogBlockId(jsonPath.toString)
          val listingId = getDeltaSharingLogBlockId(logPath.toString)

          assert(blockId == getDeltaSharingLogBlockId(jsonPath.toString))
          assert(blockId != getDeltaSharingLogBlockId(nextJsonPath.toString))
          assert(blockId.name.startsWith(DELTA_SHARING_LOG_BLOCK_ID_PREFIX))
          assert(listingId.name.endsWith("_listing"))
          assert(blockId.name.startsWith(listingId.name.stripSuffix("listing")))
          assert(!blockId.name.contains("/"))
          assert(blockId.name.length < 255)
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

  test("disk-backed delta log blocks with long paths are readable") {
      val tablePath = "/path/to/" + "a" * 220 + ".share#share.schema.table"
      val deltaLogPath = new Path(encode(tablePath), "_delta_log")
      val jsonPath = new Path(deltaLogPath, "00000000000000000000.json")
      val content = "{\"add\":{}}\n"
      val jsonBlockId = getDeltaSharingLogBlockId(jsonPath.toString)
      val listingBlockId = getDeltaSharingLogBlockId(deltaLogPath.toString)
      val statusBlockId = getDeltaSharingLogBlockId(jsonPath.toString + "_status")
      val status = DeltaSharingLogFileStatus(jsonPath.toString, content.length, 0)
      val blockManager = SparkEnv.get.blockManager

      try {
        assert(s"test_$jsonPath".length > 255)
        assert(jsonBlockId.name.length < 255)
        assert(blockManager.putIterator[String](
          blockId = jsonBlockId,
          values = Iterator(content),
          level = StorageLevel.DISK_ONLY,
          tellMaster = true
        ))
        assert(blockManager.putIterator[DeltaSharingLogFileStatus](
          blockId = listingBlockId,
          values = Iterator(status),
          level = StorageLevel.DISK_ONLY,
          tellMaster = true
        ))
        assert(blockManager.putSingle[DeltaSharingLogFileStatus](
          blockId = statusBlockId,
          value = status,
          level = StorageLevel.DISK_ONLY,
          tellMaster = true
        ))

        assert(scala.io.Source.fromInputStream(fs.open(jsonPath)).mkString == content)
        assert(fs.listStatus(deltaLogPath).map(_.getPath).toSeq == Seq(jsonPath))
        assert(fs.getFileStatus(jsonPath).getLen == content.length)
      } finally {
        Seq(jsonBlockId, listingBlockId, statusBlockId).foreach(blockManager.removeBlock(_))
      }
  }

  test("cleanup handles both block ID formats without touching another log") {
      val deltaLogPath = new Path(encode("cleanup-table"), "_delta_log")
      val otherLogPath = new Path(encode("cleanup-table__delta_log_more"), "_delta_log")
      val json0 = new Path(deltaLogPath, "00000000000000000000.json")
      val json1 = new Path(deltaLogPath, "00000000000000000001.json")
      val otherJson = new Path(otherLogPath, "00000000000000000000.json")
      val blockManager = SparkEnv.get.blockManager
      val json0Id = getDeltaSharingLogBlockId(json0.toString)
      val json1Id = getDeltaSharingLogBlockId(json1.toString)
      val otherJsonId = getDeltaSharingLogBlockId(otherJson.toString)
      val listingId = getDeltaSharingLogBlockId(deltaLogPath.toString)
      val lastCheckpointId = getDeltaSharingLogBlockId(
        new Path(deltaLogPath, "_last_checkpoint").toString)
      val legacyJson0Id = BlockId(s"test_$json0")
      val legacyJson1Id = BlockId(s"test_$json1")
      val legacyListingId = BlockId(s"test_$deltaLogPath")

      try {
        assert(!otherJsonId.name.startsWith(listingId.name.stripSuffix("listing")))
        Seq(json0Id, json1Id, otherJsonId).foreach { id =>
          assert(blockManager.putSingle(id, "{}\n", StorageLevel.DISK_ONLY))
        }
        Seq(legacyJson0Id, legacyJson1Id).foreach { id =>
          assert(blockManager.putSingle(id, "{}\n", StorageLevel.MEMORY_ONLY))
        }
        assert(blockManager.putSingle(legacyListingId, "listing", StorageLevel.MEMORY_ONLY))
        assert(blockManager.putIterator(
          listingId,
          Iterator(
            DeltaSharingLogFileStatus(json0.toString, 3, 0),
            DeltaSharingLogFileStatus(json1.toString, 3, 0)),
          StorageLevel.DISK_ONLY))

        tryToCleanUpPreviousBlocks(deltaLogPath.toString, maxVersion = 0)
        assert(blockManager.getStatus(json0Id).isEmpty)
        assert(blockManager.getStatus(legacyJson0Id).isEmpty)
        assert(blockManager.getStatus(json1Id).isDefined)
        assert(blockManager.getStatus(legacyJson1Id).isDefined)
        assert(blockManager.getStatus(otherJsonId).isDefined)
        assert(blockManager.getStatus(listingId).isDefined)
        assert(blockManager.getStatus(lastCheckpointId).isDefined)

        val listedPaths = fs.listStatus(deltaLogPath).map(_.getPath).toSet
        assert(listedPaths == Set(
          new Path(deltaLogPath, "00000000000000000000.checkpoint.parquet"),
          new Path(deltaLogPath, "_last_checkpoint"),
          json1))

        tryToCleanUpDeltaLog(deltaLogPath.toString)
        assert(blockManager.getStatus(json1Id).isEmpty)
        assert(blockManager.getStatus(legacyJson1Id).isEmpty)
        assert(blockManager.getStatus(legacyListingId).isEmpty)
        assert(blockManager.getStatus(listingId).isEmpty)
        assert(blockManager.getStatus(lastCheckpointId).isEmpty)
        assert(blockManager.getStatus(otherJsonId).isDefined)
      } finally {
        Seq(json0Id, json1Id, otherJsonId, listingId, lastCheckpointId,
          legacyJson0Id, legacyJson1Id, legacyListingId).foreach(blockManager.removeBlock(_))
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
