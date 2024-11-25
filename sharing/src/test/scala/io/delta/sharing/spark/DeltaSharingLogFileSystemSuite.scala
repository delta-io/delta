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
}
