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

package io.delta.storage

import java.io.File
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.delta.FakeFileSystem
import org.apache.spark.sql.delta.util.FileNames

/////////////////////
// Base Test Suite //
/////////////////////

class ExternalLogStoreSuite extends org.apache.spark.sql.delta.PublicLogStoreSuite {
  override protected val publicLogStoreClassName: String =
    classOf[MemoryLogStore].getName

  testHadoopConf(
    expectedErrMsg = "No FileSystem for scheme \"fake\"",
    "fs.fake.impl" -> classOf[FakeFileSystem].getName,
    "fs.fake.impl.disable.cache" -> "true"
  )

  def getDeltaVersionPath(logDir: File, version: Int): Path = {
    FileNames.unsafeDeltaFile(new Path(logDir.toURI), version)
  }

  def getFailingDeltaVersionPath(logDir: File, version: Int): Path = {
    FileNames.unsafeDeltaFile(new Path(s"failing:${logDir.getCanonicalPath}"), version)
  }

  test("single write") {
    withTempLogDir { tempLogDir =>
      val store = createLogStore(spark)
      val path = getDeltaVersionPath(tempLogDir, 0)
      store.write(path, Iterator("foo", "bar"), overwrite = false, sessionHadoopConf)
      val entry = MemoryLogStore.get(path);
      assert(entry != null)
      assert(entry.complete);
    }
  }

  test("double write") {
    withTempLogDir { tempLogDir =>
      val store = createLogStore(spark)
      val path = getDeltaVersionPath(tempLogDir, 0)
      store.write(path, Iterator("foo", "bar"), overwrite = false, sessionHadoopConf)
      assert(MemoryLogStore.containsKey(path))
      assertThrows[java.nio.file.FileSystemException] {
        store.write(path, Iterator("foo", "bar"), overwrite = false, sessionHadoopConf)
      }
    }
  }

  test("overwrite") {
    withTempLogDir { tempLogDir =>
      val store = createLogStore(spark)
      val path = getDeltaVersionPath(tempLogDir, 0)
      store.write(path, Iterator("foo", "bar"), overwrite = false, sessionHadoopConf)
      assert(MemoryLogStore.containsKey(path))
      store.write(path, Iterator("foo", "bar"), overwrite = true, sessionHadoopConf)
      assert(MemoryLogStore.containsKey(path))
    }
  }

  test("write N fails if overwrite=false and N already exists in FileSystem " +
    "and N does not exist in external store") {
    withTempLogDir { tempLogDir =>
      val delta0 = getDeltaVersionPath(tempLogDir, 0)
      val delta1_a = getDeltaVersionPath(tempLogDir, 1)
      val delta1_b = getDeltaVersionPath(tempLogDir, 1)

      val store = createLogStore(spark)
      store.write(delta0, Iterator("zero"), overwrite = false, sessionHadoopConf)
      store.write(delta1_a, Iterator("one_a"), overwrite = false, sessionHadoopConf)

      // Pretend that BaseExternalLogStore.getExpirationDelaySeconds() seconds have
      // transpired and that the external store has run TTL cleanup.
      MemoryLogStore.hashMap.clear();

      val e = intercept[java.nio.file.FileAlreadyExistsException] {
        store.write(delta1_b, Iterator("one_b"), overwrite = false, sessionHadoopConf)
      }

      assert(e.getMessage.contains(delta1_b.toString))
    }
  }

  test("write N fails and does not write to external store if overwrite=false and N " +
    "already exists in FileSystem and N already exists in external store") {
    withTempLogDir { tempLogDir =>
      val delta0 = getDeltaVersionPath(tempLogDir, 0)
      val delta1_a = getDeltaVersionPath(tempLogDir, 1)
      val delta1_b = getDeltaVersionPath(tempLogDir, 1)

      val store = createLogStore(spark)
      store.write(delta0, Iterator("zero"), overwrite = false, sessionHadoopConf)
      store.write(delta1_a, Iterator("one_a"), overwrite = false, sessionHadoopConf)

      assert(MemoryLogStore.hashMap.size() == 2)

      val e = intercept[java.nio.file.FileAlreadyExistsException] {
        store.write(delta1_b, Iterator("one_b"), overwrite = false, sessionHadoopConf)
      }

      assert(e.getMessage.contains(delta1_b.toString))
      assert(MemoryLogStore.hashMap.size() == 2)
    }
  }

  test("write N+1 fails if N doesn't exist in external store or FileSystem") {
    withTempLogDir { tempLogDir =>
      val store = createLogStore(spark)

      val delta0 = getDeltaVersionPath(tempLogDir, 0)
      val delta1 = getDeltaVersionPath(tempLogDir, 1)
      val e = intercept[java.nio.file.FileSystemException] {
        store.write(delta1, Iterator("one"), overwrite = false, sessionHadoopConf)
      }
      assert(e.getMessage == s"previous commit $delta0 doesn't exist on the file system but does in the external log store")
    }
  }

  // scalastyle:off line.size.limit
  test("write N+1 fails if N is marked as complete in external store but doesn't exist in FileSystem") {
    // scalastyle:on line.size.limit
    withTempLogDir { tempLogDir =>
      val store = createLogStore(spark)

      val delta0 = getDeltaVersionPath(tempLogDir, 0)
      val delta1 = getDeltaVersionPath(tempLogDir, 1)

      store.write(delta0, Iterator("one"), overwrite = false, sessionHadoopConf)
      delta0.getFileSystem(sessionHadoopConf).delete(delta0, true)
      val e = intercept[java.nio.file.FileSystemException] {
        store.write(delta1, Iterator("one"), overwrite = false, sessionHadoopConf)
      }
      assert(e.getMessage == s"previous commit $delta0 doesn't exist on the file system but does in the external log store")
    }
  }

  test("write N+1 succeeds and recovers version N if N is incomplete in external store") {
    withSQLConf(
      "fs.failing.impl" -> classOf[FailingFileSystem].getName,
      "fs.failing.impl.disable.cache" -> "true"
    ) {
      withTempLogDir { tempLogDir =>
        val store = createLogStore(spark)

        val delta0_normal = getDeltaVersionPath(tempLogDir, 0)
        val delta0_fail = getFailingDeltaVersionPath(tempLogDir, 0)
        val delta1 = getDeltaVersionPath(tempLogDir, 1)

        // Create N (incomplete) in external store, with no N in FileSystem
        FailingFileSystem.failOnSuffix = Some(delta0_fail.getName)
        store.write(delta0_fail, Iterator("zero"), overwrite = false, sessionHadoopConf)
        assert(!delta0_fail.getFileSystem(sessionHadoopConf).exists(delta0_fail))
        assert(!MemoryLogStore.get(delta0_fail).complete)

        // Write N + 1 and check that recovery was performed
        store.write(delta1, Iterator("one"), overwrite = false, sessionHadoopConf)
        assert(delta0_fail.getFileSystem(sessionHadoopConf).exists(delta0_fail))
        assert(MemoryLogStore.get(delta0_fail).complete)
        assert(MemoryLogStore.get(delta1).complete)
      }
    }
  }

  test("listFrom performs recovery") {
    withSQLConf(
      "fs.failing.impl" -> classOf[FailingFileSystem].getName,
      "fs.failing.impl.disable.cache" -> "true"
    ) {
      withTempLogDir { tempLogDir =>
        val store = createLogStore(spark)
        val delta0_normal = getDeltaVersionPath(tempLogDir, 0)
        val delta0_fail = getFailingDeltaVersionPath(tempLogDir, 0)

        // fail to write to FileSystem when we try to commit 0000.json
        FailingFileSystem.failOnSuffix = Some(delta0_fail.getName)

        // try and commit 0000.json
        store.write(delta0_fail, Iterator("foo", "bar"), overwrite = false, sessionHadoopConf)

        // check that entry was written to external store and that it doesn't exist in FileSystem
        val entry = MemoryLogStore.get(delta0_fail)
        assert(!entry.complete)
        assert(!delta0_fail.getFileSystem(sessionHadoopConf).exists(delta0_fail))

        // Now perform a `listFrom` read, which should fix the transaction log
        val contents = store.read(entry.absoluteTempPath(), sessionHadoopConf).toList
        FailingFileSystem.failOnSuffix = None
        store.listFrom(delta0_normal, sessionHadoopConf)

        val entry2 = MemoryLogStore.get(delta0_normal)
        assert(entry2.complete)
        assert(store.read(entry2.absoluteFilePath(), sessionHadoopConf).toList == contents)
      }
    }
  }

  test("write to new Delta table but a DynamoDB entry for it already exists") {
    withTempLogDir { tempLogDir =>
      val store = createLogStore(spark)

      // write 0000.json
      val path = getDeltaVersionPath(tempLogDir, 0)
      store.write(path, Iterator("foo"), overwrite = false, sessionHadoopConf)

      // delete 0000.json from FileSystem
      val fs = path.getFileSystem(sessionHadoopConf)
      fs.delete(path, false)

      // try and write a new 0000.json, while the external store entry still exists
      val e = intercept[java.nio.file.FileSystemException] {
        store.write(path, Iterator("bar"), overwrite = false, sessionHadoopConf)
      }.getMessage

      val tablePath = path.getParent.getParent
      assert(e == s"Old entries for table $tablePath still exist in the external log store")
    }
  }

  test("listFrom exceptions") {
    val store = createLogStore(spark)
    assertThrows[java.io.FileNotFoundException] {
      store.listFrom("/non-existing-path/with-parent")
    }
  }

  test("MemoryLogStore ignores failing scheme") {
    withSQLConf(
      "fs.failing.impl" -> classOf[FailingFileSystem].getName,
      "fs.failing.impl.disable.cache" -> "true"
    ) {
      withTempLogDir { tempLogDir =>
        val store = createLogStore(spark)
        val delta0_normal = getDeltaVersionPath(tempLogDir, 0)
        val delta0_fail = getFailingDeltaVersionPath(tempLogDir, 0)

        store.write(delta0_fail, Iterator("zero"), overwrite = false, sessionHadoopConf)
        assert(MemoryLogStore.get(delta0_fail) eq MemoryLogStore.get(delta0_normal))
      }
    }
  }

  protected def shouldUseRenameToWriteCheckpoint: Boolean = false
}

///////////////////////////////////
// S3DynamoDBLogStore Test Suite //
///////////////////////////////////

class S3DynamoDBLogStoreSuite extends AnyFunSuite {
  test("getParam") {
    import S3DynamoDBLogStore._

    val sparkPrefixKey = "spark.io.delta.storage.S3DynamoDBLogStore.ddb.tableName"
    val basePrefixKey = "io.delta.storage.S3DynamoDBLogStore.ddb.tableName"

    // Sanity check
    require(sparkPrefixKey == SPARK_CONF_PREFIX + "." + DDB_CLIENT_TABLE)
    require(basePrefixKey == BASE_CONF_PREFIX + "." + DDB_CLIENT_TABLE)

    // Case 1: no parameters exist, should use default
    assert(getParam(new Configuration(), DDB_CLIENT_TABLE, "default_table") == "default_table")

    // Case 2: spark-prefix param only
    {
      val hadoopConf = new Configuration()
      hadoopConf.set(sparkPrefixKey, "some_other_table_2")
      assert(getParam(hadoopConf, DDB_CLIENT_TABLE, "default_table") == "some_other_table_2")
    }

    // Case 3: base-prefix param only
    {
      val hadoopConf = new Configuration()
      hadoopConf.set(basePrefixKey, "some_other_table_3")
      assert(getParam(hadoopConf, DDB_CLIENT_TABLE, "default_table") == "some_other_table_3")
    }

    // Case 4: both params set, same value
    {
      val hadoopConf = new Configuration()
      hadoopConf.set(sparkPrefixKey, "some_other_table_4")
      hadoopConf.set(basePrefixKey, "some_other_table_4")
      assert(getParam(hadoopConf, DDB_CLIENT_TABLE, "default_table") == "some_other_table_4")
    }

    // Case 5: both param set, different value
    {
      val hadoopConf = new Configuration()
      hadoopConf.set(sparkPrefixKey, "some_other_table_5a")
      hadoopConf.set(basePrefixKey, "some_other_table_5b")
      val e = intercept[IllegalArgumentException] {
        getParam(hadoopConf, DDB_CLIENT_TABLE, "default_table")
      }.getMessage
      assert(e == (s"Configuration properties `$sparkPrefixKey=some_other_table_5a` and " +
        s"`$basePrefixKey=some_other_table_5b` have different values. Please set only one."))
    }
  }
}

////////////////////////////////
// File System Helper Classes //
////////////////////////////////

/**
 * This utility enables failure simulation on file system.
 * Providing a matching suffix results in an exception being
 * thrown that allows to test file system failure scenarios.
 */
class FailingFileSystem extends RawLocalFileSystem {
  override def getScheme: String = FailingFileSystem.scheme

  override def getUri: URI = FailingFileSystem.uri

  override def create(path: Path, overwrite: Boolean): FSDataOutputStream = {

    FailingFileSystem.failOnSuffix match {
      case Some(suffix) =>
        if (path.toString.endsWith(suffix)) {
          throw new java.nio.file.FileSystemException("fail")
        }
      case None => ;
    }
    super.create(path, overwrite)
  }
}

object FailingFileSystem {
  private val scheme = "failing"
  private val uri: URI = URI.create(s"$scheme:///")

  var failOnSuffix: Option[String] = None
}
