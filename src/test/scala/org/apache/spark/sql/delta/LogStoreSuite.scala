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

import java.io.{File, IOException}
import java.net.URI

import org.apache.spark.sql.delta.storage._
import org.apache.hadoop.fs.{Path, RawLocalFileSystem}

import org.apache.spark.sql.{QueryTest, SparkSession}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.Utils

abstract class LogStoreSuiteBase extends QueryTest with SharedSQLContext {

  def createLogStore(spark: SparkSession): LogStore

  test("read / write") {
    val tempDir = Utils.createTempDir()
    val store = createLogStore(spark)

    val deltas = Seq(0, 1).map(i => new File(tempDir, i.toString)).map(_.getCanonicalPath)
    store.write(deltas(0), Iterator("zero", "none"))
    store.write(deltas(1), Iterator("one"))

    assert(store.read(deltas(0)) == Seq("zero", "none"))
    assert(store.read(deltas(1)) == Seq("one"))
  }

  test("detects conflict") {
    val tempDir = Utils.createTempDir()
    val store = createLogStore(spark)

    val deltas = Seq(0, 1).map(i => new File(tempDir, i.toString)).map(_.getCanonicalPath)
    store.write(deltas(0), Iterator("zero"))
    store.write(deltas(1), Iterator("one"))

    intercept[java.nio.file.FileAlreadyExistsException] {
      store.write(deltas(1), Iterator("uno"))
    }
  }

  test("iteratorFrom") {
    val tempDir = Utils.createTempDir()
    val store = createLogStore(spark)

    val deltas =
      Seq(0, 1, 2, 3, 4).map(i => new File(tempDir, i.toString)).map(_.toURI).map(new Path(_))
    store.write(deltas(1), Iterator("zero"))
    store.write(deltas(2), Iterator("one"))
    store.write(deltas(3), Iterator("two"))

    assert(
      store.iteratorFrom(deltas(0)).map(_.getPath.getName).toArray === Seq(1, 2, 3).map(_.toString))
    assert(
      store.iteratorFrom(deltas(1)).map(_.getPath.getName).toArray === Seq(1, 2, 3).map(_.toString))
    assert(
      store.iteratorFrom(deltas(2)).map(_.getPath.getName).toArray === Seq(2, 3).map(_.toString))
    assert(store.iteratorFrom(deltas(3)).map(_.getPath.getName).toArray === Seq(3).map(_.toString))
    assert(store.iteratorFrom(deltas(4)).map(_.getPath.getName).toArray === Nil)
  }

  protected def testHadoopConf(expectedErrMsg: String, fsImplConfs: (String, String)*): Unit = {
    test("should pick up fs impl conf from session Hadoop configuration") {
      withTempDir { tempDir =>
        val path = new Path(new URI(s"fake://${tempDir.toURI.getRawPath}/1.json"))

        // Make sure it will fail without FakeFileSystem
        val e = intercept[IOException] {
          createLogStore(spark).iteratorFrom(path)
        }
        assert(e.getMessage.contains(expectedErrMsg))

        withSQLConf(fsImplConfs: _*) {
          createLogStore(spark).iteratorFrom(path)
        }
      }
    }
  }
}

class AzureLogStoreSuite extends LogStoreSuiteBase {
  override def createLogStore(spark: SparkSession): LogStore = {
    new AzureLogStore(spark.sparkContext.getConf, spark.sessionState.newHadoopConf())
  }

  testHadoopConf(
    "No FileSystem for scheme: fake",
    "fs.fake.impl" -> classOf[FakeFileSystem].getName,
    "fs.fake.impl.disable.cache" -> "true")
}

class HDFSLogStoreImplSuite extends LogStoreSuiteBase {
  override def createLogStore(spark: SparkSession): LogStore = {
    new HDFSLogStoreImpl(spark.sparkContext.getConf, spark.sessionState.newHadoopConf())
  }

  testHadoopConf(
    "No AbstractFileSystem",
    "fs.AbstractFileSystem.fake.impl" -> classOf[FakeAbstractFileSystem].getName)
}

/** A fake file system to test whether session Hadoop configuration will be picked up. */
class FakeFileSystem extends RawLocalFileSystem {
  override def getScheme: String = FakeFileSystem.scheme
  override def getUri: URI = FakeFileSystem.uri
}

object FakeFileSystem {
  val scheme = "fake"
  val uri = URI.create(s"$scheme:///")
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
  override def getServerDefaults(): FsServerDefaults = LocalConfigKeys.getServerDefaults()
  override def isValidName(src: String): Boolean = true
}
