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

import org.apache.spark.sql.delta.storage.LogStore
import org.apache.hadoop.fs.{Path, RawLocalFileSystem}

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.Utils

class LogStoreSuite extends QueryTest with SharedSQLContext {

  override def beforeAll(): Unit = {
    super.beforeAll()
    switchToPrivilegedAclUser()
  }

  override def afterAll(): Unit = {
    switchToSuper()
    super.afterAll()
  }

  test("simple read / write") {
    val tempDir = Utils.createTempDir()
    val store = LogStore(spark.sparkContext)

    val deltas = Seq(0, 1).map(i => new File(tempDir, i.toString)).map(_.getCanonicalPath)
    store.write(deltas(0), Iterator("zero", "none"))
    store.write(deltas(1), Iterator("one"))

    assert(store.read(deltas(0)) == Seq("zero", "none"))
    assert(store.read(deltas(1)) == Seq("one"))
  }

  test("detects conflict") {
    val tempDir = Utils.createTempDir()
    val store = LogStore(spark.sparkContext)

    val deltas = Seq(0, 1).map(i => new File(tempDir, i.toString)).map(_.getCanonicalPath)
    store.write(deltas(0), Iterator("zero"))
    store.write(deltas(1), Iterator("one"))

    intercept[java.nio.file.FileAlreadyExistsException] {
      store.write(deltas(1), Iterator("uno"))
    }
  }

  test("listFrom") {
    val tempDir = Utils.createTempDir()
    val store = LogStore(spark.sparkContext)

    val deltas =
      Seq(0, 1, 2, 3, 4).map(i => new File(tempDir, i.toString)).map(_.toURI).map(new Path(_))
    store.write(deltas(1), Iterator("zero"))
    store.write(deltas(2), Iterator("one"))
    store.write(deltas(3), Iterator("two"))

    assert(
      store.listFrom(deltas(0)).map(_.getPath.getName).toArray === Seq(1, 2, 3).map(_.toString))
    assert(
      store.listFrom(deltas(1)).map(_.getPath.getName).toArray === Seq(1, 2, 3).map(_.toString))
    assert(store.listFrom(deltas(2)).map(_.getPath.getName).toArray === Seq(2, 3).map(_.toString))
    assert(store.listFrom(deltas(3)).map(_.getPath.getName).toArray === Seq(3).map(_.toString))
    assert(store.listFrom(deltas(4)).map(_.getPath.getName).toArray === Nil)
  }

  test("should pick up the session Hadoop configuration") {
    val tempDir = Utils.createTempDir()
    val path = new Path(new URI(s"fake://${tempDir.toURI.getRawPath}/1.json"))

    // Make sure it will fail without FakeFileSystem
    val e = intercept[IOException] {
      spark.sharedState.logStore.listFrom(path)
    }
    assert(e.getMessage.contains("No FileSystem for scheme: fake"))

    withSQLConf(s"fs.fake.impl" -> classOf[FakeFileSystem].getName) {
      spark.sharedState.logStore.listFrom(path)
    }
  }
}

/** A fake file system to test whether session Hadoop configuration will be picked up. */
class FakeFileSystem extends RawLocalFileSystem {

  override def getScheme: String = "fake"

  override def getUri: URI = {
    URI.create(s"fake:///")
  }
}
