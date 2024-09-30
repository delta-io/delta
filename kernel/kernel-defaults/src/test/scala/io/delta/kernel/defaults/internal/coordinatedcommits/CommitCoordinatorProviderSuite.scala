/*
 * Copyright (2024) The Delta Lake Project Authors.
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

package io.delta.kernel.defaults.internal.coordinatedcommits

import CommitCoordinatorProvider._
import io.delta.kernel.defaults.DeltaTableWriteSuiteBase
import io.delta.kernel.defaults.utils.TestUtils
import io.delta.kernel.internal.actions.Metadata
import io.delta.kernel.internal.TableConfig
import io.delta.storage.commit.{Commit, CommitCoordinatorClient, CommitResponse, GetCommitsResponse, TableDescriptor, TableIdentifier, UpdatedActions}
import io.delta.storage.LogStore
import io.delta.storage.commit.actions.{AbstractMetadata, AbstractProtocol}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.scalatest.funsuite.AnyFunSuite

import java.{lang, util}
import java.util.{Collections, Optional}
import scala.collection.convert.ImplicitConversions.`map AsScala`
import scala.collection.JavaConverters._

class CommitCoordinatorProviderSuite extends AnyFunSuite with TestUtils {
  test("getCommitCoordinator - builder returns same object") {
    val hadoopConf = new Configuration()
    hadoopConf.set(
      getCommitCoordinatorNameConfKey("cc-x"),
      classOf[Builder1].getName)
    val cc1 = getCommitCoordinatorClient(hadoopConf, "cc-x", Map("url" -> "url1").asJava)
    assert(cc1.isInstanceOf[TestCommitCoordinatorClient1])
    val cc1_again = getCommitCoordinatorClient(hadoopConf, "cc-x", Map("url" -> "url1").asJava)
    assert(cc1 eq cc1_again)
    val cc2 =
      getCommitCoordinatorClient(hadoopConf, "cc-x", Map("url" -> "url2", "a" -> "b").asJava)
    assert(cc2.isInstanceOf[TestCommitCoordinatorClient2])
    // If builder receives a config which doesn't have expected params, then it can throw exception.
    intercept[IllegalArgumentException] {
      getCommitCoordinatorClient(hadoopConf, "cc-x", Map("url" -> "url3").asJava)
    }
  }

  test("getCommitCoordinatorClient - builder returns new object each time") {
    val hadoopConf = new Configuration()
    hadoopConf.set(getCommitCoordinatorNameConfKey("cc-name"), classOf[Builder2].getName)
    val cc1 = getCommitCoordinatorClient(hadoopConf, "cc-name", Map("url" -> "url1").asJava)
    assert(cc1.isInstanceOf[TestCommitCoordinatorClient1])
    val cc1_again = getCommitCoordinatorClient(hadoopConf, "cc-name", Map("url" -> "url1").asJava)
    assert(cc1 ne cc1_again)
  }

  test("Semantic Equality works as expected on CommitCoordinatorClientHandler") {

    withTempDirAndEngine( { (tablePath, engine) =>
      // Different CommitCoordinatorHandler with same keys should be semantically equal.
      val obj1 = engine.getCommitCoordinatorClientHandler("cc-name", Map("key" -> "url1").asJava)
      val obj2 = engine.getCommitCoordinatorClientHandler("cc-name", Map("key" -> "url1").asJava)
      assert(obj1 != obj2)
      assert(obj1.semanticEquals(obj2))

      // Different CommitCoordinatorHandler with different keys should be semantically unequal.
      val obj3 = engine.getCommitCoordinatorClientHandler("cc-name", Map("key" -> "url2").asJava)
      assert(obj1 != obj3)
      assert(!obj1.semanticEquals(obj3))
    }, Map(getCommitCoordinatorNameConfKey("cc-name") -> classOf[Builder3].getName))
  }

  test("CommitCoordinatorClientHandler works as expected") {
    withTempDirAndEngine( { (tablePath, engine) =>
      val hadoopConf = new Configuration()
      hadoopConf.set(getCommitCoordinatorNameConfKey("cc-name"), classOf[Builder4].getName)

      // Different CommitCoordinatorHandler with same keys should be semantically equal.
      val obj1 = engine.getCommitCoordinatorClientHandler("cc-name", Map("key" -> "url1").asJava)
      val obj2 = getCommitCoordinatorClient(hadoopConf, "cc-name", Map("key" -> "url1").asJava)

      assert(
        obj1.registerTable("logPath", 1, null, null) ===
          obj2.registerTable(new Path("logPath"), Optional.empty(), 1, null, null))

      val tableDesc =
        new TableDescriptor(new Path("logPath"), Optional.empty(), Collections.emptyMap())
      assert(
        obj1.getCommits("logPath", Collections.emptyMap(), 1, 2).getLatestTableVersion ===
          obj2.getCommits(tableDesc, 1, 2).getLatestTableVersion)

      assert(
        obj1.commit("logPath", Collections.emptyMap(), 1, null, null).getCommit.getVersion ===
          obj2
            .commit(null, null, tableDesc, 1, null, null)
            .getCommit
            .getVersion)

      val ex = intercept[UnsupportedOperationException] {
        obj1.backfillToVersion("logPath", null, 1, null)
      }

      assert(
        ex.getMessage.contains(
          "BackfillToVersion not implemented in TestCommitCoordinatorClient for logPath"))
    }, Map(getCommitCoordinatorNameConfKey("cc-name") -> classOf[Builder4].getName))
  }

  test("set CommitCoordinator config to a class that doesn't extend CommitCoordinator") {
    val hadoopConf = new Configuration()
    hadoopConf.set(getCommitCoordinatorNameConfKey("fake"), "java.lang.String")
    val e = intercept[IllegalArgumentException](
      getCommitCoordinatorClient(hadoopConf, "fake", Collections.emptyMap())
    )
    assert(e.getMessage.contains(
      "Can not instantiate `CommitCoordinatorBuilder` class (from config): %s"
        .format("java.lang.String")))
  }
}

protected trait TestCommitCoordinatorClientBase extends CommitCoordinatorClient {
  override def registerTable(
    logPath: Path,
    tableIdentifier: Optional[TableIdentifier],
    currentVersion: Long,
    currentMetadata: AbstractMetadata,
    currentProtocol: AbstractProtocol): util.Map[String, String] = {
    throw new UnsupportedOperationException("Not implemented")
  }

  override def commit(
    logStore: LogStore,
    hadoopConf: Configuration,
    tableDesc: TableDescriptor,
    commitVersion: Long,
    actions: util.Iterator[String],
    updatedActions: UpdatedActions): CommitResponse = {
    throw new UnsupportedOperationException("Not implemented")
  }

  override def getCommits(
    tableDesc: TableDescriptor,
    startVersion: lang.Long,
    endVersion: lang.Long = null): GetCommitsResponse =
    new GetCommitsResponse(Collections.emptyList(), -1)

  override def backfillToVersion(
    logStore: LogStore,
    hadoopConf: Configuration,
    tableDesc: TableDescriptor,
    version: Long,
    lastKnownBackfilledVersion: lang.Long): Unit = {}

  override def semanticEquals(other: CommitCoordinatorClient): Boolean = this == other
}

// Test 1
// Builder that  returns same object
private[coordinatedcommits] class TestCommitCoordinatorClient1
  extends TestCommitCoordinatorClientBase
private[coordinatedcommits] class TestCommitCoordinatorClient2
  extends TestCommitCoordinatorClientBase

object TestCommitCoordinatorClientInstances {
  val cc1 = new TestCommitCoordinatorClient1()
  val cc2 = new TestCommitCoordinatorClient2()
}

class Builder1(hadoopConf: Configuration) extends CommitCoordinatorBuilder(hadoopConf) {
  override def build(conf: util.Map[String, String]): CommitCoordinatorClient = {
    conf.getOrElse("url", "") match {
      case "url1" => TestCommitCoordinatorClientInstances.cc1
      case "url2" => TestCommitCoordinatorClientInstances.cc2
      case _ => throw new IllegalArgumentException("Invalid url")
    }
  }
  override def getName: String = "cc-x"
}

// Test 2
// Builder that returns new object each time
class Builder2(hadoopConf: Configuration) extends CommitCoordinatorBuilder(hadoopConf) {
  override def build(conf: util.Map[String, String]): CommitCoordinatorClient = {
    conf.getOrElse("url", "") match {
      case "url1" => new TestCommitCoordinatorClient1()
      case _ => throw new IllegalArgumentException("Invalid url")
    }
  }
  override def getName: String = "cc-name"
}

// Test 3
// Commit Coordinator Client with semanticEquals implemented for testing
class TestCommitCoordinatorClient3(val key: String) extends TestCommitCoordinatorClientBase {
  override def semanticEquals(other: CommitCoordinatorClient): Boolean =
    other.isInstanceOf[TestCommitCoordinatorClient3] &&
      other.asInstanceOf[TestCommitCoordinatorClient3].key == key
}

// Builder that builds TestCommitCoordinatorClient3
class Builder3(hadoopConf: Configuration) extends CommitCoordinatorBuilder(hadoopConf) {
  override def build(conf: util.Map[String, String]): CommitCoordinatorClient = {
    new TestCommitCoordinatorClient3(conf("key"))
  }
  override def getName: String = "cc-name"
}

// Test 4
// Commit Coordinator Client with all methods implemented for testing the usage of
// CommitCoordinatorClientHandler
class TestCommitCoordinatorClient4 extends TestCommitCoordinatorClientBase {
  val fileStatus = new FileStatus()
  fileStatus.setPath(new Path("logPath"))
  override def registerTable(
    logPath: Path,
    tableIdentifier: Optional[TableIdentifier],
    currentVersion: Long,
    currentMetadata: AbstractMetadata,
    currentProtocol: AbstractProtocol): util.Map[String, String] = {
    Map("tableKey" -> "tableValue").asJava
  }

  override def getCommits(
    tableDesc: TableDescriptor,
    startVersion: lang.Long,
    endVersion: lang.Long = null): GetCommitsResponse = {
    new GetCommitsResponse(
      List(new Commit(-1, fileStatus, -1)).asJava, -1)
  }

  override def commit(
    logStore: LogStore,
    hadoopConf: Configuration,
    tableDesc: TableDescriptor,
    commitVersion: Long,
    actions: util.Iterator[String],
    updatedActions: UpdatedActions): CommitResponse = {
    new CommitResponse(new Commit(-1, fileStatus, -1))
  }

  override def backfillToVersion(
    logStore: LogStore,
    hadoopConf: Configuration,
    tableDesc: TableDescriptor,
    version: Long,
    lastKnownBackfilledVersion: lang.Long): Unit = {
    throw new UnsupportedOperationException(
      "BackfillToVersion not implemented in TestCommitCoordinatorClient" +
        " for %s".format(tableDesc.getLogPath))
  }
}

// Builder that builds TestCommitCoordinatorClient4
class Builder4(hadoopConf: Configuration) extends CommitCoordinatorBuilder(hadoopConf) {
  lazy val coordinator = new TestCommitCoordinatorClient4()
  override def build(conf: util.Map[String, String]): CommitCoordinatorClient = {
    coordinator
  }
  override def getName: String = "cc-name"
}
