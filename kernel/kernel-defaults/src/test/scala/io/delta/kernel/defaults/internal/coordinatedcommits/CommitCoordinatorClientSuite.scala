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

package io.delta.kernel.defaults.internal.coordinatedcommits

import io.delta.kernel.defaults.DeltaTableWriteSuiteBase
import io.delta.kernel.internal.actions.Metadata
import io.delta.kernel.internal.TableConfig
import io.delta.storage.commit.{Commit, CommitCoordinatorClient, CommitResponse, GetCommitsResponse, UpdatedActions}
import io.delta.storage.LogStore
import io.delta.storage.commit.actions.{AbstractMetadata, AbstractProtocol}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

import java.{lang, util}
import java.util.Collections
import scala.collection.convert.ImplicitConversions.`map AsScala`
import scala.collection.JavaConverters._

class CommitCoordinatorClientSuite extends DeltaTableWriteSuiteBase
  with CoordinatedCommitsTestUtils {

  protected trait TestCommitCoordinatorClientBase extends CommitCoordinatorClient {
    override def registerTable(
      logPath: Path,
      currentVersion: Long,
      currentMetadata: AbstractMetadata,
      currentProtocol: AbstractProtocol): util.Map[String, String] = {
      throw new UnsupportedOperationException("Not implemented")
    }

    override def commit(
      logStore: LogStore,
      hadoopConf: Configuration,
      logPath: Path,
      coordinatedCommitsTableConf: util.Map[String, String],
      commitVersion: Long,
      actions: util.Iterator[String],
      updatedActions: UpdatedActions): CommitResponse = {
      throw new UnsupportedOperationException("Not implemented")
    }

    override def getCommits(
      logPath: Path,
      coordinatedCommitsTableConf: util.Map[String, String],
      startVersion: lang.Long,
      endVersion: lang.Long = null): GetCommitsResponse =
      new GetCommitsResponse(Collections.emptyList(), -1)

    override def backfillToVersion(
      logStore: LogStore,
      hadoopConf: Configuration,
      logPath: Path,
      coordinatedCommitsTableConf: util.Map[String, String],
      version: Long,
      lastKnownBackfilledVersion: lang.Long): Unit = {}

    override def semanticEquals(other: CommitCoordinatorClient): lang.Boolean = this == other
  }

  private class TestCommitCoordinatorClient1 extends TestCommitCoordinatorClientBase
  private class TestCommitCoordinatorClient2 extends TestCommitCoordinatorClientBase

  test("registering multiple commit-coordinator builders with same name") {
    CommitCoordinatorProvider.clearBuilders()
    object Builder1 extends CommitCoordinatorBuilder {
      override def build(conf: util.Map[String, String]): CommitCoordinatorClient = null
      override def getName: String = "builder-1"
    }
    object BuilderWithSameName extends CommitCoordinatorBuilder {
      override def build(conf: util.Map[String, String]): CommitCoordinatorClient = null
      override def getName: String = "builder-1"
    }
    object Builder3 extends CommitCoordinatorBuilder {
      override def build(conf: util.Map[String, String]): CommitCoordinatorClient = null
      override def getName: String = "builder-3"
    }
    CommitCoordinatorProvider.registerBuilder(Builder1)
    intercept[Exception] {
      CommitCoordinatorProvider.registerBuilder(BuilderWithSameName)
    }
    CommitCoordinatorProvider.registerBuilder(Builder3)
  }

  test("getCommitCoordinator - builder returns same object") {
    CommitCoordinatorProvider.clearBuilders()
    object Builder1 extends CommitCoordinatorBuilder {
      val cs1 = new TestCommitCoordinatorClient1()
      val cs2 = new TestCommitCoordinatorClient2()
      override def build(conf: util.Map[String, String]): CommitCoordinatorClient = {
        conf.getOrElse("url", "") match {
          case "url1" => cs1
          case "url2" => cs2
          case _ => throw new IllegalArgumentException("Invalid url")
        }
      }
      override def getName: String = "cs-x"
    }
    CommitCoordinatorProvider.registerBuilder(Builder1)
    val cs1 =
      CommitCoordinatorProvider.getCommitCoordinatorClient("cs-x", Map("url" -> "url1").asJava)
    assert(cs1.isInstanceOf[TestCommitCoordinatorClient1])
    val cs1_again =
      CommitCoordinatorProvider.getCommitCoordinatorClient("cs-x", Map("url" -> "url1").asJava)
    assert(cs1 eq cs1_again)
    val cs2 = CommitCoordinatorProvider
      .getCommitCoordinatorClient("cs-x", Map("url" -> "url2", "a" -> "b").asJava)
    assert(cs2.isInstanceOf[TestCommitCoordinatorClient2])
    // If builder receives a config which doesn't have expected params, then it can throw exception.
    intercept[IllegalArgumentException] {
      CommitCoordinatorProvider.getCommitCoordinatorClient("cs-x", Map("url" -> "url3").asJava)
    }
  }

  test("getCommitCoordinatorClient - builder returns new object each time") {
    CommitCoordinatorProvider.clearBuilders()
    object Builder1 extends CommitCoordinatorBuilder {
      override def build(conf: util.Map[String, String]): CommitCoordinatorClient = {
        conf.getOrElse("url", "") match {
          case "url1" => new TestCommitCoordinatorClient1()
          case _ => throw new IllegalArgumentException("Invalid url")
        }
      }
      override def getName: String = "cs-name"
    }
    CommitCoordinatorProvider.registerBuilder(Builder1)
    val cs1 =
      CommitCoordinatorProvider.getCommitCoordinatorClient("cs-name", Map("url" -> "url1").asJava)
    assert(cs1.isInstanceOf[TestCommitCoordinatorClient1])
    val cs1_again =
      CommitCoordinatorProvider.getCommitCoordinatorClient("cs-name", Map("url" -> "url1").asJava)
    assert(cs1 ne cs1_again)
  }

  test("Coordinated Commit Related Properties from Metadata") {
    CommitCoordinatorProvider.clearBuilders()
    val m1 = Metadata.empty().withNewConfiguration(
      Map(
        TableConfig.COORDINATED_COMMITS_COORDINATOR_CONF.getKey ->
          """{"key1": "string_value", "key2Int": 2, "key3ComplexStr": "\"hello\""}""",
        TableConfig.COORDINATED_COMMITS_TABLE_CONF.getKey ->
          """{"key1": "string_value", "key2Int": 2, "key3ComplexStr": "\"hello\""}""").asJava
    )
    assert(TableConfig.COORDINATED_COMMITS_COORDINATOR_CONF.fromMetadata(m1) ===
      Map("key1" -> "string_value", "key2Int" -> "2", "key3ComplexStr" -> "\"hello\"").asJava)

    assert(TableConfig.COORDINATED_COMMITS_TABLE_CONF.fromMetadata(m1) ===
      Map("key1" -> "string_value", "key2Int" -> "2", "key3ComplexStr" -> "\"hello\"").asJava)

    val m2 = Metadata.empty().withNewConfiguration(
      Map(
        TableConfig.COORDINATED_COMMITS_COORDINATOR_CONF.getKey ->
          """{"key1": "string_value", "key2Int": "2""",
        TableConfig.COORDINATED_COMMITS_TABLE_CONF.getKey ->
          """{"key1": "string_value", "key2Int": "2""").asJava
    )
    intercept[RuntimeException] {
      TableConfig.COORDINATED_COMMITS_COORDINATOR_CONF.fromMetadata(m2)
    }
    intercept[RuntimeException] {
      TableConfig.COORDINATED_COMMITS_TABLE_CONF.fromMetadata(m2)
    }
  }

  test("Semantic Equality works as expected on CommitCoordinatorClients") {
    CommitCoordinatorProvider.clearBuilders()
    class TestCommitCoordinatorClient(val key: String) extends TestCommitCoordinatorClientBase {
      override def semanticEquals(other: CommitCoordinatorClient): lang.Boolean =
        other.isInstanceOf[TestCommitCoordinatorClient] &&
          other.asInstanceOf[TestCommitCoordinatorClient].key == key
    }
    object Builder extends CommitCoordinatorBuilder {
      override def build(conf: util.Map[String, String]): CommitCoordinatorClient = {
        new TestCommitCoordinatorClient(conf("key"))
      }
      override def getName: String = "cs-name"
    }
    CommitCoordinatorProvider.registerBuilder(Builder)

    // Different CommitCoordinator with same keys should be semantically equal.
    val obj1 =
      CommitCoordinatorProvider.getCommitCoordinatorClient("cs-name", Map("key" -> "url1").asJava)
    val obj2 =
      CommitCoordinatorProvider.getCommitCoordinatorClient("cs-name", Map("key" -> "url1").asJava)
    assert(obj1 != obj2)
    assert(obj1.semanticEquals(obj2))

    // Different CommitCoordinator with different keys should be semantically unequal.
    val obj3 =
      CommitCoordinatorProvider.getCommitCoordinatorClient("cs-name", Map("key" -> "url2").asJava)
    assert(obj1 != obj3)
    assert(!obj1.semanticEquals(obj3))
  }

  test("Semantic Equality works as expected on CommitCoordinatorClientHandler") {
    CommitCoordinatorProvider.clearBuilders()
    class TestCommitCoordinatorClient(val key: String) extends TestCommitCoordinatorClientBase {
      override def semanticEquals(other: CommitCoordinatorClient): lang.Boolean =
        other.isInstanceOf[TestCommitCoordinatorClient] &&
          other.asInstanceOf[TestCommitCoordinatorClient].key == key
    }
    object Builder extends CommitCoordinatorBuilder {
      override def build(conf: util.Map[String, String]): CommitCoordinatorClient = {
        new TestCommitCoordinatorClient(conf("key"))
      }
      override def getName: String = "cs-name"
    }
    CommitCoordinatorProvider.registerBuilder(Builder)

    withTempDirAndEngine { (tablePath, engine) =>
      // Different CommitCoordinatorHandler with same keys should be semantically equal.
      val obj1 = engine.getCommitCoordinatorClientHandler("cs-name", Map("key" -> "url1").asJava)
      val obj2 = engine.getCommitCoordinatorClientHandler("cs-name", Map("key" -> "url1").asJava)
      assert(obj1 != obj2)
      assert(obj1.semanticEquals(obj2))

      // Different CommitCoordinator with different keys should be semantically unequal.
      val obj3 = engine.getCommitCoordinatorClientHandler("cs-name", Map("key" -> "url2").asJava)
      assert(obj1 != obj3)
      assert(!obj1.semanticEquals(obj3))
    }
  }

  test("CommitCoordinatorClientHandler works as expected") {
    CommitCoordinatorProvider.clearBuilders()
    val fileStatus = new FileStatus()
    fileStatus.setPath(new Path("logPath"))
    class TestCommitCoordinatorClient extends TestCommitCoordinatorClientBase {
      override def registerTable(
        logPath: Path,
        currentVersion: Long,
        currentMetadata: AbstractMetadata,
        currentProtocol: AbstractProtocol): util.Map[String, String] = {
        Map("tableKey" -> "tableValue").asJava
      }

      override def getCommits(
        logPath: Path,
        coordinatedCommitsTableConf: util.Map[String, String],
        startVersion: lang.Long,
        endVersion: lang.Long = null): GetCommitsResponse = {
        new GetCommitsResponse(
          List(new Commit(-1, fileStatus, -1)).asJava, -1)
      }

      override def commit(
        logStore: LogStore,
        hadoopConf: Configuration,
        logPath: Path,
        coordinatedCommitsTableConf: util.Map[String, String],
        commitVersion: Long,
        actions: util.Iterator[String],
        updatedActions: UpdatedActions): CommitResponse = {
        new CommitResponse(new Commit(-1, fileStatus, -1))
      }

      override def backfillToVersion(
        logStore: LogStore,
        hadoopConf: Configuration,
        logPath: Path,
        coordinatedCommitsTableConf: util.Map[String, String],
        version: Long,
        lastKnownBackfilledVersion: lang.Long): Unit = {
        throw new UnsupportedOperationException(
          "BackfillToVersion not implemented in TestCommitCoordinatorClient for %s".format(logPath))
      }
    }
    object Builder extends CommitCoordinatorBuilder {
      lazy val coordinator = new TestCommitCoordinatorClient()
      override def build(conf: util.Map[String, String]): CommitCoordinatorClient = {
        coordinator
      }
      override def getName: String = "cs-name"
    }
    CommitCoordinatorProvider.registerBuilder(Builder)

    withTempDirAndEngine { (tablePath, engine) =>
      // Different CommitCoordinatorHandler with same keys should be semantically equal.
      val obj1 = engine.getCommitCoordinatorClientHandler("cs-name", Map("key" -> "url1").asJava)
      val obj2 = CommitCoordinatorProvider.getCommitCoordinatorClient(
        "cs-name", Map("key" -> "url1").asJava)

      assert(
        obj1.registerTable("logPath", 1, null, null) ===
          obj2.registerTable(new Path("logPath"), 1, null, null))

      assert(
        obj1.getCommits("logPath", Collections.emptyMap(), 1, 2).getLatestTableVersion ===
            obj2.getCommits(
              new Path("logPath"), Collections.emptyMap(), 1, 2).getLatestTableVersion)

      assert(
        obj1.commit("logPath", Collections.emptyMap(), 1, null, null).getCommit.getVersion ===
          obj2
            .commit(null, null, new Path("logPath"), Collections.emptyMap(), 1, null, null)
            .getCommit
            .getVersion)

      val ex = intercept[UnsupportedOperationException] {
        obj1.backfillToVersion("logPath", null, 1, null)
      }

      assert(
        ex.getMessage.contains(
          "BackfillToVersion not implemented in TestCommitCoordinatorClient for logPath"))
    }
  }
}
