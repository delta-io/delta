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

package org.apache.spark.sql.delta.managedcommit

import org.apache.spark.sql.delta.{DeltaConfigs, DeltaLog, DeltaOperations, ManagedCommitTableFeature}
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.storage.LogStore
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaSQLTestUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

class CommitStoreSuite extends QueryTest with DeltaSQLTestUtils with SharedSparkSession
  with DeltaSQLCommandTest {

  trait TestCommitStoreBase extends CommitStore {
    override def commit(
        logStore: LogStore,
        hadoopConf: Configuration,
        logPath: Path,
        commitVersion: Long,
        actions: Iterator[String],
        updatedActions: UpdatedActions): CommitResponse = {
      throw new UnsupportedOperationException("Not implemented")
    }

    override def getCommits(
      logPath: Path,
      startVersion: Long,
      endVersion: Option[Long] = None): GetCommitsResponse = GetCommitsResponse(Seq.empty, -1)

    override def backfillToVersion(
        logStore: LogStore,
        hadoopConf: Configuration,
        logPath: Path,
        startVersion: Long,
        endVersion: Option[Long]): Unit = {}

    override def semanticEquals(other: CommitStore): Boolean = this == other
  }

  class TestCommitStore1 extends TestCommitStoreBase
  class TestCommitStore2 extends TestCommitStoreBase

  override def beforeEach(): Unit = {
    super.beforeEach()
    CommitStoreProvider.clearNonDefaultBuilders()
    CommitStoreProvider.registerBuilder(InMemoryCommitStoreBuilder(batchSize = 1))
  }

  test("registering multiple commit store builders with same name") {
    object Builder1 extends CommitStoreBuilder {
      override def build(conf: Map[String, String]): CommitStore = null
      override def name: String = "builder-1"
    }
    object BuilderWithSameName extends CommitStoreBuilder {
      override def build(conf: Map[String, String]): CommitStore = null
      override def name: String = "builder-1"
    }
    object Builder3 extends CommitStoreBuilder {
      override def build(conf: Map[String, String]): CommitStore = null
      override def name: String = "builder-3"
    }
    CommitStoreProvider.registerBuilder(Builder1)
    intercept[Exception] {
      CommitStoreProvider.registerBuilder(BuilderWithSameName)
    }
    CommitStoreProvider.registerBuilder(Builder3)
  }

  test("getCommitStore - builder returns same object") {
    object Builder1 extends CommitStoreBuilder {
      val cs1 = new TestCommitStore1()
      val cs2 = new TestCommitStore2()
      override def build(conf: Map[String, String]): CommitStore = {
        conf.getOrElse("url", "") match {
          case "url1" => cs1
          case "url2" => cs2
          case _ => throw new IllegalArgumentException("Invalid url")
        }
      }
      override def name: String = "cs-x"
    }
    CommitStoreProvider.registerBuilder(Builder1)
    val cs1 = CommitStoreProvider.getCommitStore("cs-x", Map("url" -> "url1"))
    assert(cs1.isInstanceOf[TestCommitStore1])
    val cs1_again = CommitStoreProvider.getCommitStore("cs-x", Map("url" -> "url1"))
    assert(cs1 eq cs1_again)
    val cs2 = CommitStoreProvider.getCommitStore("cs-x", Map("url" -> "url2", "a" -> "b"))
    assert(cs2.isInstanceOf[TestCommitStore2])
    // If builder receives a config which doesn't have expected params, then it can throw exception.
    intercept[IllegalArgumentException] {
      CommitStoreProvider.getCommitStore("cs-x", Map("url" -> "url3"))
    }
  }

  test("getCommitStore - builder returns new object each time") {
    object Builder1 extends CommitStoreBuilder {
      override def build(conf: Map[String, String]): CommitStore = {
        conf.getOrElse("url", "") match {
          case "url1" => new TestCommitStore1()
          case _ => throw new IllegalArgumentException("Invalid url")
        }
      }
      override def name: String = "cs-name"
    }
    CommitStoreProvider.registerBuilder(Builder1)
    val cs1 = CommitStoreProvider.getCommitStore("cs-name", Map("url" -> "url1"))
    assert(cs1.isInstanceOf[TestCommitStore1])
    val cs1_again = CommitStoreProvider.getCommitStore("cs-name", Map("url" -> "url1"))
    assert(cs1 ne cs1_again)
  }

  test("MANAGED_COMMIT_PROVIDER_CONF") {
    val m1 = Metadata(
      configuration = Map(
        DeltaConfigs.MANAGED_COMMIT_OWNER_CONF.key ->
          """{"key1": "string_value", "key2Int": 2, "key3ComplexStr": "\"hello\""}""")
    )
    assert(DeltaConfigs.MANAGED_COMMIT_OWNER_CONF.fromMetaData(m1) ===
      Map("key1" -> "string_value", "key2Int" -> "2", "key3ComplexStr" -> "\"hello\""))

    val m2 = Metadata(
      configuration = Map(
        DeltaConfigs.MANAGED_COMMIT_OWNER_CONF.key ->
          """{"key1": "string_value", "key2Int": "2""")
    )
    intercept[com.fasterxml.jackson.core.JsonParseException] {
      DeltaConfigs.MANAGED_COMMIT_OWNER_CONF.fromMetaData(m2)
    }
  }

  test("Commit fails if we try to put bad value for MANAGED_COMMIT_PROVIDER_CONF") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(10).write.format("delta").mode("append").save(path)
      val deltaLog = DeltaLog.forTable(spark, path)

      val metadataWithCorrectConf = Metadata(
        configuration = Map(
          DeltaConfigs.MANAGED_COMMIT_OWNER_CONF.key ->
            """{"key1": "string_value", "key2Int": 2, "key3ComplexStr": "\"hello\""}""")
      )
      val metadataWithIncorrectConf = Metadata(
        configuration = Map(
          DeltaConfigs.MANAGED_COMMIT_OWNER_CONF.key ->
            """{"key1": "string_value", "key2Int": "2""")
      )

      intercept[com.fasterxml.jackson.core.JsonParseException] {
        deltaLog.startTransaction().commit(
          Seq(metadataWithIncorrectConf), DeltaOperations.ManualUpdate)
      }
      DeltaConfigs.MANAGED_COMMIT_OWNER_CONF.fromMetaData(metadataWithCorrectConf)
    }
  }

  test("Adding MANAGED_COMMIT_PROVIDER_NAME table property automatically upgrades the Protocol") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(10).write.format("delta").mode("append").save(path)
      val metadata =
        Metadata(configuration = Map(DeltaConfigs.MANAGED_COMMIT_OWNER_NAME.key -> "in-memory"))
      val deltaLog = DeltaLog.forTable(spark, path)

      def getWriterFeatures(log: DeltaLog): Set[String] = {
        log.update().protocol.writerFeatures.getOrElse(Set.empty)
      }

      assert(!getWriterFeatures(deltaLog).contains(ManagedCommitTableFeature.name))
      deltaLog.startTransaction().commit(Seq(metadata), DeltaOperations.ManualUpdate)
      assert(getWriterFeatures(deltaLog).contains(ManagedCommitTableFeature.name))
    }
  }

  test("Semantic Equality works as expected on CommitStores") {
    class TestCommitStore(val key: String) extends TestCommitStoreBase {
      override def semanticEquals(other: CommitStore): Boolean =
        other.isInstanceOf[TestCommitStore] && other.asInstanceOf[TestCommitStore].key == key
    }
    object Builder1 extends CommitStoreBuilder {
      override def build(conf: Map[String, String]): CommitStore = {
        new TestCommitStore(conf("key"))
      }
      override def name: String = "cs-name"
    }
    CommitStoreProvider.registerBuilder(Builder1)

    // Different CommitStores with same keys should be semantically equal.
    val obj1 = CommitStoreProvider.getCommitStore("cs-name", Map("key" -> "url1"))
    val obj2 = CommitStoreProvider.getCommitStore("cs-name", Map("key" -> "url1"))
    assert(obj1 != obj2)
    assert(obj1.semanticEquals(obj2))

    // Different CommitStores with different keys should be semantically unequal.
    val obj3 = CommitStoreProvider.getCommitStore("cs-name", Map("key" -> "url2"))
    assert(obj1 != obj3)
    assert(!obj1.semanticEquals(obj3))
  }
}
