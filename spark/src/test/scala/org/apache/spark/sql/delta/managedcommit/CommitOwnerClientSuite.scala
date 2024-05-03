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

import scala.reflect.runtime.universe._

import org.apache.spark.sql.delta.{DeltaConfigs, DeltaLog, DeltaOperations, ManagedCommitTableFeature}
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.storage.LogStore
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaSQLTestUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

class CommitOwnerClientSuite extends QueryTest with DeltaSQLTestUtils with SharedSparkSession
  with DeltaSQLCommandTest {

  private trait TestCommitOwnerClientBase extends CommitOwnerClient {
    override def commit(
        logStore: LogStore,
        hadoopConf: Configuration,
        logPath: Path,
        managedCommitTableConf: Map[String, String],
        commitVersion: Long,
        actions: Iterator[String],
        updatedActions: UpdatedActions): CommitResponse = {
      throw new UnsupportedOperationException("Not implemented")
    }

    override def getCommits(
      logPath: Path,
      managedCommitTableConf: Map[String, String],
      startVersion: Option[Long],
      endVersion: Option[Long] = None): GetCommitsResponse = GetCommitsResponse(Seq.empty, -1)

    override def backfillToVersion(
        logStore: LogStore,
        hadoopConf: Configuration,
        logPath: Path,
        managedCommitTableConf: Map[String, String],
        version: Long,
        lastKnownBackfilledVersion: Option[Long]): Unit = {}

    override def semanticEquals(other: CommitOwnerClient): Boolean = this == other
  }

  private class TestCommitOwnerClient1 extends TestCommitOwnerClientBase
  private class TestCommitOwnerClient2 extends TestCommitOwnerClientBase

  override def beforeEach(): Unit = {
    super.beforeEach()
    CommitOwnerProvider.clearNonDefaultBuilders()
    CommitOwnerProvider.registerBuilder(InMemoryCommitOwnerBuilder(batchSize = 1))
  }

  test("registering multiple commit-owner builders with same name") {
    object Builder1 extends CommitOwnerBuilder {
      override def build(conf: Map[String, String]): CommitOwnerClient = null
      override def name: String = "builder-1"
    }
    object BuilderWithSameName extends CommitOwnerBuilder {
      override def build(conf: Map[String, String]): CommitOwnerClient = null
      override def name: String = "builder-1"
    }
    object Builder3 extends CommitOwnerBuilder {
      override def build(conf: Map[String, String]): CommitOwnerClient = null
      override def name: String = "builder-3"
    }
    CommitOwnerProvider.registerBuilder(Builder1)
    intercept[Exception] {
      CommitOwnerProvider.registerBuilder(BuilderWithSameName)
    }
    CommitOwnerProvider.registerBuilder(Builder3)
  }

  test("getCommitOwner - builder returns same object") {
    object Builder1 extends CommitOwnerBuilder {
      val cs1 = new TestCommitOwnerClient1()
      val cs2 = new TestCommitOwnerClient2()
      override def build(conf: Map[String, String]): CommitOwnerClient = {
        conf.getOrElse("url", "") match {
          case "url1" => cs1
          case "url2" => cs2
          case _ => throw new IllegalArgumentException("Invalid url")
        }
      }
      override def name: String = "cs-x"
    }
    CommitOwnerProvider.registerBuilder(Builder1)
    val cs1 = CommitOwnerProvider.getCommitOwnerClient("cs-x", Map("url" -> "url1"))
    assert(cs1.isInstanceOf[TestCommitOwnerClient1])
    val cs1_again = CommitOwnerProvider.getCommitOwnerClient("cs-x", Map("url" -> "url1"))
    assert(cs1 eq cs1_again)
    val cs2 = CommitOwnerProvider.getCommitOwnerClient("cs-x", Map("url" -> "url2", "a" -> "b"))
    assert(cs2.isInstanceOf[TestCommitOwnerClient2])
    // If builder receives a config which doesn't have expected params, then it can throw exception.
    intercept[IllegalArgumentException] {
      CommitOwnerProvider.getCommitOwnerClient("cs-x", Map("url" -> "url3"))
    }
  }

  test("getCommitOwnerClient - builder returns new object each time") {
    object Builder1 extends CommitOwnerBuilder {
      override def build(conf: Map[String, String]): CommitOwnerClient = {
        conf.getOrElse("url", "") match {
          case "url1" => new TestCommitOwnerClient1()
          case _ => throw new IllegalArgumentException("Invalid url")
        }
      }
      override def name: String = "cs-name"
    }
    CommitOwnerProvider.registerBuilder(Builder1)
    val cs1 = CommitOwnerProvider.getCommitOwnerClient("cs-name", Map("url" -> "url1"))
    assert(cs1.isInstanceOf[TestCommitOwnerClient1])
    val cs1_again = CommitOwnerProvider.getCommitOwnerClient("cs-name", Map("url" -> "url1"))
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

  test("Semantic Equality works as expected on CommitOwnerClients") {
    class TestCommitOwnerClient(val key: String) extends TestCommitOwnerClientBase {
      override def semanticEquals(other: CommitOwnerClient): Boolean =
        other.isInstanceOf[TestCommitOwnerClient] &&
          other.asInstanceOf[TestCommitOwnerClient].key == key
    }
    object Builder1 extends CommitOwnerBuilder {
      override def build(conf: Map[String, String]): CommitOwnerClient = {
        new TestCommitOwnerClient(conf("key"))
      }
      override def name: String = "cs-name"
    }
    CommitOwnerProvider.registerBuilder(Builder1)

    // Different CommitOwner with same keys should be semantically equal.
    val obj1 = CommitOwnerProvider.getCommitOwnerClient("cs-name", Map("key" -> "url1"))
    val obj2 = CommitOwnerProvider.getCommitOwnerClient("cs-name", Map("key" -> "url1"))
    assert(obj1 != obj2)
    assert(obj1.semanticEquals(obj2))

    // Different CommitOwner with different keys should be semantically unequal.
    val obj3 = CommitOwnerProvider.getCommitOwnerClient("cs-name", Map("key" -> "url2"))
    assert(obj1 != obj3)
    assert(!obj1.semanticEquals(obj3))
  }

  private def checkMissing[Interface: TypeTag, Class: TypeTag](): Set[String] = {
    val fields = typeOf[Class].decls.collect {
      case m: MethodSymbol if m.isCaseAccessor => m.name.toString
    }

    val getters = typeOf[Interface].decls.collect {
      case m: MethodSymbol if m.isAbstract => m.name.toString
    }.toSet

    fields.filterNot { field =>
      getters.contains(s"get${field.capitalize}")
    }.toSet
  }

  /**
   * We expect the Protocol action to have the same fields as AbstractProtocol (part of the
   * CommitOwnerClient interface). With this if any change has happened in the Protocol of the
   * table, the same change is propagated to the CommitOwnerClient as AbstractProtocol. The
   * CommitOwnerClient can access the changes using getters and decide to act on the changes
   * based on the spec of the commit owner.
   *
   * This test case ensures that any new field added in the Protocol action is also accessible in
   * the CommitOwnerClient via the getter. If the new field is something which we do not expect to
   * be passed to the CommitOwnerClient, the test needs to be modified accordingly.
   */
  test("AbstractProtocol should have getter methods for all fields in Protocol") {
    val missingFields = checkMissing[AbstractProtocol, Protocol]()
    val expectedMissingFields = Set.empty[String]
    assert(missingFields == expectedMissingFields,
      s"Missing getter methods in AbstractProtocol")
  }

  /**
   * We expect the Metadata action to have the same fields as AbstractMetadata (part of the
   * CommitOwnerClient interface). With this if any change has happened in the Metadata of the
   * table, the same change is propagated to the CommitOwnerClient as AbstractMetadata. The
   * CommitOwnerClient can access the changes using getters and decide to act on the changes
   * based on the spec of the commit owner.
   *
   * This test case ensures that any new field added in the Metadata action is also accessible in
   * the CommitOwnerClient via the getter. If the new field is something which we do not expect to
   * be passed to the CommitOwnerClient, the test needs to be modified accordingly.
   */
  test("BaseMetadata should have getter methods for all fields in Metadata") {
    val missingFields = checkMissing[AbstractMetadata, Metadata]()
    val expectedMissingFields = Set("format")
    assert(missingFields == expectedMissingFields,
      s"Missing getter methods in AbstractMetadata")
  }
}
