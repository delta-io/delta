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

package org.apache.spark.sql.delta.coordinatedcommits

import java.util.Optional

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe._

import org.apache.spark.sql.delta.{CoordinatedCommitsTableFeature, DeltaConfigs, DeltaLog, DeltaOperations}
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaSQLTestUtils
import io.delta.storage.LogStore
import io.delta.storage.commit.{CommitCoordinatorClient, CommitResponse, GetCommitsResponse => JGetCommitsResponse, TableDescriptor, TableIdentifier, UpdatedActions}
import io.delta.storage.commit.actions.{AbstractMetadata, AbstractProtocol}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{QueryTest, SparkSession}
import org.apache.spark.sql.test.SharedSparkSession

class CommitCoordinatorClientSuite extends QueryTest with DeltaSQLTestUtils with SharedSparkSession
  with DeltaSQLCommandTest {

  private trait TestCommitCoordinatorClientBase extends CommitCoordinatorClient {
    override def commit(
        logStore: LogStore,
        hadoopConf: Configuration,
        tableDesc: TableDescriptor,
        commitVersion: Long,
        actions: java.util.Iterator[String],
        updatedActions: UpdatedActions): CommitResponse = {
      throw new UnsupportedOperationException("Not implemented")
    }

    override def getCommits(
        tableDesc: TableDescriptor,
        startVersion: java.lang.Long,
        endVersion: java.lang.Long): JGetCommitsResponse =
      new JGetCommitsResponse(Seq.empty.asJava, -1)

    override def backfillToVersion(
        logStore: LogStore,
        hadoopConf: Configuration,
        tableDesc: TableDescriptor,
        version: Long,
        lastKnownBackfilledVersion: java.lang.Long): Unit = {}

    override def registerTable(
        logPath: Path,
        tableIdentifier: Optional[TableIdentifier],
        currentVersion: Long,
        currentMetadata: AbstractMetadata,
        currentProtocol: AbstractProtocol): java.util.Map[String, String] =
      Map.empty[String, String].asJava

    override def semanticEquals(other: CommitCoordinatorClient): Boolean = this == other
  }

  private class TestCommitCoordinatorClient1 extends TestCommitCoordinatorClientBase
  private class TestCommitCoordinatorClient2 extends TestCommitCoordinatorClientBase

  override def beforeEach(): Unit = {
    super.beforeEach()
    CommitCoordinatorProvider.clearNonDefaultBuilders()
    CommitCoordinatorProvider.registerBuilder(InMemoryCommitCoordinatorBuilder(batchSize = 1))
  }

  test("registering multiple commit-coordinator builders with same name") {
    object Builder1 extends CommitCoordinatorBuilder {
      override def build(
          spark: SparkSession, conf: Map[String, String]): CommitCoordinatorClient = null
      override def getName: String = "builder-1"
    }
    object BuilderWithSameName extends CommitCoordinatorBuilder {
      override def build(
          spark: SparkSession, conf: Map[String, String]): CommitCoordinatorClient = null
      override def getName: String = "builder-1"
    }
    object Builder3 extends CommitCoordinatorBuilder {
      override def build(
          spark: SparkSession, conf: Map[String, String]): CommitCoordinatorClient = null
      override def getName: String = "builder-3"
    }
    CommitCoordinatorProvider.registerBuilder(Builder1)
    intercept[Exception] {
      CommitCoordinatorProvider.registerBuilder(BuilderWithSameName)
    }
    CommitCoordinatorProvider.registerBuilder(Builder3)
  }

  test("getCommitCoordinator - builder returns same object") {
    object Builder1 extends CommitCoordinatorBuilder {
      val cs1 = new TestCommitCoordinatorClient1()
      val cs2 = new TestCommitCoordinatorClient2()
      override def build(
          spark: SparkSession, conf: Map[String, String]): CommitCoordinatorClient = {
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
      CommitCoordinatorProvider.getCommitCoordinatorClient("cs-x", Map("url" -> "url1"), spark)
    assert(cs1.isInstanceOf[TestCommitCoordinatorClient1])
    val cs1_again =
      CommitCoordinatorProvider.getCommitCoordinatorClient("cs-x", Map("url" -> "url1"), spark)
    assert(cs1 eq cs1_again)
    val cs2 = CommitCoordinatorProvider
      .getCommitCoordinatorClient("cs-x", Map("url" -> "url2", "a" -> "b"), spark)
    assert(cs2.isInstanceOf[TestCommitCoordinatorClient2])
    // If builder receives a config which doesn't have expected params, then it can throw exception.
    intercept[IllegalArgumentException] {
      CommitCoordinatorProvider.getCommitCoordinatorClient("cs-x", Map("url" -> "url3"), spark)
    }
  }

  test("getCommitCoordinatorClient - builder returns new object each time") {
    object Builder1 extends CommitCoordinatorBuilder {
      override def build(
          spark: SparkSession, conf: Map[String, String]): CommitCoordinatorClient = {
        conf.getOrElse("url", "") match {
          case "url1" => new TestCommitCoordinatorClient1()
          case _ => throw new IllegalArgumentException("Invalid url")
        }
      }
      override def getName: String = "cs-name"
    }
    CommitCoordinatorProvider.registerBuilder(Builder1)
    val cs1 =
      CommitCoordinatorProvider.getCommitCoordinatorClient("cs-name", Map("url" -> "url1"), spark)
    assert(cs1.isInstanceOf[TestCommitCoordinatorClient1])
    val cs1_again =
      CommitCoordinatorProvider.getCommitCoordinatorClient("cs-name", Map("url" -> "url1"), spark)
    assert(cs1 ne cs1_again)
  }

  test("COORDINATED_COMMITS_PROVIDER_CONF") {
    val m1 = Metadata(
      configuration = Map(
        DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_CONF.key ->
          """{"key1": "string_value", "key2Int": 2, "key3ComplexStr": "\"hello\""}""")
    )
    assert(DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_CONF.fromMetaData(m1) ===
      Map("key1" -> "string_value", "key2Int" -> "2", "key3ComplexStr" -> "\"hello\""))

    val m2 = Metadata(
      configuration = Map(
        DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_CONF.key ->
          """{"key1": "string_value", "key2Int": "2""")
    )
    intercept[com.fasterxml.jackson.core.JsonParseException] {
      DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_CONF.fromMetaData(m2)
    }
  }

  test("Commit fails if we try to put bad value for COORDINATED_COMMITS_PROVIDER_CONF") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(10).write.format("delta").mode("append").save(path)
      val deltaLog = DeltaLog.forTable(spark, path)

      val metadataWithCorrectConf = Metadata(
        configuration = Map(
          DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_CONF.key ->
            """{"key1": "string_value", "key2Int": 2, "key3ComplexStr": "\"hello\""}""")
      )
      val metadataWithIncorrectConf = Metadata(
        configuration = Map(
          DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_CONF.key ->
            """{"key1": "string_value", "key2Int": "2""")
      )

      intercept[com.fasterxml.jackson.core.JsonParseException] {
        deltaLog.startTransaction().commit(
          Seq(metadataWithIncorrectConf), DeltaOperations.ManualUpdate)
      }
      DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_CONF.fromMetaData(metadataWithCorrectConf)
    }
  }

  test(
    "Adding COORDINATED_COMMITS_PROVIDER_NAME table property automatically upgrades the Protocol") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(10).write.format("delta").mode("append").save(path)
      val metadata = Metadata(
          configuration = Map(DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_NAME.key -> "in-memory"))
      val deltaLog = DeltaLog.forTable(spark, path)

      def getWriterFeatures(log: DeltaLog): Set[String] = {
        log.update().protocol.writerFeatures.getOrElse(Set.empty)
      }

      assert(!getWriterFeatures(deltaLog).contains(CoordinatedCommitsTableFeature.name))
      deltaLog.startTransaction().commit(Seq(metadata), DeltaOperations.ManualUpdate)
      assert(getWriterFeatures(deltaLog).contains(CoordinatedCommitsTableFeature.name))
    }
  }

  test("Semantic Equality works as expected on CommitCoordinatorClients") {
    class TestCommitCoordinatorClient(val key: String) extends TestCommitCoordinatorClientBase {
      override def semanticEquals(other: CommitCoordinatorClient): Boolean =
        other.isInstanceOf[TestCommitCoordinatorClient] &&
          other.asInstanceOf[TestCommitCoordinatorClient].key == key
    }
    object Builder1 extends CommitCoordinatorBuilder {
      override def build(
          spark: SparkSession, conf: Map[String, String]): CommitCoordinatorClient = {
        new TestCommitCoordinatorClient(conf("key"))
      }
      override def getName: String = "cs-name"
    }
    CommitCoordinatorProvider.registerBuilder(Builder1)

    // Different CommitCoordinator with same keys should be semantically equal.
    val obj1 =
      CommitCoordinatorProvider.getCommitCoordinatorClient("cs-name", Map("key" -> "url1"), spark)
    val obj2 =
      CommitCoordinatorProvider.getCommitCoordinatorClient("cs-name", Map("key" -> "url1"), spark)
    assert(obj1 != obj2)
    assert(obj1.semanticEquals(obj2))

    // Different CommitCoordinator with different keys should be semantically unequal.
    val obj3 =
      CommitCoordinatorProvider.getCommitCoordinatorClient("cs-name", Map("key" -> "url2"), spark)
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
   * CommitCoordinatorClient interface). With this if any change has happened in the Protocol of the
   * table, the same change is propagated to the CommitCoordinatorClient as AbstractProtocol. The
   * CommitCoordinatorClient can access the changes using getters and decide to act on the changes
   * based on the spec of the commit coordinator.
   *
   * This test case ensures that any new field added in the Protocol action is also accessible in
   * the CommitCoordinatorClient via the getter. If the new field is something which we do not
   * expect to be passed to the CommitCoordinatorClient, the test needs to be modified accordingly.
   */
  test("AbstractProtocol should have getter methods for all fields in Protocol") {
    val missingFields = checkMissing[AbstractProtocol, Protocol]()
    val expectedMissingFields = Set.empty[String]
    assert(missingFields == expectedMissingFields,
      s"Missing getter methods in AbstractProtocol")
  }

  /**
   * We expect the Metadata action to have the same fields as AbstractMetadata (part of the
   * CommitCoordinatorClient interface). With this if any change has happened in the Metadata of the
   * table, the same change is propagated to the CommitCoordinatorClient as AbstractMetadata. The
   * CommitCoordinatorClient can access the changes using getters and decide to act on the changes
   * based on the spec of the commit coordinator.
   *
   * This test case ensures that any new field added in the Metadata action is also accessible in
   * the CommitCoordinatorClient via the getter. If the new field is something which we do not
   * expect to be passed to the CommitCoordinatorClient, the test needs to be modified accordingly.
   */
  test("BaseMetadata should have getter methods for all fields in Metadata") {
    val missingFields = checkMissing[AbstractMetadata, Metadata]()
    val expectedMissingFields = Set("format")
    assert(missingFields == expectedMissingFields,
      s"Missing getter methods in AbstractMetadata")
  }
}
