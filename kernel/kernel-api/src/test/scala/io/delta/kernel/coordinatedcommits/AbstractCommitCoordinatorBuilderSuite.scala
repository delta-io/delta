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

package io.delta.kernel.coordinatedcommits

import io.delta.kernel.TableIdentifier
import io.delta.kernel.config.{ConfigurationProvider, MapBasedConfigurationProvider}
import io.delta.kernel.coordinatedcommits.AbstractCommitCoordinatorBuilder._
import io.delta.kernel.data.Row
import io.delta.kernel.engine.Engine
import io.delta.kernel.engine.coordinatedcommits.{CommitResponse, GetCommitsResponse, UpdatedActions}
import io.delta.kernel.engine.coordinatedcommits.actions.{AbstractMetadata, AbstractProtocol}
import io.delta.kernel.exceptions.KernelException
import io.delta.kernel.utils.CloseableIterator
import org.scalatest.funsuite.AnyFunSuite

import java.util.Collections

class AbstractCommitCoordinatorBuilderSuite extends AnyFunSuite {
  test("buildCommitCoordinatorClient with builder that returns same instance") {
    val configProvider = new MapBasedConfigurationProvider(
      Map(getCommitCoordinatorBuilderConfKey("cc-x") -> classOf[SameInstanceCCBuilder].getName)
    )
    val coordinatorConf: java.util.Map[String, String] = Collections.emptyMap()
    val cccA = buildCommitCoordinatorClient("cc-x", configProvider, coordinatorConf)
    val cccB = buildCommitCoordinatorClient("cc-x", configProvider, coordinatorConf)
    assert(cccA eq cccB)
    assert(cccA.semanticEquals(cccB))
  }

  test("buildCommitCoordinatorClient with builder that returns new instance") {
    val configProvider = new MapBasedConfigurationProvider(
      Map(getCommitCoordinatorBuilderConfKey("cc-x") -> classOf[NewInstanceCCBuilder].getName)
    )
    val coordinatorConf: java.util.Map[String, String] = Collections.emptyMap()
    val cccA = buildCommitCoordinatorClient("cc-x", configProvider, coordinatorConf)
    val cccB = buildCommitCoordinatorClient("cc-x", configProvider, coordinatorConf)
    assert(!(cccA eq cccB))
    assert(cccA.semanticEquals(cccB))
  }

  test("buildCommitCoordinatorClient throws if builderConfKey not found") {
    val configProvider = new MapBasedConfigurationProvider(Map.empty[String, String])
    val coordinatorConf: java.util.Map[String, String] = Collections.emptyMap()
    val e = intercept[KernelException] {
      buildCommitCoordinatorClient("cc-x", configProvider, coordinatorConf)
    }.getMessage
    assert(e == "Unknown commit coordinator: 'cc-x'. Please ensure that session config " +
      "'io.delta.kernel.commitCoordinatorBuilder.cc-x.impl' is set.")
  }

  test("buildCommitCoordinatorClient throws if Builder cannot be constructed") {
    val coordinatorConf: java.util.Map[String, String] = Collections.emptyMap()

    val configProvider1 = new MapBasedConfigurationProvider(
      Map(getCommitCoordinatorBuilderConfKey("cc-x") -> classOf[BadCCBuilder1].getName)
    )
    val e1 = intercept[KernelException] {
      buildCommitCoordinatorClient("cc-x", configProvider1, coordinatorConf)
    }.getMessage
    assert (e1 == "Could not instantiate Commit Coordinator Client for 'cc-x' using builder " +
      "class 'io.delta.kernel.coordinatedcommits.BadCCBuilder1'.")

    val configProvider2 = new MapBasedConfigurationProvider(
      Map(getCommitCoordinatorBuilderConfKey("cc-x") -> classOf[BadCCBuilder2].getName)
    )
    val e2 = intercept[KernelException] {
      buildCommitCoordinatorClient("cc-x", configProvider2, coordinatorConf)
    }.getMessage
    assert(e2 == "Could not instantiate Commit Coordinator Client for 'cc-x' using builder " +
      "class 'io.delta.kernel.coordinatedcommits.BadCCBuilder2'.")
  }
}

////////////////////
// Helper Classes //
////////////////////

private class TestCommitCoordinatorClient extends CommitCoordinatorClient {

  override def registerTable(
      engine: Engine,
      logPath: String,
      tableIdentifier: TableIdentifier,
      currentVersion: Long,
      currentMetadata: AbstractMetadata,
      currentProtocol: AbstractProtocol): java.util.Map[String, String] = {
    throw new UnsupportedOperationException("Not implemented")
  }

  override def commit(
      engine: Engine,
      tableDescriptor: TableDescriptor,
      commitVersion: Long,
      actions: CloseableIterator[Row],
      updatedActions: UpdatedActions): CommitResponse = {
    throw new UnsupportedOperationException("Not implemented")
  }

  override def getCommits(
      engine: Engine,
      tableDescriptor: TableDescriptor,
      startVersion: java.lang.Long,
      endVersion: java.lang.Long): GetCommitsResponse = {
    throw new UnsupportedOperationException("Not implemented")
  }

  override def backfillToVersion(
      engine: Engine,
      tableDescriptor: TableDescriptor,
      version: Long,
      lastKnownBackfilledVersion: java.lang.Long): Unit = {
    throw new UnsupportedOperationException("Not implemented")
  }

  override def semanticEquals(other: CommitCoordinatorClient): Boolean = {
    other.isInstanceOf[TestCommitCoordinatorClient]
  }
}

/** Builder that returns the same instance each build invocation. */
private class SameInstanceCCBuilder extends AbstractCommitCoordinatorBuilder {
  override def getName: String = "cc-x"

  override def build(
      sessionConfig: ConfigurationProvider,
      commitCoordinatorConf: java.util.Map[String, String]): CommitCoordinatorClient = {
    SameInstanceCCBuilder.ccc1
  }
}

private object SameInstanceCCBuilder {
  val ccc1 = new TestCommitCoordinatorClient()
}

/** Builder that returns a new CCC instance of each build invocation. */
private class NewInstanceCCBuilder extends AbstractCommitCoordinatorBuilder {
  override def getName: String = "cc-x"

  override def build(
      sessionConfig: ConfigurationProvider,
      commitCoordinatorConf: java.util.Map[String, String]): CommitCoordinatorClient = {
    new TestCommitCoordinatorClient()
  }
}

/** Builder that extends AbstractCommitCoordinatorBuilder but has a constructor with a parameter. */
private class BadCCBuilder1(param: Int) extends AbstractCommitCoordinatorBuilder {
  override def getName: String = "cc-x"

  override def build(
      sessionConfig: ConfigurationProvider,
      commitCoordinatorConf: java.util.Map[String, String]): CommitCoordinatorClient = {
    new TestCommitCoordinatorClient()
  }
}

/** Builder that does not extend AbstractCommitCoordinatorBuilder. */
private class BadCCBuilder2() {
  def getName: String = "cc-x"

  def build(
      sessionConfig: ConfigurationProvider,
      commitCoordinatorConf: java.util.Map[String, String]): CommitCoordinatorClient = {
    new TestCommitCoordinatorClient()
  }
}
