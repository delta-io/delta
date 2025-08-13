/*
 * Copyright (2025) The Delta Lake Project Authors.
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

package io.delta.unity

import java.util.Optional

import scala.collection.JavaConverters._

import io.delta.kernel.commit.CommitMetadata
import io.delta.kernel.internal.tablefeatures.TableFeatures
import io.delta.kernel.test.VectorTestUtils

import org.scalatest.funsuite.AnyFunSuite

class UCCatalogManagedCommitterSuite
    extends AnyFunSuite
    with UCCatalogManagedTestUtils
    with VectorTestUtils {

  test("constructor throws on null inputs") {
    val ucClient = new InMemoryUCClient("ucMetastoreId")

    assertThrows[NullPointerException] {
      new UCCatalogManagedCommitter(null, "ucTableId", baseTestTablePath)
    }
    assertThrows[NullPointerException] {
      new UCCatalogManagedCommitter(ucClient, null, baseTestTablePath)
    }
    assertThrows[NullPointerException] {
      new UCCatalogManagedCommitter(ucClient, "ucTableId", null)
    }
  }

  test("commit throws on null inputs") {
    val ucClient = new InMemoryUCClient("ucMetastoreId")
    val committer = new UCCatalogManagedCommitter(ucClient, "ucTableId", baseTestTablePath)

    // Null engine
    assertThrows[NullPointerException] {
      committer.commit(null, emptyActionsIterator, catalogManagedWriteCommitMetadata())
    }

    // Null finalizedActions
    assertThrows[NullPointerException] {
      committer.commit(defaultEngine, null, catalogManagedWriteCommitMetadata())
    }

    // Null commitMetadata
    assertThrows[NullPointerException] {
      committer.commit(defaultEngine, emptyActionsIterator, null)
    }
  }

  test("commit throws if CommitMetadata is for a different table") {
    val ucClient = new InMemoryUCClient("ucMetastoreId")
    val committer = new UCCatalogManagedCommitter(ucClient, "ucTableId", baseTestTablePath)
    val badCommitMetadata = catalogManagedWriteCommitMetadata("/path/to/different/table/_delta_log")

    val exMsg = intercept[IllegalArgumentException] {
      committer.commit(defaultEngine, emptyActionsIterator, badCommitMetadata)
    }.getMessage

    assert(exMsg.contains("Delta log path '/path/to/table/_delta_log' does not match expected " +
      "'/path/to/different/table/_delta_log'"))
  }

  // ========== CommitType Tests START ==========

  implicit class CommitMetadataOps(base: CommitMetadata) {
    def withCommitTypeOverride(commitType: CommitMetadata.CommitType): CommitMetadata = {
      new CommitMetadata(
        base.getVersion(),
        base.getDeltaLogDirPath(),
        base.getCommitInfo(),
        base.getReadProtocolOpt(),
        base.getReadMetadataOpt(),
        base.getNewProtocolOpt(),
        base.getNewMetadataOpt()) {
        override def getCommitType() = commitType
      }
    }
  }

  val unsupportedCommitTypes = Seq(
    CommitMetadata.CommitType.FILESYSTEM_CREATE,
    CommitMetadata.CommitType.CATALOG_CREATE,
    CommitMetadata.CommitType.FILESYSTEM_WRITE,
    CommitMetadata.CommitType.FILESYSTEM_UPGRADE_TO_CATALOG,
    CommitMetadata.CommitType.CATALOG_DOWNGRADE_TO_FILESYSTEM)

  unsupportedCommitTypes foreach { commitType =>
    test(s"commit throws UnsupportedOperationException for $commitType") {
      val ucClient = new InMemoryUCClient("ucMetastoreId")
      val committer = new UCCatalogManagedCommitter(ucClient, "ucTableId", baseTestTablePath)
      val commitMetadata = catalogManagedWriteCommitMetadata().withCommitTypeOverride(commitType)

      assert(commitMetadata.getCommitType == commitType)

      val exception = intercept[UnsupportedOperationException] {
        committer.commit(defaultEngine, emptyActionsIterator, commitMetadata)
      }
      assert(exception.getMessage == s"Unsupported commit type: $commitType")
    }
  }

  // ========== CommitType Tests END ==========

  test("commit protocol change is currently not implemented") {
    val ucClient = new InMemoryUCClient("ucMetastoreId")
    val committer = new UCCatalogManagedCommitter(ucClient, "ucTableId", baseTestTablePath)
    val protocolUpgrade = protocolWithCatalogManagedSupport
      .withFeature(TableFeatures.DELETION_VECTORS_RW_FEATURE)
    val commitMetadata = createCommitMetadata(
      readProtocolOpt = Optional.of(protocolWithCatalogManagedSupport),
      readMetadataOpt = Optional.of(basicPartitionedMetadata),
      newProtocolOpt = Optional.of(protocolUpgrade))

    val exMsg = intercept[UnsupportedOperationException] {
      committer.commit(defaultEngine, emptyActionsIterator, commitMetadata)
    }.getMessage
    assert(exMsg.contains("Protocol change is not yet implemented"))
  }

  test("commit metadata change is currently not implemented") {
    val ucClient = new InMemoryUCClient("ucMetastoreId")
    val committer = new UCCatalogManagedCommitter(ucClient, "ucTableId", baseTestTablePath)
    val metadataUpgrade = basicPartitionedMetadata
      .withMergedConfiguration(Map("foo" -> "bar").asJava)
    val commitMetadata = createCommitMetadata(
      readProtocolOpt = Optional.of(protocolWithCatalogManagedSupport),
      readMetadataOpt = Optional.of(basicPartitionedMetadata),
      newMetadataOpt = Optional.of(metadataUpgrade))

    val exMsg = intercept[UnsupportedOperationException] {
      committer.commit(defaultEngine, emptyActionsIterator, commitMetadata)
    }.getMessage
    assert(exMsg.contains("Metadata change is not yet implemented"))
  }

  test("the remaining commit logic is not yet implemented") {
    val ucClient = new InMemoryUCClient("ucMetastoreId")
    val committer = new UCCatalogManagedCommitter(ucClient, "ucTableId", baseTestTablePath)
    val commitMetadata = catalogManagedWriteCommitMetadata()

    val exMsg = intercept[UnsupportedOperationException] {
      committer.commit(defaultEngine, emptyActionsIterator, commitMetadata)
    }.getMessage
    assert(exMsg.contains("Commit logic is not yet implemented"))
  }

}
