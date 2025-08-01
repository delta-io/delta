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

package io.delta.kernel.internal.commit

import java.util.Optional

import io.delta.kernel.TableManager
import io.delta.kernel.commit.CommitMetadata
import io.delta.kernel.internal.actions.Protocol
import io.delta.kernel.test.{ActionUtils, MockFileSystemClientUtils, VectorTestUtils}
import io.delta.kernel.types.{IntegerType, StructType}

import org.scalatest.funsuite.AnyFunSuite

class DefaultCommitterSuite extends AnyFunSuite
    with MockFileSystemClientUtils
    with ActionUtils
    with VectorTestUtils {

  private val protocol12 = new Protocol(1, 2)

  Seq(
    (protocol12, protocolWithCatalogManagedSupport, "Upgrade"),
    (protocolWithCatalogManagedSupport, protocol12, "Downgrade"),
    (
      protocolWithCatalogManagedSupport,
      protocolWithCatalogManagedSupport,
      "CatalogManagedWrite")).foreach { case (readProtocol, newProtocol, testCase) =>
    test(s"default committer does not support committing to catalog-managed tables -- $testCase") {
      val emptyMockEngine = createMockFSListFromEngine(Nil)
      val schema = new StructType().add("col1", IntegerType.INTEGER)
      val metadata = testMetadata(schema, Seq[String]())
      val committer = TableManager.loadSnapshot(dataPath.toString)
        .withProtocolAndMetadata(readProtocol, metadata)
        .atVersion(1)
        .build(emptyMockEngine)
        .getCommitter

      assert(committer.isInstanceOf[DefaultFileSystemManagedTableOnlyCommitter])

      val exMsg = intercept[UnsupportedOperationException] {
        committer.commit(
          emptyMockEngine,
          emptyActionsIterator,
          new CommitMetadata(
            3L,
            "/fake/_delta_log",
            testCommitInfo,
            Optional.of(readProtocol),
            Optional.of(metadata),
            Optional.of(newProtocol),
            Optional.of(metadata)))
      }.getMessage

      assert(exMsg.contains("No io.delta.kernel.commit.Committer has been provided to Kernel, so " +
        "Kernel is using a default Committer that only supports committing to " +
        "filesystem-managed Delta tables, not catalog-managed Delta tables. Since this table " +
        "is catalog-managed, this commit operation is unsupported"))
    }
  }

}
