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

package io.delta.kernel.defaults.catalogManaged

import scala.collection.JavaConverters._

import io.delta.kernel.{Operation, TableManager, Transaction}
import io.delta.kernel.commit.{CommitMetadata, CommitResponse, Committer}
import io.delta.kernel.data.Row
import io.delta.kernel.defaults.utils.TestUtils
import io.delta.kernel.engine.Engine
import io.delta.kernel.internal.SnapshotImpl
import io.delta.kernel.internal.files.ParsedLogData
import io.delta.kernel.internal.tablefeatures.TableFeatures
import io.delta.kernel.internal.util.FileNames
import io.delta.kernel.types.{IntegerType, StructType}
import io.delta.kernel.utils.{CloseableIterator, FileStatus}
import io.delta.kernel.utils.CloseableIterable.emptyIterable

import org.scalatest.funsuite.AnyFunSuite

class CatalogManagedEnablementSuite extends AnyFunSuite with TestUtils {

  case class CatalogManagedEnablementTestCase(
      testName: String,
      operationType: String, // "CREATE" or "UPDATE"
      initialTableProperties: Map[String, String] = Map.empty,
      transactionProperties: Map[String, String],
      expectedSuccess: Boolean,
      expectedExceptionMessage: Option[String] = None,
      expectedIctEnabled: Boolean = false,
      expectedCatalogManagedSupported: Boolean = false)

  val catalogManagedTestCases = Seq(
    CatalogManagedEnablementTestCase(
      testName = "CREATE: catalogManaged enablement flag => enables catalogManaged and ICT",
      operationType = "CREATE",
      transactionProperties = Map("delta.feature.catalogOwned-preview" -> "supported"),
      expectedSuccess = true,
      expectedIctEnabled = true,
      expectedCatalogManagedSupported = true),
    CatalogManagedEnablementTestCase(
      testName = "UPDATE: catalogManaged enablement flag => enables catalogManaged and ICT",
      operationType = "UPDATE",
      initialTableProperties = Map.empty, // Start with basic table
      transactionProperties = Map("delta.feature.catalogOwned-preview" -> "supported"),
      expectedSuccess = true,
      expectedIctEnabled = true,
      expectedCatalogManagedSupported = true),
    CatalogManagedEnablementTestCase(
      testName = "UPDATE: catalogManaged enablement flag => enables ICT if previously disabled",
      operationType = "UPDATE",
      initialTableProperties = Map("delta.enableInCommitTimestamps" -> "false"),
      transactionProperties = Map("delta.feature.catalogOwned-preview" -> "supported"),
      expectedSuccess = true,
      expectedIctEnabled = true,
      expectedCatalogManagedSupported = true),
    CatalogManagedEnablementTestCase(
      testName =
        "ILLEGAL CREATE: catalogManaged enablement flag but ICT explicitly disabled too => THROW",
      operationType = "CREATE",
      transactionProperties = Map(
        "delta.feature.catalogOwned-preview" -> "supported",
        "delta.enableInCommitTimestamps" -> "false"),
      expectedSuccess = false,
      expectedExceptionMessage =
        Some("Cannot disable inCommitTimestamp when enabling catalogManaged")),
    CatalogManagedEnablementTestCase(
      testName =
        "ILLEGAL UPDATE: catalogManaged enablement flag but ICT explicitly disabled too => THROW",
      operationType = "UPDATE",
      initialTableProperties = Map.empty,
      transactionProperties = Map(
        "delta.feature.catalogOwned-preview" -> "supported",
        "delta.enableInCommitTimestamps" -> "false"),
      expectedSuccess = false,
      expectedExceptionMessage =
        Some("Cannot disable inCommitTimestamp when enabling catalogManaged")),
    CatalogManagedEnablementTestCase(
      testName = "UPDATE: catalogManaged enablement flag => ICT already enabled",
      operationType = "UPDATE",
      initialTableProperties = Map("delta.enableInCommitTimestamps" -> "true"),
      transactionProperties = Map("delta.feature.catalogOwned-preview" -> "supported"),
      expectedSuccess = true,
      expectedIctEnabled = true,
      expectedCatalogManagedSupported = true),
    CatalogManagedEnablementTestCase(
      testName = "No-op: catalogOwned not being enabled should not affect ICT",
      operationType = "UPDATE",
      initialTableProperties = Map.empty,
      transactionProperties = Map(),
      expectedSuccess = true,
      expectedIctEnabled = false,
      expectedCatalogManagedSupported = false))

  catalogManagedTestCases.foreach { testCase =>
    test(testCase.testName) {
      withTempDir { tempDir =>
        val tablePath = tempDir.getAbsolutePath
        val schema = new StructType().add("id", IntegerType.INTEGER)

        // Setup initial table if this is an UPDATE operation
        if (testCase.operationType == "UPDATE") {
          TableManager
            .buildCreateTableTransaction(tablePath, schema, "engineInfo")
            .withTableProperties(testCase.initialTableProperties.asJava)
            .withCommitter(committerUsingPutIfAbsent)
            .build(defaultEngine)
            .commit(defaultEngine, emptyIterable[Row])
        }

        // CreateTableTransactionBuilder and UpdateTableTransactionBuilder don't share a common
        // parent interface. So, we treat the `txnBuilder` as a trait that has a `build(engine)`
        // method. Scalastyle doesn't like this, but it's valid.
        //
        // scalastyle:off
        val txnBuilder: { def build(engine: Engine): Transaction } = testCase.operationType match {
          case "CREATE" =>
            TableManager
              .buildCreateTableTransaction(tablePath, schema, "engineInfo")
              .withTableProperties(testCase.transactionProperties.asJava)
              .withCommitter(committerUsingPutIfAbsent)

          case "UPDATE" =>
            TableManager
              .loadSnapshot(tablePath)
              .withCommitter(committerUsingPutIfAbsent)
              .build(defaultEngine)
              .buildUpdateTableTransaction("engineInfo", Operation.MANUAL_UPDATE)
              .withTablePropertiesAdded(testCase.transactionProperties.asJava)
        }
        // scalastyle:on

        if (testCase.expectedSuccess) {
          // Transaction building should succeed
          txnBuilder.build(defaultEngine).commit(defaultEngine, emptyIterable[Row])

          // Verify the results
          val snapshot = TableManager
            .loadSnapshot(tablePath)
            .build(defaultEngine)
            .asInstanceOf[SnapshotImpl]

          // Check if catalogManaged feature is supported
          val catalogManagedSupported = snapshot.getProtocol
            .supportsFeature(TableFeatures.CATALOG_MANAGED_R_W_FEATURE_PREVIEW)
          assert(
            catalogManagedSupported == testCase.expectedCatalogManagedSupported,
            s"Expected catalogManaged supported: ${testCase.expectedCatalogManagedSupported}, " +
              s"but was: $catalogManagedSupported")

          // Check if ICT is enabled in metadata
          val ictEnabled = snapshot.getMetadata.getConfiguration.asScala
            .get("delta.enableInCommitTimestamps")
            .contains("true")
          assert(
            ictEnabled == testCase.expectedIctEnabled,
            s"Expected ICT enabled: ${testCase.expectedIctEnabled}, but was: $ictEnabled")

          // If catalogManaged is supported, ICT feature should also be supported
          if (testCase.expectedCatalogManagedSupported) {
            assert(
              snapshot.getProtocol.supportsFeature(TableFeatures.IN_COMMIT_TIMESTAMP_W_FEATURE))
          }

        } else {
          // Transaction building should fail
          val exception = intercept[Exception] {
            txnBuilder.build(defaultEngine).commit(defaultEngine, emptyIterable[Row])
          }

          testCase.expectedExceptionMessage.foreach { expectedMsg =>
            assert(
              exception.getMessage.contains(expectedMsg),
              s"Expected exception message to contain '$expectedMsg', " +
                s"but was: '${exception.getMessage}'")
          }
        }
      }
    }
  }
}
