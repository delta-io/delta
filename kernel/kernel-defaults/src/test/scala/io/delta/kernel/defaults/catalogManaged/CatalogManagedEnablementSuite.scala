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
import io.delta.kernel.data.Row
import io.delta.kernel.defaults.utils.TestUtils
import io.delta.kernel.engine.Engine
import io.delta.kernel.internal.SnapshotImpl
import io.delta.kernel.internal.tablefeatures.TableFeatures
import io.delta.kernel.types.{IntegerType, StructType}
import io.delta.kernel.utils.CloseableIterable.emptyIterable

import org.scalatest.funsuite.AnyFunSuite

class CatalogManagedEnablementSuite extends AnyFunSuite with TestUtils {

  case class CatalogManagedTestCase(
      testName: String,
      operationType: String, // "CREATE", "UPDATE", or "REPLACE"
      initialTableProperties: Map[String, String] = Map.empty,
      transactionProperties: Map[String, String],
      expectedSuccess: Boolean,
      expectedExceptionMessage: Option[String] = None,
      expectedIctEnabled: Boolean = false,
      expectedCatalogManagedSupported: Boolean = false)

  val catalogManagedTestCases = Seq(
    // ===== CREATE cases =====
    CatalogManagedTestCase(
      testName = "CREATE: set catalogManaged=supported => enables catalogManaged and ICT",
      operationType = "CREATE",
      transactionProperties = Map("delta.feature.catalogOwned-preview" -> "supported"),
      expectedSuccess = true,
      expectedIctEnabled = true,
      expectedCatalogManagedSupported = true),
    CatalogManagedTestCase(
      testName = "ILLEGAL CREATE: set catalogManaged=supported and explicitly disable ICT => THROW",
      operationType = "CREATE",
      transactionProperties = Map(
        "delta.feature.catalogOwned-preview" -> "supported",
        "delta.enableInCommitTimestamps" -> "false"),
      expectedSuccess = false,
      expectedExceptionMessage =
        Some("Cannot disable inCommitTimestamp when enabling catalogManaged")),

    // ===== UPDATE cases =====
    CatalogManagedTestCase(
      testName = "UPDATE: set catalogManaged=supported => enables catalogManaged and ICT",
      operationType = "UPDATE",
      initialTableProperties = Map.empty, // Start with basic table
      transactionProperties = Map("delta.feature.catalogOwned-preview" -> "supported"),
      expectedSuccess = true,
      expectedIctEnabled = true,
      expectedCatalogManagedSupported = true),
    CatalogManagedTestCase(
      testName = "UPDATE: set catalogManaged=supported => enables ICT if previously disabled",
      operationType = "UPDATE",
      initialTableProperties = Map("delta.enableInCommitTimestamps" -> "false"),
      transactionProperties = Map("delta.feature.catalogOwned-preview" -> "supported"),
      expectedSuccess = true,
      expectedIctEnabled = true,
      expectedCatalogManagedSupported = true),
    CatalogManagedTestCase(
      testName = "UPDATE: set catalogManaged=supported and ICT already enabled => Okay",
      operationType = "UPDATE",
      initialTableProperties = Map("delta.enableInCommitTimestamps" -> "true"),
      transactionProperties = Map("delta.feature.catalogOwned-preview" -> "supported"),
      expectedSuccess = true,
      expectedIctEnabled = true,
      expectedCatalogManagedSupported = true),
    CatalogManagedTestCase(
      testName = "ILLEGAL UPDATE: set catalogManaged=supported and disable ICT => THROW",
      operationType = "UPDATE",
      initialTableProperties = Map.empty,
      transactionProperties = Map(
        "delta.feature.catalogOwned-preview" -> "supported",
        "delta.enableInCommitTimestamps" -> "false"),
      expectedSuccess = false,
      expectedExceptionMessage =
        Some("Cannot disable inCommitTimestamp when enabling catalogManaged")),
    CatalogManagedTestCase(
      testName = "ILLEGAL UPDATE: catalogManaged already supported, then disable ICT => THROW",
      operationType = "UPDATE",
      initialTableProperties = Map("delta.feature.catalogOwned-preview" -> "supported"),
      transactionProperties = Map("delta.enableInCommitTimestamps" -> "false"),
      expectedSuccess = false,
      expectedExceptionMessage =
        Some("Cannot disable inCommitTimestamp on a catalogManaged table")),
    CatalogManagedTestCase(
      testName = "NO-OP UPDATE: catalogManaged not being enabled should not affect ICT",
      operationType = "UPDATE",
      initialTableProperties = Map.empty,
      transactionProperties = Map(),
      expectedSuccess = true,
      expectedIctEnabled = false,
      expectedCatalogManagedSupported = false),

    // ===== REPLACE cases =====
    CatalogManagedTestCase(
      testName = "REPLACE: normal replace should succeed on a catalogManaged table",
      operationType = "REPLACE",
      initialTableProperties = Map("delta.feature.catalogOwned-preview" -> "supported"),
      transactionProperties = Map(),
      expectedSuccess = true,
      expectedIctEnabled = true,
      expectedCatalogManagedSupported = true),
    CatalogManagedTestCase(
      testName = "ILLEGAL REPLACE: set catalogManaged=supported => THROW",
      operationType = "REPLACE",
      initialTableProperties = Map.empty,
      transactionProperties = Map("delta.feature.catalogOwned-preview" -> "supported"),
      expectedSuccess = false,
      expectedExceptionMessage =
        Some("Cannot enable the catalogManaged feature during a REPLACE command.")),
    CatalogManagedTestCase(
      testName = "ILLEGAL REPLACE: catalogManaged already supported, then disable ICT => THROW",
      operationType = "REPLACE",
      initialTableProperties = Map("delta.feature.catalogOwned-preview" -> "supported"),
      transactionProperties = Map("delta.enableInCommitTimestamps" -> "false"),
      expectedSuccess = false,
      expectedExceptionMessage =
        Some("Cannot disable inCommitTimestamp on a catalogManaged table")))

  catalogManagedTestCases.foreach { testCase =>
    test(testCase.testName) {
      withTempDir { tempDir =>
        val tablePath = tempDir.getAbsolutePath
        val schema = new StructType().add("id", IntegerType.INTEGER)

        // Setup initial table if this is an UPDATE operation
        if (testCase.operationType == "UPDATE" || testCase.operationType == "REPLACE") {
          TableManager
            .buildCreateTableTransaction(tablePath, schema, "engineInfo")
            .withTableProperties(testCase.initialTableProperties.asJava)
            .withCommitter(committerUsingPutIfAbsent)
            .build(defaultEngine)
            .commit(defaultEngine, emptyIterable[Row])
        }

        // CREATE, UPDATE, and REPLACE txnBuilders don't share a common parent interface. So, we
        // treat the `txnBuilder` as a trait that has a `build(engine)` method. Scalastyle doesn't
        // like this, but it's valid.
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

          case "REPLACE" =>
            val replaceSchema = schema.add("col2", IntegerType.INTEGER)

            TableManager
              .loadSnapshot(tablePath)
              .withCommitter(committerUsingPutIfAbsent)
              .build(defaultEngine)
              .asInstanceOf[SnapshotImpl]
              .buildReplaceTableTransaction(replaceSchema, "engineInfo")
              .withTableProperties(testCase.transactionProperties.asJava)
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

          val protocol = snapshot.getProtocol

          // Check if catalogManaged feature is supported
          val catalogManagedSupported = protocol
            .supportsFeature(TableFeatures.CATALOG_MANAGED_R_W_FEATURE_PREVIEW)
          assert(catalogManagedSupported == testCase.expectedCatalogManagedSupported)

          // Check if ICT is enabled in metadata
          val ictEnabled = snapshot.getMetadata.getConfiguration.asScala
            .get("delta.enableInCommitTimestamps")
            .contains("true")
          assert(ictEnabled == testCase.expectedIctEnabled)

          // If catalogManaged is supported, ICT feature should also be supported
          if (testCase.expectedCatalogManagedSupported) {
            assert(protocol.supportsFeature(TableFeatures.IN_COMMIT_TIMESTAMP_W_FEATURE))
          }

        } else {
          // Transaction building should fail
          val exception = intercept[Exception] {
            txnBuilder.build(defaultEngine)
          }

          testCase.expectedExceptionMessage.foreach { expectedMsg =>
            assert(exception.getMessage.contains(expectedMsg))
          }
        }
      }
    }
  }
}
