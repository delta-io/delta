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
import io.delta.kernel.commit.Committer
import io.delta.kernel.data.Row
import io.delta.kernel.defaults.utils.TestUtils
import io.delta.kernel.engine.Engine
import io.delta.kernel.internal.SnapshotImpl
import io.delta.kernel.internal.tablefeatures.TableFeatures
import io.delta.kernel.types.{IntegerType, StructType}
import io.delta.kernel.utils.CloseableIterable.emptyIterable

import org.scalatest.funsuite.AnyFunSuite

class CatalogManagedPropertyValidationSuite extends AnyFunSuite with TestUtils {

  val catalogManagedFeaturePropMap = Map("delta.feature.catalogManaged" -> "supported")
  val validRequiredCatalogPropMap = Map(
    customCatalogCommitter.REQUIRED_PROPERTY_KEY -> customCatalogCommitter.REQUIRED_PROPERTY_VALUE)
  val invalidRequiredCatalogPropMap = Map(
    customCatalogCommitter.REQUIRED_PROPERTY_KEY -> "invalid")

  case class CatalogManagedTestCase(
      testName: String,
      /** "CREATE", "UPDATE", or "REPLACE" */
      operationType: String,
      initialTableProperties: Map[String, String] = Map.empty,
      transactionProperties: Map[String, String],
      /** only applicable to UPDATE */
      removedPropertyKeys: Set[String] = Set.empty,
      /** create table for UPDATE/REPLACE */
      createInitialTableCommitter: Committer = customCatalogCommitter,
      expectedSuccess: Boolean = true,
      expectedExceptionMessage: Option[String] = None,
      /** only applicable if SUCCESS */
      expectedIctEnabled: Boolean = true,
      /** only applicable if SUCCESS */
      expectedCatalogManagedSupported: Boolean = true)

  val catalogManagedTestCases = Seq(
    // ===== CREATE cases =====
    CatalogManagedTestCase(
      testName = "CREATE: set catalogManaged=supported => enables catalogManaged and ICT",
      operationType = "CREATE",
      transactionProperties = catalogManagedFeaturePropMap),
    CatalogManagedTestCase(
      testName = "ILLEGAL CREATE: set catalogManaged=supported and explicitly disable ICT => THROW",
      operationType = "CREATE",
      transactionProperties = Map(
        "delta.feature.catalogManaged" -> "supported",
        "delta.enableInCommitTimestamps" -> "false"),
      expectedSuccess = false,
      expectedExceptionMessage =
        Some("Cannot disable inCommitTimestamp when enabling catalogManaged")),

    // ===== UPDATE cases =====
    CatalogManagedTestCase(
      testName = "UPDATE: set catalogManaged=supported => enables catalogManaged and ICT",
      operationType = "UPDATE",
      initialTableProperties = Map.empty, // Start with basic table
      transactionProperties = catalogManagedFeaturePropMap),
    CatalogManagedTestCase(
      testName = "UPDATE: set catalogManaged=supported => enables ICT if previously disabled",
      operationType = "UPDATE",
      initialTableProperties = Map("delta.enableInCommitTimestamps" -> "false"),
      transactionProperties = catalogManagedFeaturePropMap),
    CatalogManagedTestCase(
      testName = "UPDATE: set catalogManaged=supported and ICT already enabled => Okay",
      operationType = "UPDATE",
      initialTableProperties = Map("delta.enableInCommitTimestamps" -> "true"),
      transactionProperties = catalogManagedFeaturePropMap),
    CatalogManagedTestCase(
      testName = "ILLEGAL UPDATE: set catalogManaged=supported and disable ICT => THROW",
      operationType = "UPDATE",
      initialTableProperties = Map.empty,
      transactionProperties = Map(
        "delta.feature.catalogManaged" -> "supported",
        "delta.enableInCommitTimestamps" -> "false"),
      expectedSuccess = false,
      expectedExceptionMessage =
        Some("Cannot disable inCommitTimestamp when enabling catalogManaged")),
    CatalogManagedTestCase(
      testName = "ILLEGAL UPDATE: catalogManaged already supported, then disable ICT => THROW",
      operationType = "UPDATE",
      initialTableProperties = catalogManagedFeaturePropMap,
      transactionProperties = Map("delta.enableInCommitTimestamps" -> "false"),
      expectedSuccess = false,
      expectedExceptionMessage =
        Some("Cannot disable inCommitTimestamp on a catalogManaged table")),
    CatalogManagedTestCase(
      testName = "NO-OP UPDATE: catalogManaged not being enabled should not affect ICT",
      operationType = "UPDATE",
      initialTableProperties = Map.empty,
      transactionProperties = Map(),
      expectedIctEnabled = false,
      expectedCatalogManagedSupported = false),

    // ===== REPLACE cases =====
    CatalogManagedTestCase(
      testName = "REPLACE: normal replace should succeed on a catalogManaged table",
      operationType = "REPLACE",
      initialTableProperties = catalogManagedFeaturePropMap,
      transactionProperties = Map()),
    CatalogManagedTestCase(
      testName = "ILLEGAL REPLACE: set catalogManaged=supported => THROW",
      operationType = "REPLACE",
      initialTableProperties = Map.empty,
      transactionProperties = catalogManagedFeaturePropMap,
      expectedSuccess = false,
      expectedExceptionMessage =
        Some("Cannot enable the catalogManaged feature during a REPLACE command.")),
    CatalogManagedTestCase(
      testName = "ILLEGAL REPLACE: catalogManaged already supported, then disable ICT => THROW",
      operationType = "REPLACE",
      initialTableProperties = catalogManagedFeaturePropMap,
      transactionProperties = Map("delta.enableInCommitTimestamps" -> "false"),
      expectedSuccess = false,
      expectedExceptionMessage =
        Some("Cannot disable inCommitTimestamp on a catalogManaged table")),

    // ===== Required catalog table property cases: Txn allowed to not explicitly set value =====
    CatalogManagedTestCase(
      testName = "CREATE: User does not explicitly set catalog property => auto-set",
      operationType = "CREATE",
      transactionProperties = catalogManagedFeaturePropMap
    ), // <-- Missing, will be auto-set
    CatalogManagedTestCase(
      testName = "REPLACE: User does not explicitly set catalog property => auto-set",
      operationType = "REPLACE",
      initialTableProperties = catalogManagedFeaturePropMap,
      transactionProperties = Map.empty
    ), // <-- Missing, will be auto-set
    CatalogManagedTestCase(
      testName = "UPDATE: Normal updates succeed",
      operationType = "UPDATE",
      initialTableProperties = catalogManagedFeaturePropMap ++ validRequiredCatalogPropMap,
      transactionProperties = Map("zip" -> "zap")
    ), // <-- Just testing that normal updates succee

    // ===== Required catalog table property cases: User can input correct value =====
    CatalogManagedTestCase(
      testName = "CREATE: Can set required catalog property to correct value",
      operationType = "CREATE",
      transactionProperties =
        catalogManagedFeaturePropMap ++ validRequiredCatalogPropMap
    ), // <-- Set to valid
    CatalogManagedTestCase(
      testName = "REPLACE: Can set required catalog property to correct value",
      operationType = "REPLACE",
      initialTableProperties = catalogManagedFeaturePropMap,
      transactionProperties = validRequiredCatalogPropMap
    ), // <-- Set to valid
    CatalogManagedTestCase(
      testName = "UPDATE: Can set required catalog property to correct value",
      operationType = "UPDATE",
      initialTableProperties = catalogManagedFeaturePropMap,
      transactionProperties = validRequiredCatalogPropMap
    ), // <-- Set to valid

    // ===== Required catalog table property case: User cannot remove or input incorrect value =====
    CatalogManagedTestCase(
      testName = "ILLEGAL CREATE: Set required catalog property to incorrect value => THROW",
      operationType = "CREATE",
      transactionProperties =
        catalogManagedFeaturePropMap ++ invalidRequiredCatalogPropMap, // <-- Set to invalid
      expectedSuccess = false),
    CatalogManagedTestCase(
      testName = "ILLEGAL REPLACE: Set required catalog property to incorrect value => THROW",
      operationType = "REPLACE",
      initialTableProperties = catalogManagedFeaturePropMap,
      transactionProperties = invalidRequiredCatalogPropMap, // <-- Set to invalid
      expectedSuccess = false,
      expectedExceptionMessage =
        Some("Metadata is missing or has incorrect values for required catalog properties")),
    CatalogManagedTestCase(
      testName = "ILLEGAL UPDATE: Set required catalog property to incorrect value => THROW",
      operationType = "UPDATE",
      initialTableProperties = catalogManagedFeaturePropMap,
      transactionProperties = invalidRequiredCatalogPropMap, // <-- Set to invalid
      expectedSuccess = false,
      expectedExceptionMessage =
        Some("Metadata is missing or has incorrect values for required catalog properties")),
    CatalogManagedTestCase(
      testName = "ILLEGAL UPDATE: Remove required catalog property => THROW",
      operationType = "UPDATE",
      initialTableProperties = catalogManagedFeaturePropMap ++ validRequiredCatalogPropMap,
      transactionProperties = Map.empty,
      removedPropertyKeys = Set(customCatalogCommitter.REQUIRED_PROPERTY_KEY), // <-- Removed!
      expectedSuccess = false,
      expectedExceptionMessage =
        Some("Metadata is missing or has incorrect values for required catalog properties")),

    // ===== Required catalog table property case: Existing table invalid =====
    CatalogManagedTestCase(
      testName = "REPLACE: On existing table with incorrect required catalog property => sets it",
      operationType = "REPLACE",
      initialTableProperties =
        catalogManagedFeaturePropMap ++ invalidRequiredCatalogPropMap, // <-- Set to invalid
      createInitialTableCommitter = committerUsingPutIfAbsent, // allow creating the invalid table
      transactionProperties = Map.empty),
    CatalogManagedTestCase(
      testName = "UPDATE: On existing table with incorrect required catalog property => throws",
      operationType = "UPDATE",
      initialTableProperties =
        catalogManagedFeaturePropMap ++ invalidRequiredCatalogPropMap, // <-- Set to invalid
      createInitialTableCommitter = committerUsingPutIfAbsent, // allow creating the invalid table
      transactionProperties = Map.empty,
      expectedSuccess = false,
      expectedExceptionMessage =
        Some("Metadata is missing or has incorrect values for required catalog properties")))

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
            .withCommitter(testCase.createInitialTableCommitter)
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
              .withCommitter(customCatalogCommitter)

          case "UPDATE" =>
            val updateBuilder = TableManager
              .loadSnapshot(tablePath)
              .withCommitter(customCatalogCommitter)
              .withMaxCatalogVersionIfApplicable(
                isCatalogManaged = TableFeatures.isPropertiesManuallySupportingTableFeature(
                  testCase.initialTableProperties.asJava,
                  TableFeatures.CATALOG_MANAGED_RW_FEATURE),
                maxCatalogVersion = 0)
              .build(defaultEngine)
              .buildUpdateTableTransaction("engineInfo", Operation.MANUAL_UPDATE)
              .withTablePropertiesAdded(testCase.transactionProperties.asJava)

            if (testCase.removedPropertyKeys.nonEmpty) {
              updateBuilder.withTablePropertiesRemoved(testCase.removedPropertyKeys.asJava)
            } else {
              updateBuilder
            }

          case "REPLACE" =>
            val replaceSchema = schema.add("col2", IntegerType.INTEGER)

            TableManager
              .loadSnapshot(tablePath)
              .withCommitter(customCatalogCommitter)
              .withMaxCatalogVersionIfApplicable(
                isCatalogManaged = TableFeatures.isPropertiesManuallySupportingTableFeature(
                  testCase.initialTableProperties.asJava,
                  TableFeatures.CATALOG_MANAGED_RW_FEATURE),
                maxCatalogVersion = 0)
              .build(defaultEngine)
              .asInstanceOf[SnapshotImpl]
              .buildReplaceTableTransaction(replaceSchema, "engineInfo")
              .withTableProperties(testCase.transactionProperties.asJava)
        }
        // scalastyle:on

        if (testCase.expectedSuccess) {
          // Transaction building should succeed
          val result = txnBuilder.build(defaultEngine).commit(defaultEngine, emptyIterable[Row])

          val postCommitSnapshot = result
            .getPostCommitSnapshot
            .orElseThrow(() =>
              new RuntimeException("Expected post-commit snapshot when no concurrent writes"))
            .asInstanceOf[SnapshotImpl]

          // Verify the results
          val protocol = postCommitSnapshot.getProtocol

          // Check if catalogManaged feature is supported
          val catalogManagedSupported = protocol
            .supportsFeature(TableFeatures.CATALOG_MANAGED_RW_FEATURE)
          assert(catalogManagedSupported == testCase.expectedCatalogManagedSupported)

          // Check if ICT is enabled in metadata
          val ictEnabled = postCommitSnapshot.getMetadata.getConfiguration.asScala
            .get("delta.enableInCommitTimestamps")
            .contains("true")
          assert(ictEnabled == testCase.expectedIctEnabled)

          // If catalogManaged is supported, ICT feature should also be supported
          if (testCase.expectedCatalogManagedSupported) {
            assert(protocol.supportsFeature(TableFeatures.IN_COMMIT_TIMESTAMP_W_FEATURE))

            assert(
              customCatalogCommitter
                .getRequiredTableProperties
                .asScala.toSet.subsetOf(postCommitSnapshot.getTableProperties.asScala.toSet))
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
