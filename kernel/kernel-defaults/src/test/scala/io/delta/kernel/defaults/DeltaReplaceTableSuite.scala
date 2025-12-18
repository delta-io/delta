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
package io.delta.kernel.defaults

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq

import io.delta.kernel.{Operation, Table, TableManager}
import io.delta.kernel.commit.{CommitMetadata, CommitResponse, Committer}
import io.delta.kernel.data.Row
import io.delta.kernel.defaults.utils.{TestRow, WriteUtils, WriteUtilsWithV2Builders}
import io.delta.kernel.engine.Engine
import io.delta.kernel.exceptions.{KernelException, MaxCommitRetryLimitReachedException, TableNotFoundException}
import io.delta.kernel.expressions.{Column, Literal}
import io.delta.kernel.internal.{SnapshotImpl, TableConfig, TableImpl}
import io.delta.kernel.internal.TableConfig._
import io.delta.kernel.internal.tablefeatures.TableFeatures
import io.delta.kernel.types.{IntegerType, StringType, StructType}
import io.delta.kernel.utils.CloseableIterable.emptyIterable
import io.delta.kernel.utils.CloseableIterator

class DeltaReplaceTableTransactionBuilderV1Suite extends DeltaReplaceTableSuite with WriteUtils

class DeltaReplaceTableTransactionBuilderV2Suite extends DeltaReplaceTableSuite
    with WriteUtilsWithV2Builders {

  test("ReplaceTableTransactionBuilder uses the committer provided during snapshot building") {
    withTempDirAndEngine { (tablePath, engine) =>
      class FakeCommitter extends Committer {
        override def commit(
            engine: Engine,
            finalizedActions: CloseableIterator[Row],
            commitMetadata: CommitMetadata): CommitResponse = {
          throw new RuntimeException("This is a fake committer")
        }
      }
      createEmptyTable(
        engine,
        tablePath,
        testSchema)

      // Build snapshot with committer and start txn
      val txn = TableManager.loadSnapshot(tablePath)
        .withCommitter(new FakeCommitter())
        .build(engine).asInstanceOf[SnapshotImpl]
        .buildReplaceTableTransaction(testSchema, "test-engine")
        .build(engine)

      // Check the txn returns the correct committer
      assert(txn.getCommitter.isInstanceOf[FakeCommitter])
      // Check that the txn invokes the provided committer upon commit
      val e = intercept[RuntimeException] {
        txn.commit(engine, emptyIterable())
      }
      assert(e.getMessage.contains("This is a fake committer"))
    }
  }
}

abstract class DeltaReplaceTableSuite extends DeltaReplaceTableSuiteBase {

  /* ----- ERROR CASES ------ */

  test("Conflict resolution is disabled for replace table") {
    withTempDirAndEngine { (tablePath, engine) =>
      createInitialTable(engine, tablePath)
      // Start replace transaction - use default maxRetries
      val txn1 = getReplaceTxn(engine, tablePath, testSchema)
      // Start replace transaction - explicitly set maxRetries > 0
      val txn2 = getReplaceTxn(engine, tablePath, testSchema, maxRetries = 100)
      // Commit a simple blind append as a conflicting txn
      appendData(
        engine,
        tablePath,
        data = Seq(Map.empty[String, Literal] -> (dataBatches2)))
      // Try to commit replace table and intercept conflicting txn (no conflict resolution)
      intercept[MaxCommitRetryLimitReachedException] {
        commitTransaction(txn1, engine, emptyIterable())
      }
      intercept[MaxCommitRetryLimitReachedException] {
        commitTransaction(txn2, engine, emptyIterable())
      }
    }
  }

  test("Table::createTransactionBuilder does not allow REPLACE TABLE") {
    withTempDirAndEngine { (tablePath, engine) =>
      createInitialTable(engine, tablePath)
      assert(intercept[UnsupportedOperationException] {
        Table.forPath(engine, tablePath)
          .createTransactionBuilder(engine, testEngineInfo, Operation.REPLACE_TABLE)
          .build(engine)
      }.getMessage.contains("REPLACE TABLE is not yet supported"))
    }
  }

  test("Cannot replace a table that does not exist") {
    withTempDirAndEngine { (tablePath, engine) =>
      assert(
        intercept[TableNotFoundException] {
          // This is not possible on an API level for V2 builders since building is from a Snapshot
          Table.forPath(engine, tablePath).asInstanceOf[TableImpl]
            .createReplaceTableTransactionBuilder(engine, "test-engine")
            .withSchema(engine, testSchema)
            .build(engine)
        }.getMessage.contains("Trying to replace a table that does not exist"))
    }
  }

  test("Cannot enable a feature that Kernel does not support") {
    withTempDirAndEngine { (tablePath, engine) =>
      createInitialTable(engine, tablePath)
      assert(
        intercept[KernelException] {
          commitReplaceTable(
            engine,
            tablePath,
            tableProperties = Map(TableConfig.CHANGE_DATA_FEED_ENABLED.getKey -> "true"))
        }.getMessage.contains("Unsupported Delta writer feature"))
    }
  }

  test("Cannot replace a table with a protocol Kernel does not support") {
    withTempDirAndEngine { (tablePath, engine) =>
      spark.sql(
        s"""
          |CREATE TABLE delta.`$tablePath` (id INT) USING DELTA
          |TBLPROPERTIES('delta.enableChangeDataFeed' = 'true')
          |""".stripMargin)
      assert(
        intercept[KernelException] {
          commitReplaceTable(
            engine,
            tablePath)
        }.getMessage.contains("Unsupported Delta writer feature"))
    }
  }

  test("Must provide a schema for replace table transaction") {
    withTempDirAndEngine { (tablePath, engine) =>
      createInitialTable(engine, tablePath)
      assert(intercept[KernelException] {
        Table.forPath(engine, tablePath).asInstanceOf[TableImpl]
          .createReplaceTableTransactionBuilder(engine, "test-engine")
          .build(engine)
      }.getMessage.contains("Must provide a new schema for REPLACE TABLE"))
    }
  }

  test("Cannot define both partition and clustering columns at the same time") {
    withTempDirAndEngine { (tablePath, engine) =>
      createInitialTable(engine, tablePath)
      assert(intercept[IllegalArgumentException] {
        // Setting both is not possible on an API level for v2 builders
        Table.forPath(engine, tablePath).asInstanceOf[TableImpl]
          .createReplaceTableTransactionBuilder(engine, "test-engine")
          .withSchema(engine, testPartitionSchema)
          .withPartitionColumns(engine, testPartitionColumns.asJava)
          .withClusteringColumns(engine, testClusteringColumns.asJava)
          .build(engine)
      }.getMessage.contains(
        "Partition Columns and Clustering Columns cannot be set at the same time"))
    }
  }

  test("Schema provided must be valid") {
    withTempDirAndEngine { (tablePath, engine) =>
      createInitialTable(engine, tablePath)
      assert(intercept[KernelException] {
        getReplaceTxn(
          engine,
          tablePath,
          schema = new StructType().add("col", IntegerType.INTEGER).add("col", IntegerType.INTEGER))
      }.getMessage.contains(
        "Schema contains duplicate columns"))
    }
  }

  test("Partition columns provided must be valid") {
    withTempDirAndEngine { (tablePath, engine) =>
      createInitialTable(engine, tablePath)
      assert(intercept[IllegalArgumentException] {
        getReplaceTxn(
          engine,
          tablePath,
          schema = testSchema,
          partCols = Seq("foo"))
      }.getMessage.contains(
        "Partition column foo not found in the schema"))
    }
  }

  test("Clustering columns provided must be valid") {
    withTempDirAndEngine { (tablePath, engine) =>
      createInitialTable(engine, tablePath)
      assert(intercept[KernelException] {
        getReplaceTxn(
          engine,
          tablePath,
          schema = testSchema,
          clusteringColsOpt = Some(Seq(new Column("foo"))))
      }.getMessage.contains(
        "Column 'column(`foo`)' was not found in the table schema"))
    }
  }

  test("icebergWriterCompatV1 checks are enforced") {
    withTempDirAndEngine { (tablePath, engine) =>
      createInitialTable(engine, tablePath)
      assert(
        intercept[KernelException] {
          commitReplaceTable(
            engine,
            tablePath,
            tableProperties = Map(
              TableConfig.ICEBERG_WRITER_COMPAT_V1_ENABLED.getKey -> "true",
              TableConfig.COLUMN_MAPPING_MODE.getKey -> "name"))
        }.getMessage.contains("The value 'name' for the property 'delta.columnMapping.mode' is " +
          "not compatible with icebergWriterCompatV1 requirements"))
    }
  }

  test("icebergCompatV2 checks are enforced") {
    withTempDirAndEngine { (tablePath, engine) =>
      createInitialTable(engine, tablePath)
      assert(
        intercept[KernelException] {
          commitReplaceTable(
            engine,
            tablePath,
            tableProperties = Map(
              TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true",
              TableConfig.DELETION_VECTORS_CREATION_ENABLED.getKey -> "true"))
        }.getMessage.contains(
          "Table features [deletionVectors] are incompatible with icebergCompatV2"))
    }
  }

  test("REPLACE is not supported on existing table with icebergCompatV3 feature") {
    withTempDirAndEngine { (tablePath, engine) =>
      createInitialTable(
        engine,
        tablePath,
        tableProperties = Map(TableConfig.ICEBERG_COMPAT_V3_ENABLED.getKey -> "true"),
        includeData = false // To avoid writing data with correct CM schema
      )
      assert(
        intercept[UnsupportedOperationException] {
          commitReplaceTable(
            engine,
            tablePath,
            tableProperties = Map(TableConfig.ICEBERG_COMPAT_V3_ENABLED.getKey -> "true"))
        }.getMessage.contains("REPLACE TABLE is not yet supported on IcebergCompatV3 tables"))
    }
  }

  test("REPLACE is not supported when enabling icebergCompatV3 feature") {
    withTempDirAndEngine { (tablePath, engine) =>
      createInitialTable(
        engine,
        tablePath,
        tableProperties = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "name"),
        includeData = false // To avoid writing data with correct CM schema
      )
      assert(
        intercept[UnsupportedOperationException] {
          commitReplaceTable(
            engine,
            tablePath,
            tableProperties = Map(
              TableConfig.ICEBERG_COMPAT_V3_ENABLED.getKey -> "true",
              TableConfig.COLUMN_MAPPING_MODE.getKey -> "name"))
        }.getMessage.contains("REPLACE TABLE is not yet supported on IcebergCompatV3 tables"))
    }
  }

  /* ----------------- POSITIVE CASES ----------------- */

  // TODO can we refactor other suites to run with both create + replace?

  Seq(Seq(), Seq(Map.empty[String, Literal] -> (dataBatches1))).foreach { replaceData =>
    test(s"Basic case with no metadata changes, insertData=${replaceData.nonEmpty}") {
      withTempDirAndEngine { (tablePath, engine) =>
        createInitialTable(engine, tablePath)
        checkReplaceTable(engine, tablePath, data = replaceData)
      }
    }

    test(s"Basic case with initial empty table, insertData=${replaceData.nonEmpty}") {
      withTempDirAndEngine { (tablePath, engine) =>
        createInitialTable(engine, tablePath)
        checkReplaceTable(engine, tablePath, data = replaceData)
      }
    }
  }

  // Note, these tests cover things like transitioning between unpartitioned, partitioned, and
  // clustered tables. This means it includes removing existing clustering domains when the initial
  // table was clustered.
  validSchemaDefs.foreach { case (initialSchemaDef, initialData) =>
    validSchemaDefs.foreach { case (replaceSchemaDef, replaceData) =>
      Seq(true, false).foreach { initialTableEmpty =>
        Seq(true, false).foreach { insertDataInReplace =>
          test(s"Schema change from $initialSchemaDef to $replaceSchemaDef; " +
            s"initialTableEmpty=$initialTableEmpty, insertDataInReplace=$insertDataInReplace") {
            withTempDirAndEngine { (tablePath, engine) =>
              createInitialTable(
                engine,
                tablePath,
                schema = initialSchemaDef.schema,
                partitionColumns = initialSchemaDef.partitionColumns,
                clusteringColumns = initialSchemaDef.clusteringColumns,
                includeData = !initialTableEmpty,
                data = initialData)
              checkReplaceTable(
                engine,
                tablePath,
                schema = replaceSchemaDef.schema,
                partitionColumns = replaceSchemaDef.partitionColumns,
                clusteringColumns = replaceSchemaDef.clusteringColumns,
                data = if (insertDataInReplace) replaceData else Seq.empty)
            }
          }

        }

      }

      test(s"Schema change from $initialSchemaDef to $replaceSchemaDef") {
        withTempDirAndEngine { (tablePath, engine) =>
          createInitialTable(
            engine,
            tablePath,
            schema = initialSchemaDef.schema,
            partitionColumns = initialSchemaDef.partitionColumns,
            clusteringColumns = initialSchemaDef.clusteringColumns,
            includeData = false)
          checkReplaceTable(
            engine,
            tablePath,
            schema = replaceSchemaDef.schema,
            partitionColumns = replaceSchemaDef.partitionColumns,
            clusteringColumns = replaceSchemaDef.clusteringColumns)
        }
      }
    }
  }

  test("Case with DVs in the initial table") {
    withTempDirAndEngine { (tablePath, engine) =>
      spark.sql(
        s"""
           |CREATE TABLE delta.`$tablePath` (id INT) USING DELTA
           |TBLPROPERTIES('delta.enableDeletionVectors' = 'true')
           |""".stripMargin)
      spark.sql(
        s"""
           |INSERT INTO delta.`$tablePath` VALUES (0), (1), (2), (3)
           |""".stripMargin)
      spark.sql(
        s"""
           |DELETE FROM delta.`$tablePath` WHERE id > 1
           |""".stripMargin)
      checkTable(tablePath, Seq(TestRow(0), TestRow(1)))
      checkReplaceTable(engine, tablePath) // check it is empty after (also DVs no longer enabled)
    }
  }

  test("Existing table properties are removed") {
    withTempDirAndEngine { (tablePath, engine) =>
      createInitialTable(
        engine,
        tablePath,
        tableProperties = Map(
          TableConfig.APPEND_ONLY_ENABLED.getKey -> "true",
          "user.facing.prop" -> "existing_prop"))
      checkReplaceTable(engine, tablePath)
    }
  }

  test("New table features are correctly enabled") {
    // This also validates that withDomainMetadataSupported works
    withTempDirAndEngine { (tablePath, engine) =>
      createInitialTable(engine, tablePath)
      checkReplaceTable(
        engine,
        tablePath,
        tableProperties = Map(
          TableConfig.DELETION_VECTORS_CREATION_ENABLED.getKey -> "true"),
        domainsToAdd = Seq(("domain-name", "some-config")),
        expectedTableFeaturesSupported =
          Seq(TableFeatures.DELETION_VECTORS_RW_FEATURE, TableFeatures.DOMAIN_METADATA_W_FEATURE))
    }
  }

  test("Domain metadata are reset (user-facing)") {
    // (1) checks that we correctly override an existing domain with the new config if set in the
    //     replace txn
    // (2) checks we remove stale ones that are not set in the replace txn
    withTempDirAndEngine { (tablePath, engine) =>
      // Create initial table with 2 domains
      val txn = getCreateTxn(engine, tablePath, testSchema, withDomainMetadataSupported = true)
      txn.addDomainMetadata("domainToOverride", "check1")
      txn.addDomainMetadata("domainToRemove", "check2")
      commitTransaction(txn, engine, emptyIterable())

      // Validate the 2 domains are present
      val snapshot = Table.forPath(engine, tablePath).getLatestSnapshot(engine)
      assert(snapshot.getDomainMetadata("domainToOverride").toScala.contains("check1"))
      assert(snapshot.getDomainMetadata("domainToRemove").toScala.contains("check2"))

      // Replace table and override 1/2 of the domains
      checkReplaceTable(
        engine,
        tablePath,
        domainsToAdd = Seq(("domainToOverride", "overridden-config")))
    }
  }

  test("Column mapping maxFieldId is preserved during REPLACE TABLE " +
    "- turning off column mapping mode") {
    // Note: DeltaReplaceTableColumnMappingSuite already tests that we preserve it correctly for the
    // column mapping case
    // TODO: once we support Id -> None mode during replace update this test
    // We should preserve maxFieldId regardless of column mapping mode (if a future replace
    // operation re-enables id mode we should not start our fieldIds from 0)
    withTempDirAndEngine { (tablePath, engine) =>
      createInitialTable(
        engine,
        tablePath,
        tableProperties = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> "id"),
        includeData = false // To avoid writing data with correct CM schema
      )
      intercept[UnsupportedOperationException] {
        checkReplaceTable(
          engine,
          tablePath,
          expectedTableProperties =
            Some(Map(TableConfig.COLUMN_MAPPING_MAX_COLUMN_ID.getKey -> "1")))
      }
    }
  }

  test("icebergCompatV2 checks are executed and properties updated/auto-enabled") {
    withTempDirAndEngine { (tablePath, engine) =>
      createInitialTable(engine, tablePath)
      // TODO once we support column mapping update this test
      intercept[UnsupportedOperationException] {
        checkReplaceTable(
          engine,
          tablePath,
          tableProperties = Map(TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true"),
          expectedTableProperties = Some(Map(
            TableConfig.COLUMN_MAPPING_MODE.getKey -> "name",
            TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true")),
          expectedTableFeaturesSupported = Seq(
            TableFeatures.ICEBERG_COMPAT_V2_W_FEATURE,
            TableFeatures.COLUMN_MAPPING_RW_FEATURE))
      }
    }
  }

  // TODO - can we reuse the tests in IcebergWriterCompatV1Suite to run with both create table and
  //  replace table?
  test("icebergWriterCompatV1 checks are executed and properties updated/auto-enabled") {
    // This also validates you can enable icebergWriterCompatV1 on an existing table during replace
    withTempDirAndEngine { (tablePath, engine) =>
      createInitialTable(engine, tablePath)
      // TODO once we support column mapping update this test
      intercept[UnsupportedOperationException] {
        checkReplaceTable(
          engine,
          tablePath,
          tableProperties = Map(TableConfig.ICEBERG_WRITER_COMPAT_V1_ENABLED.getKey -> "true"),
          expectedTableProperties = Some(Map(
            TableConfig.ICEBERG_WRITER_COMPAT_V1_ENABLED.getKey -> "true",
            TableConfig.COLUMN_MAPPING_MODE.getKey -> "id",
            TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true")),
          expectedTableFeaturesSupported = Seq(
            TableFeatures.ICEBERG_COMPAT_V2_W_FEATURE,
            TableFeatures.ICEBERG_WRITER_COMPAT_V1,
            TableFeatures.COLUMN_MAPPING_RW_FEATURE))
      }
    }
  }

  test("When cmMode=None it is possible to have column with same name different type") {
    withTempDirAndEngine { (tablePath, engine) =>
      createInitialTable(
        engine,
        tablePath,
        schema = new StructType()
          .add("col1", StringType.STRING),
        includeData = false)
      checkReplaceTable(
        engine,
        tablePath,
        schema = new StructType()
          .add("col1", IntegerType.INTEGER))
    }
  }

  test("REPLACE TABLE preserves ICT enablement tracking properties") {
    withTempDirAndEngine { (tablePath, engine) =>
      val snapshotV1 = createTableThenEnableIctAndVerify(engine, tablePath)
      val ictEnablementTimestamp = snapshotV1.getTimestamp(engine)

      checkReplaceTable(
        engine,
        tablePath,
        expectedTableProperties = Some(Map(
          IN_COMMIT_TIMESTAMPS_ENABLED.getKey -> "true",
          IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP.getKey -> ictEnablementTimestamp.toString,
          IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION.getKey -> "1")))
    }
  }

  test("REPLACE TABLE removes ICT enablement tracking properties when explicitly disabling ICT") {
    withTempDirAndEngine { (tablePath, engine) =>
      createTableThenEnableIctAndVerify(engine, tablePath)

      checkReplaceTable(
        engine,
        tablePath,
        tableProperties = Map(IN_COMMIT_TIMESTAMPS_ENABLED.getKey -> "false"),
        expectedTableProperties = Some(Map(
          IN_COMMIT_TIMESTAMPS_ENABLED.getKey -> "false")))
    }
  }

  test("REPLACE TABLE can enable ICT") {
    withTempDirAndEngine { (tablePath, engine) =>
      createEmptyTable(engine, tablePath, testSchema)

      checkReplaceTable(
        engine,
        tablePath,
        tableProperties = Map(IN_COMMIT_TIMESTAMPS_ENABLED.getKey -> "true"),
        expectedTableProperties = Some(Map(
          IN_COMMIT_TIMESTAMPS_ENABLED.getKey -> "true",
          IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP.getKey -> "__check_exists__",
          IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION.getKey -> "1")))
    }
  }
}
