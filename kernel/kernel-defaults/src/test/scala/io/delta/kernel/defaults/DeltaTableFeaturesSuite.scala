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

import java.util.Collections

import scala.collection.immutable.Seq
import scala.jdk.CollectionConverters._

import io.delta.kernel.{Operation, Table}
import io.delta.kernel.Operation.CREATE_TABLE
import io.delta.kernel.defaults.utils.{AbstractWriteUtils, WriteUtils, WriteUtilsWithV2Builders}
import io.delta.kernel.engine.Engine
import io.delta.kernel.exceptions.{InvalidConfigurationValueException, KernelException}
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.{SnapshotImpl, TableConfig}
import io.delta.kernel.internal.TableConfig.UniversalFormats
import io.delta.kernel.internal.actions.{Protocol => KernelProtocol}
import io.delta.kernel.internal.tablefeatures.TableFeatures
import io.delta.kernel.types.{StructType, TimestampNTZType}
import io.delta.kernel.types.IntegerType.INTEGER
import io.delta.kernel.utils.CloseableIterable.emptyIterable

import org.apache.spark.sql.delta.{DeltaLog, DeltaTableFeatureException}
import org.apache.spark.sql.delta.actions.Protocol

import org.scalatest.funsuite.AnyFunSuite

class DeltaTableFeaturesTransactionBuilderV1Suite extends DeltaTableFeaturesSuiteBase
    with WriteUtils {}

class DeltaTableFeaturesTransactionBuilderV2Suite extends DeltaTableFeaturesSuiteBase
    with WriteUtilsWithV2Builders {}

/**
 * Integration test suite for Delta table features.
 */
trait DeltaTableFeaturesSuiteBase extends AnyFunSuite with AbstractWriteUtils {

  ///////////////////////////////////////////////////////////////////////////
  // Tests for deletionVector, v2Checkpoint table features
  ///////////////////////////////////////////////////////////////////////////
  Seq(
    // Test format: feature (readerWriter type), table property to enable the feature
    // For each feature, we test the following scenarios:
    // 1. able to write to an existing Delta table with the feature supported
    // 2. create a table with the feature supported and append data
    // 3. update an existing table with the feature supported
    ("deletionVectors", "delta.enableDeletionVectors", "true"),
    ("v2Checkpoint", "delta.checkpointPolicy", "v2")).foreach {
    case (feature, tblProp, propValue) =>
      test(s"able to write to an existing Delta table with $feature supported") {
        withTempDirAndEngine { (tablePath, engine) =>
          // Create a table with the feature supported
          spark.sql(s"CREATE TABLE delta.`$tablePath` (id INTEGER) USING delta " +
            s"TBLPROPERTIES ('$tblProp' = '$propValue')")

          checkReaderWriterFeaturesSupported(tablePath, feature)

          // Write data to the table using Kernel
          val testData = Seq(Map.empty[String, Literal] -> dataBatches1)
          appendData(
            engine,
            tablePath,
            data = testData)

          // Check the data using Kernel and Delta-Spark readers
          verifyWrittenContent(tablePath, testSchema, dataBatches1.flatMap(_.toTestRows))
        }
      }

      test(s"create a table with $feature supported") {
        withTempDirAndEngine { (tablePath, engine) =>
          val testData = Seq(Map.empty[String, Literal] -> dataBatches1)

          // create a table with the feature supported and append testData
          appendData(
            engine,
            tablePath,
            isNewTable = true,
            testSchema,
            data = testData,
            tableProperties = Map(tblProp -> propValue))

          checkReaderWriterFeaturesSupported(tablePath, feature)

          // insert more data
          appendData(
            engine,
            tablePath,
            data = testData)

          // Check the data using Kernel and Delta-Spark readers
          verifyWrittenContent(
            tablePath,
            testSchema,
            dataBatches1.flatMap(_.toTestRows) ++ dataBatches1.flatMap(_.toTestRows))
        }
      }

      test(s"update an existing table with $feature support") {
        withTempDirAndEngine { (tablePath, engine) =>
          val testData = Seq(Map.empty[String, Literal] -> dataBatches1)

          // create a table without the table feature supported
          appendData(
            engine,
            tablePath,
            isNewTable = true,
            testSchema,
            data = testData)

          checkNoReaderWriterFeaturesSupported(tablePath, feature)

          // insert more data and enable the feature
          appendData(
            engine,
            tablePath,
            data = testData,
            tableProperties = Map(tblProp -> propValue))

          checkReaderWriterFeaturesSupported(tablePath, feature)

          // Check the data using Kernel and Delta-Spark readers
          verifyWrittenContent(
            tablePath,
            testSchema,
            dataBatches1.flatMap(_.toTestRows) ++ dataBatches1.flatMap(_.toTestRows))
        }
      }
  }

  // Test format: isTimestampNtzEnabled, expected protocol.
  Seq(
    (true, new KernelProtocol(3, 7, Set("timestampNtz").asJava, Set("timestampNtz").asJava)),
    (false, new KernelProtocol(1, 2, Collections.emptySet(), Collections.emptySet())))
    .foreach({
      case (isTimestampNtzEnabled, expectedProtocol) =>
        test(s"Create table with timestampNtz enabled: $isTimestampNtzEnabled") {
          withTempDirAndEngine { (tablePath, engine) =>
            val schema = if (isTimestampNtzEnabled) {
              new StructType().add("tz", TimestampNTZType.TIMESTAMP_NTZ)
            } else {
              new StructType().add("id", INTEGER)
            }
            val txn = getCreateTxn(engine, tablePath, schema)

            assert(txn.getSchema(engine) === schema)
            assert(txn.getPartitionColumns(engine).isEmpty)
            val txnResult = commitTransaction(txn, engine, emptyIterable())

            assert(txnResult.getVersion === 0)
            val protocolRow = getProtocolActionFromCommit(engine, tablePath, 0)
            assert(protocolRow.isDefined)
            val protocol = KernelProtocol.fromRow(protocolRow.get)
            assert(protocol.getMinReaderVersion === expectedProtocol.getMinReaderVersion)
            assert(protocol.getMinWriterVersion === expectedProtocol.getMinWriterVersion)
            assert(protocol.getReaderFeatures.containsAll(expectedProtocol.getReaderFeatures))
            assert(protocol.getWriterFeatures.containsAll(expectedProtocol.getWriterFeatures))
          }
        }
    })

  test("schema evolution from Spark to add TIMESTAMP_NTZ type on a table created with kernel") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val txn = getCreateTxn(engine, tablePath, testSchema)
      val txnResult = commitTransaction(txn, engine, emptyIterable())

      assert(txnResult.getVersion === 0)
      assertThrows[DeltaTableFeatureException] {
        spark.sql("ALTER TABLE delta.`" + tablePath + "` ADD COLUMN newCol TIMESTAMP_NTZ")
      }
      spark.sql("ALTER TABLE delta.`" + tablePath +
        "` SET TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported')")
      spark.sql("ALTER TABLE delta.`" + tablePath + "` ADD COLUMN newCol TIMESTAMP_NTZ")
    }
  }

  test("feature can be enabled via delta.feature prefix") {
    withTempDirAndEngine { (tablePath, engine) =>
      val domainMetadataKey = (
        TableFeatures.SET_TABLE_FEATURE_SUPPORTED_PREFIX
          + TableFeatures.DOMAIN_METADATA_W_FEATURE.featureName)
      val properties = Map(
        "delta.feature.vacuumProtocolCheck" -> "supported",
        domainMetadataKey -> "supported")

      createEmptyTable(engine, tablePath, testSchema, tableProperties = properties)

      val table = Table.forPath(engine, tablePath)
      val writtenSnapshot = latestSnapshot(table, engine)
      assert(writtenSnapshot.getMetadata.getConfiguration.isEmpty)
      assert(writtenSnapshot.getProtocol.getExplicitlySupportedFeatures.containsAll(Set(
        TableFeatures.VACUUM_PROTOCOL_CHECK_RW_FEATURE,
        TableFeatures.DOMAIN_METADATA_W_FEATURE).asJava))
    }
  }

  test("withDomainMetadata adds corresponding feature option") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val txn = getCreateTxn(engine, tablePath, testSchema, withDomainMetadataSupported = true)
      commitTransaction(txn, engine, emptyIterable())
      assert(latestSnapshot(table, engine).getProtocol.getExplicitlySupportedFeatures.contains(
        TableFeatures.DOMAIN_METADATA_W_FEATURE))
    }
  }

  test("delta.feature prefixed keys are removed even if property is already present on protocol") {
    withTempDirAndEngine { (tablePath, engine) =>
      val properties = Map("delta.feature.vacuumProtocolCheck" -> "supported")
      createEmptyTable(engine, tablePath, testSchema, tableProperties = properties)
      val table = Table.forPath(engine, tablePath)
      assert(latestSnapshot(table, engine).getMetadata.getConfiguration.isEmpty)

      // Update table with the same feature override set.
      val updateTxn = getUpdateTxn(engine, tablePath, tableProperties = properties)

      commitTransaction(updateTxn, engine, emptyIterable())

      assert(latestSnapshot(table, engine).getMetadata.getConfiguration.isEmpty)
    }
  }

  test("delta.feature override populate dependent features") {
    withTempDirAndEngine { (tablePath, engine) =>
      val properties = Map("delta.feature.clustering" -> "supported")

      createEmptyTable(engine, tablePath, testSchema, tableProperties = properties)

      val table = Table.forPath(engine, tablePath)
      val writtenSnapshot = latestSnapshot(table, engine)
      assert(
        writtenSnapshot.getProtocol.getExplicitlySupportedFeatures.containsAll(Set(
          TableFeatures.CLUSTERING_W_FEATURE,
          TableFeatures.DOMAIN_METADATA_W_FEATURE).asJava),
        s"${writtenSnapshot.getProtocol.getExplicitlySupportedFeatures}")
    }
  }

  test("delta.feature override and TableConfig populate necessary features") {
    withTempDirAndEngine { (tablePath, engine) =>
      val properties =
        Map("delta.feature.clustering" -> "supported", "delta.enableDeletionVectors" -> "true")

      createEmptyTable(engine, tablePath, testSchema, tableProperties = properties)

      val table = Table.forPath(engine, tablePath)
      val writtenSnapshot = latestSnapshot(table, engine)
      assert(
        writtenSnapshot.getProtocol.getExplicitlySupportedFeatures.containsAll(Set(
          TableFeatures.CLUSTERING_W_FEATURE,
          TableFeatures.DOMAIN_METADATA_W_FEATURE,
          TableFeatures.DELETION_VECTORS_RW_FEATURE).asJava),
        s"${writtenSnapshot.getProtocol.getExplicitlySupportedFeatures}")
      assert(writtenSnapshot.getMetadata.getConfiguration == Map(
        "delta.enableDeletionVectors" -> "true").asJava)
    }
  }

  test("UNIVERSAL_FORMAT feature can be populated") {
    withTempDirAndEngine { (tablePath, engine) =>
      val properties =
        Map(
          TableConfig.UNIVERSAL_FORMAT_ENABLED_FORMATS.getKey -> "iceberg",
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true")
      createEmptyTable(engine, tablePath, testSchema, tableProperties = properties)

      val table = Table.forPath(engine, tablePath)
      val writtenSnapshot = latestSnapshot(table, engine)
      assert(TableConfig.UNIVERSAL_FORMAT_ENABLED_FORMATS.fromMetadata(
        writtenSnapshot.getMetadata).contains(UniversalFormats.FORMAT_ICEBERG))
    }
  }

  test("UNIVERSAL_FORMAT feature will throw if icebergCompatV2 was not enabled") {
    withTempDirAndEngine { (tablePath, engine) =>
      createEmptyTable(engine, tablePath, testSchema)

      intercept[InvalidConfigurationValueException] {
        getUpdateTxn(
          engine,
          tablePath,
          tableProperties = Map(TableConfig.UNIVERSAL_FORMAT_ENABLED_FORMATS.getKey -> "iceberg"))
      }
    }
  }

  test("read throws if the table contains unsupported table feature") {
    withTempDirAndEngine { (tablePath, engine) =>
      createEmptyTable(engine, tablePath, testSchema)
      appendData(
        engine,
        tablePath,
        isNewTable = false,
        data = Seq(Map.empty[String, Literal] -> dataBatches1))

      checkTable(tablePath, expectedAnswer = dataBatches1.flatMap(_.toTestRows))

      // If test is running in intelliJ, set DELTA_TESTING=1 in env variables.
      // This will enable the testReaderWriter feature in delta-spark. In CI jobs,
      // build.sbt already has set and effective.
      spark.sql("ALTER TABLE delta.`" + tablePath +
        "` SET TBLPROPERTIES ('delta.feature.testReaderWriter' = 'supported')")

      // try to read the table
      val ex = intercept[KernelException] {
        checkTable(
          tablePath,
          expectedAnswer = Seq.empty /* it doesn't matter as expect failure in reading */ )
      }
      assert(ex.getMessage.contains(
        "feature \"testReaderWriter\" which is unsupported by this version of Delta Kernel"))
    }
  }

  test("read succeeds with unrecognized writer-only feature") {
    withTempDirAndEngine { (tablePath, engine) =>
      createEmptyTable(engine, tablePath, testSchema)
      // Add an unknown writer feature to the protocol
      // When DELTA_TESTING=1 (set in build.sbt) this test writer feature is allowed
      spark.sql(
        "ALTER TABLE delta.`" + tablePath +
          "` SET TBLPROPERTIES ('delta.feature.testWriter' = 'supported')")

      // Read should succeed - writer-only features don't affect readers
      getTableManagerAdapter.getSnapshotAtLatest(engine, tablePath)
      assert(getProtocol(engine, tablePath).getWriterFeatures().contains("testWriter"))
    }
  }

  /* ---- Start: type widening tests ---- */
  test("only typeWidening feature is enabled when metadata supports it: new table") {
    withTempDirAndEngine { (tablePath, engine) =>
      createEmptyTable(
        engine,
        tablePath = tablePath,
        schema = testSchema,
        tableProperties = Map("delta.enableTypeWidening" -> "true"))

      val protocolV0 = getProtocol(engine, tablePath)
      assert(!protocolV0.supportsFeature(TableFeatures.TYPE_WIDENING_RW_PREVIEW_FEATURE))
      assert(protocolV0.supportsFeature(TableFeatures.TYPE_WIDENING_RW_FEATURE))

      // try enabling type widening again and expect no change in protocol
      updateTableMetadata(
        engine = engine,
        tablePath = tablePath,
        tableProperties = Map("delta.enableTypeWidening" -> "true"))
      val protocolV1 = getProtocol(engine, tablePath)
      assert(protocolV1 === protocolV0)
    }
  }

  test("only typeWidening feature is enabled when new metadata supports it: existing table") {
    withTempDirAndEngine { (tablePath, engine) =>
      createEmptyTable(engine, tablePath = tablePath, schema = testSchema)
      val protocolV0 = getProtocol(engine, tablePath)
      assert(!protocolV0.supportsFeature(TableFeatures.TYPE_WIDENING_RW_PREVIEW_FEATURE))
      assert(!protocolV0.supportsFeature(TableFeatures.TYPE_WIDENING_RW_FEATURE))

      // try enabling type widening  and expect change in protocol
      updateTableMetadata(
        engine = engine,
        tablePath = tablePath,
        tableProperties = Map("delta.enableTypeWidening" -> "true"))
      val protocolV1 = getProtocol(engine, tablePath)
      assert(!protocolV1.supportsFeature(TableFeatures.TYPE_WIDENING_RW_PREVIEW_FEATURE))
      assert(protocolV1.supportsFeature(TableFeatures.TYPE_WIDENING_RW_FEATURE))
    }
  }

  test("typeWidening-preview in existing table is respected") {
    withTempDirAndEngine { (tablePath, engine) =>
      spark.sql(s"CREATE TABLE delta.`$tablePath`(id INT) USING delta " +
        s"TBLPROPERTIES ('delta.feature.typeWidening-preview' = 'supported')")

      val protocolV0 = getProtocol(engine, tablePath)
      require(protocolV0.supportsFeature(TableFeatures.TYPE_WIDENING_RW_PREVIEW_FEATURE))
      require(!protocolV0.supportsFeature(TableFeatures.TYPE_WIDENING_RW_FEATURE))

      // now through Kernel type enabling the type widening through table property
      updateTableMetadata(
        engine = engine,
        tablePath = tablePath,
        tableProperties = Map("delta.enableTypeWidening" -> "true"))
      val protocolV1 = getProtocol(engine, tablePath)
      assert(protocolV1.supportsFeature(TableFeatures.TYPE_WIDENING_RW_PREVIEW_FEATURE))
      assert(!protocolV1.supportsFeature(TableFeatures.TYPE_WIDENING_RW_FEATURE))
    }
  }
  /* ---- End: type widening tests ---- */

  ///////////////////////////////////////////////////////////////////////////
  // Helper methods
  ///////////////////////////////////////////////////////////////////////////
  def latestSnapshot(table: Table, engine: Engine): SnapshotImpl = {
    table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
  }

  def checkWriterFeaturesSupported(
      tblPath: String,
      expWriterOnlyFeatures: String*): Unit = {
    val protocol = getLatestProtocol(tblPath)
    val missingFeatures =
      expWriterOnlyFeatures.toSet -- protocol.writerFeatures.getOrElse(Set.empty)

    assert(
      missingFeatures.isEmpty,
      s"The following expected writer features are not supported: " +
        s"${missingFeatures.mkString(", ")}")
  }

  def checkNoWriterFeaturesSupported(tblPath: String, notExpWriterOnlyFeatures: String*): Unit = {
    val protocol = getLatestProtocol(tblPath)
    assert(protocol.writerFeatures.getOrElse(Set.empty)
      .intersect(notExpWriterOnlyFeatures.toSet).isEmpty)
  }

  def checkReaderWriterFeaturesSupported(
      tblPath: String,
      expectedReaderWriterFeatures: String*): Unit = {

    val protocol = getLatestProtocol(tblPath)

    val missingInWriterSet =
      expectedReaderWriterFeatures.toSet -- protocol.writerFeatures.getOrElse(Set.empty)
    assert(
      missingInWriterSet.isEmpty,
      s"The following expected readerWriter features are not supported in writerFeatures set: " +
        s"${missingInWriterSet.mkString(", ")}")

    val missingInReaderSet =
      expectedReaderWriterFeatures.toSet -- protocol.readerFeatures.getOrElse(Set.empty)
    assert(
      missingInReaderSet.isEmpty,
      s"The following expected readerWriter features are not supported in readerFeatures set: " +
        s"${missingInReaderSet.mkString(", ")}")
  }

  def checkNoReaderWriterFeaturesSupported(
      tblPath: String,
      notExpReaderWriterFeatures: String*): Unit = {
    val protocol = getLatestProtocol(tblPath)
    assert(protocol.readerFeatures.getOrElse(Set.empty)
      .intersect(notExpReaderWriterFeatures.toSet).isEmpty)
    assert(protocol.writerFeatures.getOrElse(Set.empty)
      .intersect(notExpReaderWriterFeatures.toSet).isEmpty)
  }

  def getLatestProtocol(tblPath: String): Protocol = {
    val deltaLog = DeltaLog.forTable(spark, tblPath)
    deltaLog.update()
    deltaLog.snapshot.protocol
  }
}
