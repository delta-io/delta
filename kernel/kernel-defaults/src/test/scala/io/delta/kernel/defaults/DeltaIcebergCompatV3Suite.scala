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

import io.delta.kernel.Table
import io.delta.kernel.data.Row
import io.delta.kernel.defaults.utils.{WriteUtils, WriteUtilsWithV2Builders}
import io.delta.kernel.exceptions.KernelException
import io.delta.kernel.internal.TableConfig
import io.delta.kernel.internal.tablefeatures.{TableFeature, TableFeatures}
import io.delta.kernel.internal.util.{ColumnMapping, VectorUtils}
import io.delta.kernel.types.{DataType, DateType, FieldMetadata, IntegerType, LongType, StructField, StructType, TimestampNTZType, TypeChange, VariantType}

class DeltaIcebergCompatV3TransactionBuilderV1Suite extends DeltaIcebergCompatV3SuiteBase
    with WriteUtils {}

class DeltaIcebergCompatV3TransactionBuilderV2Suite extends DeltaIcebergCompatV3SuiteBase
    with WriteUtilsWithV2Builders {}

/** This suite tests reading or writing into Delta table that have `icebergCompatV3` enabled. */
trait DeltaIcebergCompatV3SuiteBase extends DeltaIcebergCompatBaseSuite {

  override def icebergCompatVersion: String = "icebergCompatV3"

  override def icebergCompatEnabledKey: String = TableConfig.ICEBERG_COMPAT_V3_ENABLED.getKey

  override def expectedTableFeatures: Seq[TableFeature] = Seq(
    TableFeatures.ICEBERG_COMPAT_V3_W_FEATURE,
    TableFeatures.COLUMN_MAPPING_RW_FEATURE,
    TableFeatures.ROW_TRACKING_W_FEATURE)

  override def supportedDataColumnTypes: Seq[DataType] =
    // TODO add VARIANT_TYPE once it is supported
    (Seq.empty ++ SIMPLE_TYPES ++ COMPLEX_TYPES) // ++ Seq(VariantType.VARIANT))

  override def supportedPartitionColumnTypes: Seq[DataType] = Seq.empty ++ SIMPLE_TYPES

  test(s"enable $icebergCompatVersion on a new table - verify row tracking is enabled") {
    withTempDirAndEngine { (tablePath, engine) =>
      val tblProps = Map(icebergCompatEnabledKey -> "true")
      createEmptyTable(engine, tablePath, cmTestSchema(), tableProperties = tblProps)

      val protocol = getProtocol(engine, tablePath)
      expectedTableFeatures.foreach { feature =>
        assert(protocol.supportsFeature(feature))
      }

      val metadata = getMetadata(engine, tablePath)
      assert(metadata.getConfiguration.get(TableConfig.ROW_TRACKING_ENABLED.getKey) === "true")
      assert(
        metadata.getConfiguration.get(TableConfig.MATERIALIZED_ROW_ID_COLUMN_NAME.getKey).nonEmpty)
      assert(metadata.getConfiguration.get(
        TableConfig.MATERIALIZED_ROW_COMMIT_VERSION_COLUMN_NAME.getKey).nonEmpty)

    }
  }

  test(s"enable $icebergCompatVersion on a new table with deletion vectors") {
    withTempDirAndEngine { (tablePath, engine) =>
      val tblProps = Map(
        TableConfig.DELETION_VECTORS_CREATION_ENABLED.getKey -> "true",
        icebergCompatEnabledKey -> "true")
      createEmptyTable(engine, tablePath, schema = testSchema, tableProperties = tblProps)
    }
  }

  test("can't enable icebergCompatV3 on a existing table") {
    withTempDirAndEngine { (tablePath, engine) =>
      val tblProps = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> "name")
      createEmptyTable(engine, tablePath, schema = testSchema, tableProperties = tblProps)

      checkError[KernelException](
        "Cannot enable delta.enableIcebergCompatV3 on an existing table") {
        val newTblProps = Map(TableConfig.ICEBERG_COMPAT_V3_ENABLED.getKey -> "true")
        updateTableMetadata(engine, tablePath, tableProperties = newTblProps)
      }
    }
  }

  test("can't disable icebergCompatV3 on a existing icebergCompatV3 enabled table") {
    withTempDirAndEngine { (tablePath, engine) =>
      val tblProps = Map(TableConfig.ICEBERG_COMPAT_V3_ENABLED.getKey -> "true")
      createEmptyTable(engine, tablePath, schema = testSchema, tableProperties = tblProps)

      checkError[KernelException](
        "Disabling delta.enableIcebergCompatV3 on an existing table is not allowed") {
        val newTblProps = Map(TableConfig.ICEBERG_COMPAT_V3_ENABLED.getKey -> "false")
        updateTableMetadata(engine, tablePath, tableProperties = newTblProps)
      }
    }
  }

  test("subsequent writes to icebergCompatV3 enabled tables doesn't update metadata") {
    // we want to make sure the [[IcebergCompatV3MetadataValidatorAndUpdater]] doesn't
    // make unneeded metadata updates
    withTempDirAndEngine { (tablePath, engine) =>
      val tblProps = Map(
        TableConfig.ICEBERG_COMPAT_V3_ENABLED.getKey -> "true")
      createEmptyTable(engine, tablePath, testSchema, tableProperties = tblProps)

      val metadata = getMetadata(engine, tablePath)
      val actualCMMode = metadata.getConfiguration.get(TableConfig.COLUMN_MAPPING_MODE.getKey)
      assert(actualCMMode === "name")

      appendData(engine, tablePath, data = Seq.empty) // version 1
      appendData(engine, tablePath, data = Seq.empty) // version 2

      val table = Table.forPath(engine, tablePath)
      assert(getMetadataActionFromCommit(engine, table, version = 0).isDefined)
      assert(getMetadataActionFromCommit(engine, table, version = 1).isEmpty)
      assert(getMetadataActionFromCommit(engine, table, version = 2).isEmpty)

      // make a metadata update and see it is reflected in the table
      val newProps = Map("key" -> "value")
      updateTableMetadata(engine, tablePath, tableProperties = newProps) // version 3
      val ver3Metadata: Row = getMetadataActionFromCommit(engine, table, version = 3)
        .getOrElse(fail("Metadata action not found in version 3"))

      // TODO: ugly, find a better utilities
      val result = VectorUtils.toJavaMap[String, String](
        ver3Metadata.getMap(ver3Metadata.getSchema.indexOf("configuration")))
        .get("key")
      assert(result === "value")
    }
  }
}
