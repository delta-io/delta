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
import scala.reflect.ClassTag

import io.delta.kernel.Table
import io.delta.kernel.data.Row
import io.delta.kernel.defaults.utils.{WriteUtils, WriteUtilsWithV2Builders}
import io.delta.kernel.exceptions.KernelException
import io.delta.kernel.internal.TableConfig
import io.delta.kernel.internal.tablefeatures.{TableFeature, TableFeatures}
import io.delta.kernel.internal.util.{ColumnMapping, VectorUtils}
import io.delta.kernel.types.{DataType, DateType, FieldMetadata, StructField, StructType, TimestampNTZType, TypeChange}

class DeltaIcebergCompatV2TransactionBuilderV1Suite extends DeltaIcebergCompatV2SuiteBase
    with WriteUtils {}

class DeltaIcebergCompatV2TransactionBuilderV2Suite extends DeltaIcebergCompatV2SuiteBase
    with WriteUtilsWithV2Builders {}

/** This suite tests reading or writing into Delta table that have `icebergCompatV2` enabled. */
trait DeltaIcebergCompatV2SuiteBase extends DeltaIcebergCompatBaseSuite {

  override def icebergCompatVersion: String = "icebergCompatV2"

  override def icebergCompatEnabledKey: String = TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey

  override def expectedTableFeatures: Seq[TableFeature] = Seq(
    TableFeatures.ICEBERG_COMPAT_V2_W_FEATURE,
    TableFeatures.COLUMN_MAPPING_RW_FEATURE)

  override def supportedDataColumnTypes: Seq[DataType] =
    (Seq.empty ++ SIMPLE_TYPES ++ COMPLEX_TYPES)

  override def supportedPartitionColumnTypes: Seq[DataType] = Seq.empty ++ SIMPLE_TYPES

  ignore("can't enable icebergCompatV2 on a table with icebergCompatv1 enabled") {
    // We can't test this as Kernel throws error when enabling icebergCompatV1
    // as there is no support it in the current version.
    // This is covered in unittests in [[IcebergCompatV2MetadataValidatorAndUpdaterSuite]]
  }

  ignore("test unsupported data types") {
    // Can't test this now as the only unsupported data type in Iceberg is VariantType,
    // and it also has no write support in Kernel.
    // Unit test for this is covered in the respective MetadataValidatorAndUpdaterSuite
  }

  Seq("id", "name").foreach { existingCMMode =>
    // also tests enabling icebergCompat on an existing table
    test(s"existing column mapping mode `$existingCMMode` is " +
      s"preserved after $icebergCompatVersion is enabled") {
      withTempDirAndEngine { (tablePath, engine) =>
        val tblProps = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> existingCMMode)
        createEmptyTable(engine, tablePath, cmTestSchema(), tableProperties = tblProps)

        val newTblProps =
          Map(icebergCompatEnabledKey -> "true")
        updateTableMetadata(engine, tablePath, tableProperties = newTblProps)

        val protocol = getProtocol(engine, tablePath)
        assert(protocol.supportsFeature(TableFeatures.COLUMN_MAPPING_RW_FEATURE))
        val metadata = getMetadata(engine, tablePath)
        val actualCMMode = metadata.getConfiguration.get(TableConfig.COLUMN_MAPPING_MODE.getKey)
        assert(actualCMMode === existingCMMode)
        verifyCMTestSchemaHasValidColumnMappingInfo(metadata)
      }
    }
  }

  Seq("id", "name").foreach { existingCMMode =>
    test(s"existing column mapping mode `$existingCMMode` is " +
      s"preserved after $icebergCompatVersion is enabled for new table") {
      withTempDirAndEngine { (tablePath, engine) =>
        val tblProps = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> existingCMMode,
          icebergCompatEnabledKey -> "true")
        createEmptyTable(engine, tablePath, cmTestSchema(), tableProperties = tblProps)

        val protocol = getProtocol(engine, tablePath)
        assert(protocol.supportsFeature(TableFeatures.COLUMN_MAPPING_RW_FEATURE))
        val metadata = getMetadata(engine, tablePath)
        val actualCMMode = metadata.getConfiguration.get(TableConfig.COLUMN_MAPPING_MODE.getKey)
        assert(actualCMMode === existingCMMode)
        verifyCMTestSchemaHasValidColumnMappingInfo(metadata)
      }
    }
  }

  test(s"when column mapping mode is set to 'none`, should fail enabling $icebergCompatVersion") {
    withTempDirAndEngine { (tablePath, engine) =>
      val tblProps = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> "none")
      createEmptyTable(engine, tablePath, testSchema, tableProperties = tblProps)

      checkError[KernelException](
        s"The value 'none' for the property 'delta.columnMapping.mode' is not " +
          s"compatible with $icebergCompatVersion requirements") {
        val newTblProps =
          Map(icebergCompatEnabledKey -> "true")
        updateTableMetadata(engine, tablePath, tableProperties = newTblProps)
      }
    }
  }

  test(s"can't enable $icebergCompatVersion on an existing table with no column mapping enabled") {
    withTempDirAndEngine { (tablePath, engine) =>
      createEmptyTable(engine, tablePath, testSchema)

      checkError[KernelException](
        s"The value 'none' for the property 'delta.columnMapping.mode' is not " +
          s"compatible with $icebergCompatVersion requirements") {
        val tblProps = Map(icebergCompatEnabledKey -> "true")
        updateTableMetadata(engine, tablePath, testSchema, tableProperties = tblProps)
      }
    }
  }

  test("subsequent writes to icebergCompatV2 enabled tables doesn't update metadata") {
    // we want to make sure the [[IcebergCompatV2MetadataValidatorAndUpdater]] doesn't
    // make unneeded metadata updates
    withTempDirAndEngine { (tablePath, engine) =>
      val tblProps = Map(TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true")
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

  test(s"can't be enabled on a new table with deletion vectors supported") {
    withTempDirAndEngine { (tablePath, engine) =>
      checkError[KernelException](
        s"Table features [deletionVectors] are " +
          s"incompatible with $icebergCompatVersion") {
        val tblProps = Map(
          TableConfig.DELETION_VECTORS_CREATION_ENABLED.getKey -> "true",
          icebergCompatEnabledKey -> "true")
        createEmptyTable(engine, tablePath, schema = testSchema, tableProperties = tblProps)
      }
    }
  }

  test(s"can't update an existing table with DVs supported to have $icebergCompatVersion") {
    withTempDirAndEngine { (tablePath, engine) =>
      val tblProps = Map(
        TableConfig.DELETION_VECTORS_CREATION_ENABLED.getKey -> "true",
        // without CM on existing table, you can't update to icebergCompat
        TableConfig.COLUMN_MAPPING_MODE.getKey -> "name")
      createEmptyTable(engine, tablePath, schema = testSchema, tableProperties = tblProps)

      checkError[KernelException](
        s"Table features [deletionVectors] are " +
          s"incompatible with $icebergCompatVersion") {
        val newTblProps =
          Map(icebergCompatEnabledKey -> "true")
        updateTableMetadata(engine, tablePath, tableProperties = newTblProps)
      }
    }
  }

  test(s"can't enable deletion vectors on a table with $icebergCompatVersion enabled") {
    withTempDirAndEngine { (tablePath, engine) =>
      val tblProps = Map(icebergCompatEnabledKey -> "true")
      createEmptyTable(engine, tablePath, schema = testSchema, tableProperties = tblProps)

      checkError[KernelException](
        s"Table features [deletionVectors] are incompatible with $icebergCompatVersion") {
        val tblProps = Map(TableConfig.DELETION_VECTORS_CREATION_ENABLED.getKey -> "true")
        updateTableMetadata(engine, tablePath, schema = testSchema, tableProperties = tblProps)
      }
    }
  }

  test(
    s"incompatible type widening throws exception with" +
      s" $icebergCompatVersion enabled on existing table") {
    withTempDirAndEngine { (tablePath, engine) =>
      val schema = new StructType()
        .add(new StructField(
          "dateToTimestamp",
          TimestampNTZType.TIMESTAMP_NTZ,
          false,
          FieldMetadata.builder()
            .putLong(ColumnMapping.COLUMN_MAPPING_ID_KEY, 1)
            .putString(
              ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY,
              "col-1").build()).withTypeChanges(
          Seq(new TypeChange(DateType.DATE, TimestampNTZType.TIMESTAMP_NTZ)).asJava))

      val tblProps = Map(TableConfig.TYPE_WIDENING_ENABLED.getKey -> "true")
      createEmptyTable(engine, tablePath, schema, tableProperties = tblProps)

      val e = intercept[KernelException] {
        updateTableMetadata(
          engine,
          tablePath,
          tableProperties = Map(
            icebergCompatEnabledKey -> "true",
            TableConfig.COLUMN_MAPPING_MODE.getKey -> "ID"))
      }

      assert(
        e.getMessage.contains(
          s"$icebergCompatVersion does not support type widening present in table"))
    }
  }
}
