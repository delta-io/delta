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

import scala.collection.immutable.Seq
import scala.reflect.ClassTag

import io.delta.kernel.Table
import io.delta.kernel.data.Row
import io.delta.kernel.exceptions.KernelException
import io.delta.kernel.internal.TableConfig
import io.delta.kernel.internal.tablefeatures.TableFeatures
import io.delta.kernel.internal.util.{ColumnMappingSuiteBase, VectorUtils}
import io.delta.kernel.types.{DataType, StructType}

/** This suite tests reading or writing into Delta table that have `icebergCompatV2` enabled. */
class DeltaIcebergCompatV2Suite extends DeltaTableWriteSuiteBase with ColumnMappingSuiteBase {
  import io.delta.kernel.internal.icebergcompat.IcebergCompatV2MetadataValidatorAndUpdaterSuiteBase._

  (SIMPLE_TYPES ++ COMPLEX_TYPES).foreach {
    dataType: DataType =>
      test(s"allowed data column types: $dataType on a new table") {
        withTempDirAndEngine { (tablePath, engine) =>
          val schema = new StructType().add("col", dataType)
          val tblProps = Map(TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true")
          createEmptyTable(engine, tablePath, schema, tableProperties = tblProps)
        }
      }
  }

  ignore("test unsupported data types") {
    // Can't test this now as the only unsupported data type in Iceberg is VariantType,
    // and it also has no write support in Kernel.
    // Unit test for this is covered in [[IcebergCompatV2MetadataValidatorAndUpdaterSuite]]
  }

  SIMPLE_TYPES.foreach {
    dataType: DataType =>
      test(s"allowed partition column types: $dataType on a new table") {
        withTempDirAndEngine { (tablePath, engine) =>
          val schema = new StructType().add("col", dataType)
          val partitionCols = Seq("col")
          val tblProps = Map(TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true")
          createEmptyTable(engine, tablePath, schema, partitionCols, tableProperties = tblProps)
        }
      }
  }

  test("enable icebergCompatV2 on a new table") {
    withTempDirAndEngine { (tablePath, engine) =>
      val tblProps = Map(TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true")
      createEmptyTable(engine, tablePath, cmTestSchema(), tableProperties = tblProps)

      val protocol = getProtocol(engine, tablePath)
      assert(protocol.supportsFeature(TableFeatures.COLUMN_MAPPING_RW_FEATURE))
      assert(protocol.supportsFeature(TableFeatures.ICEBERG_COMPAT_V2_W_FEATURE))

      val metadata = getMetadata(engine, tablePath)
      val actualCMMode = metadata.getConfiguration.get(TableConfig.COLUMN_MAPPING_MODE.getKey)
      assert(actualCMMode === "name")
      verifyCMTestSchemaHasValidColumnMappingInfo(metadata)
    }
  }

  Seq("id", "name").foreach { existingCMMode =>
    // also tests enabling icebergCompatV2 on an existing table
    test(s"existing column mapping mode `$existingCMMode` is " +
      s"preserved after icebergCompatV2 is enabled") {
      withTempDirAndEngine { (tablePath, engine) =>
        val tblProps = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> existingCMMode)
        createEmptyTable(engine, tablePath, cmTestSchema(), tableProperties = tblProps)

        val newTblProps = Map(TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true")
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
      s"preserved after icebergCompatV2 is enabled for new table") {
      withTempDirAndEngine { (tablePath, engine) =>
        val tblProps = Map(
          TableConfig.COLUMN_MAPPING_MODE.getKey -> existingCMMode,
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true")
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

  test("when column mapping mode is set to 'none`, should fail enabling icebergCompatV2") {
    withTempDirAndEngine { (tablePath, engine) =>
      val tblProps = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> "none")
      createEmptyTable(engine, tablePath, testSchema, tableProperties = tblProps)

      checkError[KernelException](
        "The value 'none' for the property 'delta.columnMapping.mode' is not " +
          "compatible with icebergCompatV2 requirements") {
        val newTblProps = Map(TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true")
        updateTableMetadata(engine, tablePath, tableProperties = newTblProps)
      }
    }
  }

  test("can't be enabled on a new table with deletion vectors supported") {
    withTempDirAndEngine { (tablePath, engine) =>
      checkError[KernelException](
        "Simultaneous support for icebergCompatV2 and deletion vectors is not compatible.") {
        val tblProps = Map(
          TableConfig.DELETION_VECTORS_CREATION_ENABLED.getKey -> "true",
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true")
        createEmptyTable(engine, tablePath, schema = testSchema, tableProperties = tblProps)
      }
    }
  }

  test("can't update an existing table with DVs supported to have iceberg compat v2") {
    withTempDirAndEngine { (tablePath, engine) =>
      val tblProps = Map(
        TableConfig.DELETION_VECTORS_CREATION_ENABLED.getKey -> "true",
        // without CM on existing table, you can't update to icebergCompatV2
        TableConfig.COLUMN_MAPPING_MODE.getKey -> "name")
      createEmptyTable(engine, tablePath, schema = testSchema, tableProperties = tblProps)

      checkError[KernelException](
        "Simultaneous support for icebergCompatV2 and deletion vectors is not compatible.") {
        val newTblProps = Map(TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true")
        updateTableMetadata(engine, tablePath, tableProperties = newTblProps)
      }
    }
  }

  test("can't enable deletion vectors on a table with icebergCompatV2 enabled") {
    withTempDirAndEngine { (tablePath, engine) =>
      val tblProps = Map(TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true")
      createEmptyTable(engine, tablePath, schema = testSchema, tableProperties = tblProps)

      checkError[KernelException](
        "Simultaneous support for icebergCompatV2 and deletion vectors is not compatible.") {
        val tblProps = Map(TableConfig.DELETION_VECTORS_CREATION_ENABLED.getKey -> "true")
        updateTableMetadata(engine, tablePath, schema = testSchema, tableProperties = tblProps)
      }
    }
  }

  test("can't enable icebergCompatV2 on an existing table with no column mapping enabled") {
    withTempDirAndEngine { (tablePath, engine) =>
      createEmptyTable(engine, tablePath, testSchema)

      checkError[KernelException](
        "The value 'none' for the property 'delta.columnMapping.mode' is not " +
          "compatible with icebergCompatV2 requirements") {
        val tblProps = Map(TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true")
        updateTableMetadata(engine, tablePath, testSchema, tableProperties = tblProps)
      }
    }
  }

  test("can't enable icebergCompatV2 on a table with type widening supported also enabled") {
    withTempDirAndEngine { (tablePath, engine) =>
      checkError[KernelException]("Unsupported Delta writer feature") {
        val tblProps = Map(
          TableConfig.TYPE_WIDENING_ENABLED.getKey -> "true",
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true")
        createEmptyTable(engine, tablePath, testSchema, tableProperties = tblProps)
      }
    }
  }

  ignore("can't enable icebergCompatV2 on a table with icebergCompatv1 enabled") {
    // We can't test this as Kernel throws error when enabling icebergCompatV1
    // as there is no support it in the current version.
    // This is covered in unittests in [[IcebergCompatV2MetadataValidatorAndUpdaterSuite]]
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

  /**
   * Utility that checks after executing given fn gets the given exception and error message.
   * [[ClassTag]] is used to preserve the type information during the runtime.
   */
  def checkError[T <: Throwable: ClassTag](expectedMessage: String)(fn: => Unit): Unit = {
    val e = intercept[T] {
      fn
    }
    assert(e.getMessage.contains(expectedMessage))
  }
}
