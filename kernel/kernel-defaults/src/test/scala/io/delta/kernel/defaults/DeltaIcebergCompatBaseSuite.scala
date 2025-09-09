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

import io.delta.kernel.defaults.utils.AbstractWriteUtils
import io.delta.kernel.exceptions.KernelException
import io.delta.kernel.internal.TableConfig
import io.delta.kernel.internal.tablefeatures.{TableFeature, TableFeatures}
import io.delta.kernel.internal.util.ColumnMappingSuiteBase
import io.delta.kernel.types.{DataType, DateType, IntegerType, LongType, StructField, StructType, TimestampNTZType, TypeChange}

import org.scalatest.funsuite.AnyFunSuite

/**
 * Base suite containing common test cases for Delta Iceberg compatibility features.
 * This includes tests that apply to both V2 and V3 compatibility modes.
 */
trait DeltaIcebergCompatBaseSuite extends AnyFunSuite with AbstractWriteUtils
    with ColumnMappingSuiteBase {

  /** The name of the iceberg compatibility version for display in test names */
  def icebergCompatVersion: String

  /** The table property key for enabling the specific iceberg compatibility version */
  def icebergCompatEnabledKey: String

  /** The table feature that should be enabled for this compatibility version */
  def expectedTableFeatures: Seq[TableFeature]

  def supportedDataColumnTypes: Seq[DataType]

  def supportedPartitionColumnTypes: Seq[DataType]

  supportedDataColumnTypes.foreach {
    dataType: DataType =>
      test(s"allowed data column types: $dataType on creating table") {
        withTempDirAndEngine { (tablePath, engine) =>
          val schema = new StructType().add("col", dataType)
          val tblProps = Map(icebergCompatEnabledKey -> "true")
          createEmptyTable(engine, tablePath, schema, tableProperties = tblProps)
        }
      }
  }

  supportedPartitionColumnTypes.foreach {
    dataType: DataType =>
      test(s"allowed partition column types: $dataType on creating table") {
        withTempDirAndEngine { (tablePath, engine) =>
          val schema = new StructType().add("col", dataType)
          val partitionCols = Seq("col")
          val tblProps = Map(icebergCompatEnabledKey -> "true")
          createEmptyTable(engine, tablePath, schema, partitionCols, tableProperties = tblProps)
        }
      }
  }

  test(s"enable $icebergCompatVersion on creating table") {
    withTempDirAndEngine { (tablePath, engine) =>
      val tblProps = Map(icebergCompatEnabledKey -> "true")
      createEmptyTable(engine, tablePath, cmTestSchema(), tableProperties = tblProps)

      val protocol = getProtocol(engine, tablePath)
      expectedTableFeatures.foreach { feature =>
        assert(protocol.supportsFeature(feature))
      }

      val metadata = getMetadata(engine, tablePath)
      val actualCMMode = metadata.getConfiguration.get(TableConfig.COLUMN_MAPPING_MODE.getKey)
      assert(actualCMMode === "name")
      verifyCMTestSchemaHasValidColumnMappingInfo(metadata)
    }
  }

  test(s"compatible type widening is allowed with $icebergCompatVersion") {
    withTempDirAndEngine { (tablePath, engine) =>
      // Create a table with icebergCompat and type widening enabled
      val schema = new StructType()
        .add(new StructField(
          "intToLong",
          LongType.LONG,
          false).withTypeChanges(Seq(new TypeChange(IntegerType.INTEGER, LongType.LONG)).asJava))

      val tblProps = Map(
        icebergCompatEnabledKey -> "true",
        TableConfig.TYPE_WIDENING_ENABLED.getKey -> "true")

      // This should not throw an exception
      createEmptyTable(engine, tablePath, schema, tableProperties = tblProps)
      appendData(engine, tablePath, data = Seq.empty)

      val protocol = getProtocol(engine, tablePath)
      assert(protocol.supportsFeature(TableFeatures.TYPE_WIDENING_RW_FEATURE))
      val metadata = getMetadata(engine, tablePath)
      assert(metadata.getSchema.get("intToLong").getTypeChanges.asScala == schema.get(
        "intToLong").getTypeChanges.asScala)
    }
  }

  test(s"incompatible type widening throws exception with $icebergCompatVersion") {
    withTempDirAndEngine { (tablePath, engine) =>
      // Try to create a table with icebergCompat and incompatible type widening
      val schema = new StructType()
        .add(
          new StructField(
            "dateToTimestamp",
            TimestampNTZType.TIMESTAMP_NTZ,
            false).withTypeChanges(Seq(
            new TypeChange(DateType.DATE, TimestampNTZType.TIMESTAMP_NTZ)).asJava))

      val tblProps = Map(
        icebergCompatEnabledKey -> "true",
        TableConfig.TYPE_WIDENING_ENABLED.getKey -> "true")

      val e = intercept[KernelException] {
        createEmptyTable(engine, tablePath, schema, tableProperties = tblProps)
      }

      assert(
        e.getMessage.contains(
          s"$icebergCompatVersion does not support type widening present in table"))
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
