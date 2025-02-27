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
package io.delta.kernel.internal.icebergcompat

import java.util.{Collections, Optional}

import scala.collection.JavaConverters._

import io.delta.kernel.data.{ArrayValue, ColumnVector, MapValue}
import io.delta.kernel.exceptions.KernelException
import io.delta.kernel.internal.actions.{Format, Metadata, Protocol}
import io.delta.kernel.internal.icebergcompat.IcebergCompatV2MetadataValidatorAndUpdater.validateAndUpdateIcebergCompatV2Metadata
import io.delta.kernel.internal.tablefeatures.{TableFeature, TableFeatures}
import io.delta.kernel.internal.tablefeatures.TableFeatures.{COLUMN_MAPPING_RW_FEATURE, DELETION_VECTORS_RW_FEATURE, ICEBERG_COMPAT_V2_W_FEATURE, TYPE_WIDENING_RW_FEATURE}
import io.delta.kernel.test.VectorTestUtils
import io.delta.kernel.types.{ArrayType, BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, ShortType, StringType, StructType, TimestampNTZType, TimestampType, VariantType}

import org.scalatest.funsuite.AnyFunSuite

class IcebergCompatV2MetadataValidatorAndUpdaterSuite extends AnyFunSuite with VectorTestUtils {

  // Allowed simple types as data or partition columns
  val SIMPLE_TYPES = Seq(
    BooleanType.BOOLEAN,
    ByteType.BYTE,
    ShortType.SHORT,
    IntegerType.INTEGER,
    LongType.LONG,
    FloatType.FLOAT,
    DoubleType.DOUBLE,
    DateType.DATE,
    TimestampType.TIMESTAMP,
    TimestampNTZType.TIMESTAMP_NTZ,
    StringType.STRING,
    BinaryType.BINARY,
    new DecimalType(10, 5))

  // Allowed complex types as data columns
  val COMPLEX_TYPES = Seq(
    new ArrayType(BooleanType.BOOLEAN, true),
    new MapType(IntegerType.INTEGER, LongType.LONG, true),
    new StructType().add("s1", BooleanType.BOOLEAN).add("s2", IntegerType.INTEGER))

  // Unsupported data type columns
  val UNSUPPORTED_DATA_COLUMN_TYPES = Seq(VariantType.VARIANT)

  // Unsupported partition column types
  val UNSUPPORTED_PARTITION_COLUMN_TYPES = COMPLEX_TYPES

  (SIMPLE_TYPES ++ COMPLEX_TYPES).foreach {
    dataType: DataType =>
      test(s"allowed data column types: $dataType") {
        val schema = new StructType().add("col", dataType)
        val metadata = testMetadata(schema, Seq.empty)
        val protocol = testProtocol(ICEBERG_COMPAT_V2_W_FEATURE, COLUMN_MAPPING_RW_FEATURE)
        Seq(true, false).foreach { isNewTable =>
          validateAndUpdateIcebergCompatV2Metadata(isNewTable, metadata, protocol)
        }
      }
  }

  SIMPLE_TYPES.foreach {
    dataType: DataType =>
      test(s"allowed partition column types: $dataType") {
        val schema = new StructType().add("col", dataType)
        val metadata = testMetadata(schema, Seq("col"))
        val protocol = testProtocol(ICEBERG_COMPAT_V2_W_FEATURE, COLUMN_MAPPING_RW_FEATURE)
        Seq(true, false).foreach { isNewTable =>
          validateAndUpdateIcebergCompatV2Metadata(isNewTable, metadata, protocol)
        }
      }
  }

  UNSUPPORTED_DATA_COLUMN_TYPES.foreach {
    dataType: DataType =>
      test(s"disallowed data column types: $dataType") {
        val schema = new StructType().add("col", dataType)
        val metadata = testMetadata(schema, Seq.empty)
        val protocol = testProtocol(ICEBERG_COMPAT_V2_W_FEATURE, COLUMN_MAPPING_RW_FEATURE)
        Seq(true, false).foreach { isNewTable =>
          val e = intercept[KernelException] {
            validateAndUpdateIcebergCompatV2Metadata(isNewTable, metadata, protocol)
          }
          assert(e.getMessage.contains(
            s"IcebergCompatV2 does not support the data types: [$dataType]"))
        }
      }
  }

  UNSUPPORTED_PARTITION_COLUMN_TYPES.foreach {
    dataType: DataType =>
      test(s"disallowed partition column types: $dataType") {
        val schema = new StructType().add("col", dataType)
        val metadata = testMetadata(schema, Seq("col"))
        val protocol = testProtocol(ICEBERG_COMPAT_V2_W_FEATURE, COLUMN_MAPPING_RW_FEATURE)
        Seq(true, false).foreach { isNewTable =>
          val e = intercept[KernelException] {
            validateAndUpdateIcebergCompatV2Metadata(isNewTable, metadata, protocol)
          }
          assert(e.getMessage.contains(
            s"IcebergCompatV2 does not support the data type '$dataType' for a partition column"))
        }
      }
  }

  test("can't be enabled on a table with deletion vectors supported") {
    val schema = new StructType().add("col", BooleanType.BOOLEAN)
    val metadata = testMetadata(schema, Seq.empty)
    val protocol = testProtocol(
      ICEBERG_COMPAT_V2_W_FEATURE,
      COLUMN_MAPPING_RW_FEATURE,
      DELETION_VECTORS_RW_FEATURE)
    Seq(true, false).foreach { isNewTable =>
      val e = intercept[KernelException] {
        validateAndUpdateIcebergCompatV2Metadata(isNewTable, metadata, protocol)
      }
      assert(e.getMessage.contains(
        "IcebergCompatV2 is not supported on a table with deletion vectors supported"))
    }
  }

  test("protocol has is missing required column mapping feature") {
    val schema = new StructType().add("col", BooleanType.BOOLEAN)
    val metadata = testMetadata(schema, Seq.empty)
    val protocol = new Protocol(3, 7, Set.empty.asJava, Set("icebergCompatV2").asJava)
    Seq(true, false).foreach { isNewTable =>
      val e = intercept[KernelException] {
        validateAndUpdateIcebergCompatV2Metadata(isNewTable, metadata, protocol)
      }
      assert(e.getMessage.contains(
        "The table feature 'columnMapping' is required for Iceberg compatibility"))
    }
  }

  test("column mapping mode `name` is auto enabled when icebergCompatV2 is enabled") {
    Seq(true, false).foreach { isNewTable =>
      val schema = new StructType().add("col", BooleanType.BOOLEAN)
      val metadata = testMetadata(schema, tblProps = Map("delta.enableIcebergCompatV2" -> "true"))
      val protocol = testProtocol(ICEBERG_COMPAT_V2_W_FEATURE, COLUMN_MAPPING_RW_FEATURE)

      assert(!metadata.getConfiguration.containsKey("delta.columnMapping.mode"))

      if (isNewTable) {
        val updatedMetadata =
          validateAndUpdateIcebergCompatV2Metadata(isNewTable, metadata, protocol)
        assert(updatedMetadata.isPresent)
        assert(updatedMetadata.get().getConfiguration.get("delta.columnMapping.mode") == "name")
      } else {
        val e = intercept[KernelException] {
          validateAndUpdateIcebergCompatV2Metadata(isNewTable, metadata, protocol)
        }
        assert(e.getMessage.contains(
          "The value 'none' for the property 'delta.columnMapping.mode' is" +
            " not compatible with Iceberg compat requirements"))
      }
    }
  }

  Seq("id", "name").foreach { existingCMMode =>
    test(s"existing column mapping mode `$existingCMMode` is preserved " +
      s"when icebergCompatV2 is enabled") {
      Seq(true, false).foreach { isNewTable =>
        val schema = new StructType().add("col", BooleanType.BOOLEAN)
        val metadata = testMetadata(
          schema,
          tblProps = Map(
            "delta.enableIcebergCompatV2" -> "true",
            "delta.columnMapping.mode" -> existingCMMode))
        val protocol = testProtocol(ICEBERG_COMPAT_V2_W_FEATURE, COLUMN_MAPPING_RW_FEATURE)

        assert(metadata.getConfiguration.get("delta.columnMapping.mode") === existingCMMode)

        val updatedMetadata =
          validateAndUpdateIcebergCompatV2Metadata(isNewTable, metadata, protocol)
        assert(!updatedMetadata.isPresent) // expect no changes as CM mode is already set
        assert(metadata.getConfiguration.get("delta.columnMapping.mode") === existingCMMode)
      }
    }
  }

  test("can't enable icebergCompatV2 on a table with type widening supported") {
    Seq(true, false).foreach { isNewTable =>
      val schema = new StructType().add("col", BooleanType.BOOLEAN)
      val metadata = testMetadata(
        schema,
        tblProps =
          Map("delta.enableIcebergCompatV2" -> "true", "delta.columnMapping.mode" -> "name"))
      val protocol = testProtocol(
        ICEBERG_COMPAT_V2_W_FEATURE,
        COLUMN_MAPPING_RW_FEATURE,
        TYPE_WIDENING_RW_FEATURE)

      val ex = intercept[KernelException] {
        validateAndUpdateIcebergCompatV2Metadata(isNewTable, metadata, protocol)
      }
      assert(ex.getMessage.contains(
        "Unsupported Delta table feature: table requires feature \"typeWidening\" which " +
          "is unsupported by this version of Delta Kernel."))
    }
  }

  test("can't enable icebergCompatV2 on a table with icebergCompatv1 enabled") {
    Seq(true, false).foreach { isNewTable =>
      val schema = new StructType().add("col", BooleanType.BOOLEAN)
      val metadata = testMetadata(
        schema,
        tblProps = Map(
          "delta.enableIcebergCompatV2" -> "true",
          "delta.columnMapping.mode" -> "name",
          "delta.enableIcebergCompatV1" -> "true"))

      val protocol = testProtocol(ICEBERG_COMPAT_V2_W_FEATURE, COLUMN_MAPPING_RW_FEATURE)

      val ex = intercept[KernelException] {
        validateAndUpdateIcebergCompatV2Metadata(isNewTable, metadata, protocol)
      }
      assert(ex.getMessage.contains(
        "Only one IcebergCompat version can be enabled, please explicitly " +
          "disable all otherIcebergCompat versions that are not needed."))
    }
  }

  def testMetadata(
      schema: StructType,
      partitionCols: Seq[String] = Seq.empty,
      tblProps: Map[String, String] =
        Map(
          "delta.enableIcebergCompatV2" -> "true",
          "delta.columnMapping.mode" -> "name")): Metadata = {
    new Metadata(
      "id",
      Optional.of("name"),
      Optional.of("description"),
      new Format("parquet", Collections.emptyMap()),
      schema.toJson,
      schema,
      new ArrayValue() { // partitionColumns
        override def getSize: Int = partitionCols.size
        override def getElements: ColumnVector = stringVector(partitionCols)
      },
      Optional.empty(),
      new MapValue() { // conf
        override def getSize: Int = tblProps.size
        override def getKeys: ColumnVector = stringVector(tblProps.toSeq.map(_._1))
        override def getValues: ColumnVector = stringVector(tblProps.toSeq.map(_._2))
      })
  }

  def testProtocol(tableFeatures: TableFeature*): Protocol = {
    val protocol = new Protocol(3, 7)
    protocol.withFeatures(tableFeatures.asJava).normalized()
  }
}
