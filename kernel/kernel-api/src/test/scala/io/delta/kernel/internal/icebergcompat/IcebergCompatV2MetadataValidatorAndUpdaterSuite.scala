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

import scala.collection.JavaConverters._

import io.delta.kernel.exceptions.KernelException
import io.delta.kernel.internal.actions.Protocol
import io.delta.kernel.internal.icebergcompat.IcebergCompatV2MetadataValidatorAndUpdater.validateAndUpdateIcebergCompatV2Metadata
import io.delta.kernel.internal.tablefeatures.TableFeatures.{COLUMN_MAPPING_RW_FEATURE, DELETION_VECTORS_RW_FEATURE, ICEBERG_COMPAT_V2_W_FEATURE, TYPE_WIDENING_RW_FEATURE}
import io.delta.kernel.internal.util.ColumnMappingSuiteBase
import io.delta.kernel.test.VectorTestUtils
import io.delta.kernel.types._

import org.scalatest.funsuite.AnyFunSuite

class IcebergCompatV2MetadataValidatorAndUpdaterSuite
    extends AnyFunSuite
    with VectorTestUtils with ColumnMappingSuiteBase {

  import IcebergCompatV2MetadataValidatorAndUpdaterSuite._

  (SIMPLE_TYPES ++ COMPLEX_TYPES).foreach {
    dataType: DataType =>
      Seq(true, false).foreach { isNewTable =>
        test(s"allowed data column types: $dataType, new table = $isNewTable") {
          val schema = new StructType().add("col", dataType)
          val metadata = testMetadata(schema).withIcebergCompatV2AndCMEnabled()

          val protocol = testProtocol(ICEBERG_COMPAT_V2_W_FEATURE, COLUMN_MAPPING_RW_FEATURE)
          validateAndUpdateIcebergCompatV2Metadata(isNewTable, metadata, protocol)
        }
      }
  }

  SIMPLE_TYPES.foreach {
    dataType: DataType =>
      Seq(true, false).foreach { isNewTable =>
        test(s"allowed partition column types: $dataType, new table = $isNewTable") {
          val schema = new StructType().add("col", dataType)
          val metadata = testMetadata(schema, partitionCols = Seq("col"))
            .withIcebergCompatV2AndCMEnabled()
          val protocol = testProtocol(ICEBERG_COMPAT_V2_W_FEATURE, COLUMN_MAPPING_RW_FEATURE)
          validateAndUpdateIcebergCompatV2Metadata(isNewTable, metadata, protocol)
        }
      }
  }

  UNSUPPORTED_DATA_COLUMN_TYPES.foreach {
    dataType: DataType =>
      Seq(true, false).foreach { isNewTable =>
        test(s"disallowed data column types: $dataType, new table = $isNewTable") {
          val schema = new StructType().add("col", dataType)
          val metadata = testMetadata(schema).withIcebergCompatV2AndCMEnabled()
          val protocol = testProtocol(ICEBERG_COMPAT_V2_W_FEATURE, COLUMN_MAPPING_RW_FEATURE)
          val e = intercept[KernelException] {
            validateAndUpdateIcebergCompatV2Metadata(isNewTable, metadata, protocol)
          }
          assert(e.getMessage.contains(
            s"icebergCompatV2 does not support the data types: "))
        }
      }
  }

  UNSUPPORTED_PARTITION_COLUMN_TYPES.foreach {
    dataType: DataType =>
      Seq(true, false).foreach { isNewTable =>
        test(s"disallowed partition column types: $dataType, new table = $isNewTable") {
          val schema = new StructType().add("col", dataType)
          val metadata = testMetadata(schema, partitionCols = Seq("col"))
            .withIcebergCompatV2AndCMEnabled()
          val protocol = testProtocol(ICEBERG_COMPAT_V2_W_FEATURE, COLUMN_MAPPING_RW_FEATURE)
          val e = intercept[KernelException] {
            validateAndUpdateIcebergCompatV2Metadata(isNewTable, metadata, protocol)
          }
          assert(e.getMessage.matches(
            s"icebergCompatV2 does not support the data type .* for a partition column."))
        }
      }
  }

  Seq(true, false).foreach { isNewTable =>
    test(s"can't be enabled on a table with deletion vectors supported, isNewTable $isNewTable") {
      val schema = new StructType().add("col", BooleanType.BOOLEAN)
      val metadata = testMetadata(schema, Seq.empty).withIcebergCompatV2AndCMEnabled()
      val protocol = testProtocol(
        ICEBERG_COMPAT_V2_W_FEATURE,
        COLUMN_MAPPING_RW_FEATURE,
        DELETION_VECTORS_RW_FEATURE)
      val e = intercept[KernelException] {
        validateAndUpdateIcebergCompatV2Metadata(isNewTable, metadata, protocol)
      }
      assert(e.getMessage.contains(
        "Simultaneous support for icebergCompatV2 and deletion vectors is not compatible."))
    }
  }

  Seq(true, false).foreach { isNewTable =>
    test(s"protocol is missing required column mapping feature, isNewTable $isNewTable") {
      val schema = new StructType().add("col", BooleanType.BOOLEAN)
      val metadata = testMetadata(schema, Seq.empty).withIcebergCompatV2AndCMEnabled()
      val protocol = new Protocol(3, 7, Set.empty.asJava, Set("icebergCompatV2").asJava)
      val e = intercept[KernelException] {
        validateAndUpdateIcebergCompatV2Metadata(isNewTable, metadata, protocol)
      }
      assert(e.getMessage.contains(
        "icebergCompatV2: requires the feature 'columnMapping' to be enabled."))
    }
  }

  Seq(true, false).foreach { isNewTable =>
    test(s"column mapping mode `name` is auto enabled when icebergCompatV2 is enabled, " +
      s"isNewTable = $isNewTable") {
      val metadata = testMetadata(cmTestSchema()).withIcebergCompatV2Enabled
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
    Seq(true, false).foreach { isNewTable =>
      test(s"existing column mapping mode `$existingCMMode` is preserved " +
        s"when icebergCompatV2 is enabled, isNewTable = $isNewTable") {
        val metadata = testMetadata(cmTestSchema())
          .withIcebergCompatV2Enabled
          .withColumnMappingEnabled(existingCMMode)
        val protocol = testProtocol(ICEBERG_COMPAT_V2_W_FEATURE, COLUMN_MAPPING_RW_FEATURE)

        assert(metadata.getConfiguration.get("delta.columnMapping.mode") === existingCMMode)

        val updatedMetadata =
          validateAndUpdateIcebergCompatV2Metadata(isNewTable, metadata, protocol)
        // No metadata update is needed since already compatible column mapping mode
        assert(!updatedMetadata.isPresent)
      }
    }
  }

  Seq(true, false).foreach { isNewTable =>
    test(s"can't enable icebergCompatV2 on a table with type widening supported, " +
      s"isNewTable = $isNewTable") {
      val schema = new StructType().add("col", BooleanType.BOOLEAN)
      val metadata = testMetadata(schema).withIcebergCompatV2AndCMEnabled()
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

  Seq(true, false).foreach { isNewTable =>
    test(s"can't enable icebergCompatV2 on a table with icebergCompatv1 enabled, " +
      s"isNewTable = $isNewTable") {
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
        "icebergCompatV2: Only one IcebergCompat version can be enabled. " +
          "Incompatible version enabled: delta.enableIcebergCompatV1"))
    }
  }
}

object IcebergCompatV2MetadataValidatorAndUpdaterSuite {
  // Allowed simple types as data or partition columns
  val SIMPLE_TYPES: Seq[DataType] = Seq(
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
  val COMPLEX_TYPES: Seq[DataType] = Seq(
    new ArrayType(BooleanType.BOOLEAN, true),
    new MapType(IntegerType.INTEGER, LongType.LONG, true),
    new StructType().add("s1", BooleanType.BOOLEAN).add("s2", IntegerType.INTEGER))

  // Unsupported data type columns
  val UNSUPPORTED_DATA_COLUMN_TYPES: Seq[VariantType] = Seq(VariantType.VARIANT)

  // Unsupported partition column types
  val UNSUPPORTED_PARTITION_COLUMN_TYPES: Seq[DataType] = COMPLEX_TYPES
}
