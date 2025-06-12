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

import java.util.Optional

import scala.collection.JavaConverters._

import io.delta.kernel.exceptions.KernelException
import io.delta.kernel.internal.actions.{Metadata, Protocol}
import io.delta.kernel.internal.tablefeatures.TableFeature
import io.delta.kernel.internal.tablefeatures.TableFeatures.{COLUMN_MAPPING_RW_FEATURE, DELETION_VECTORS_RW_FEATURE, ICEBERG_COMPAT_V2_W_FEATURE, TYPE_WIDENING_RW_FEATURE, TYPE_WIDENING_RW_PREVIEW_FEATURE}
import io.delta.kernel.internal.util.ColumnMappingSuiteBase
import io.delta.kernel.test.VectorTestUtils
import io.delta.kernel.types._

import org.scalatest.funsuite.AnyFunSuite

/**
 * Base trait for testing Iceberg compatibility metadata validation and updates.
 * This trait provides common functionality and test cases
 * that can be used by both writer and compat test suites.
 */
trait IcebergCompatMetadataValidatorAndUpdaterSuiteBase extends AnyFunSuite
    with VectorTestUtils with ColumnMappingSuiteBase {

  /** The version of Iceberg compatibility being tested (e.g., "V2" or "V3") */
  def icebergCompatVersion: String

  /** When testing supported simple column types skip any types defined here */
  def simpleTypesToSkip: Set[DataType]

  /** Get a metadata with the given schema and partCols with the desired icebergCompat enabled */
  def getCompatEnabledMetadata(
      schema: StructType,
      partCols: Seq[String] = Seq.empty): Metadata

  /** Get a protocol with features needed for the desired icebergCompat plus the `tableFeatures` */
  def getCompatEnabledProtocol(tableFeatures: TableFeature*): Protocol

  /** Run the desired validate and update metadata method that triggers icebergCompat checks */
  def validateAndUpdateIcebergCompatMetadata(
      isNewTable: Boolean,
      metadata: Metadata,
      protocol: Protocol): Optional[Metadata]

  /** Returns a [[Metadata]] instance with IcebergCompat feature and column mapping mode enabled */
  def withIcebergCompatAndCMEnabled(schema: StructType, partCols: Seq[String]): Metadata

  /** Get the set of supported data column types */
  def supportedDataColumnTypes: Set[DataType]

  /** Get the set of unsupported data column types */
  def unsupportedDataColumnTypes: Set[DataType]

  /** Get the set of unsupported partition column types */
  def unsupportedPartitionColumnTypes: Set[DataType]

  /** Whether deletion vectors are supported */
  def isDeletionVectorsSupported: Boolean

  /** Get the set of required table features */
  def requiredTableFeatures: Set[TableFeature]

  // Common test cases that apply to both writer and compat versions

  supportedDataColumnTypes.diff(simpleTypesToSkip).foreach {
    dataType: DataType =>
      Seq(true, false).foreach { isNewTable =>
        test(s"allowed data column types: $dataType, new table = $isNewTable") {
          val schema = new StructType().add("col", dataType)
          val metadata = getCompatEnabledMetadata(schema)
          val protocol = getCompatEnabledProtocol()
          validateAndUpdateIcebergCompatMetadata(isNewTable, metadata, protocol)
        }
      }
  }

  IcebergCompatMetadataValidatorAndUpdaterSuiteBase.SIMPLE_TYPES.diff(simpleTypesToSkip).foreach {
    dataType: DataType =>
      Seq(true, false).foreach { isNewTable =>
        test(s"allowed partition column types: $dataType, new table = $isNewTable") {
          val schema = new StructType().add("col", dataType)
          val metadata = getCompatEnabledMetadata(schema, Seq("col"))
          val protocol = getCompatEnabledProtocol()
          validateAndUpdateIcebergCompatMetadata(isNewTable, metadata, protocol)
        }
      }
  }

  unsupportedDataColumnTypes.foreach {
    dataType: DataType =>
      Seq(true, false).foreach { isNewTable =>
        test(s"disallowed data column types: $dataType, new table = $isNewTable") {
          val schema = new StructType().add("col", dataType)
          val metadata = getCompatEnabledMetadata(schema)
          val protocol = getCompatEnabledProtocol()
          val e = intercept[KernelException] {
            validateAndUpdateIcebergCompatMetadata(isNewTable, metadata, protocol)
          }
          assert(e.getMessage.contains(
            s"icebergCompat$icebergCompatVersion does not support the data types: "))
        }
      }
  }

  unsupportedPartitionColumnTypes.foreach {
    dataType: DataType =>
      Seq(true, false).foreach { isNewTable =>
        test(s"disallowed partition column types: $dataType, new table = $isNewTable") {
          val schema = new StructType().add("col", dataType)
          val metadata = getCompatEnabledMetadata(schema, Seq("col"))
          val protocol = getCompatEnabledProtocol()
          val e = intercept[KernelException] {
            validateAndUpdateIcebergCompatMetadata(isNewTable, metadata, protocol)
          }
          assert(e.getMessage.matches(
            s"icebergCompat$icebergCompatVersion does not support" +
              s" the data type .* for a partition column."))
        }
      }
  }

  Seq(true, false).foreach { isNewTable =>
    test(s"deletion vectors support behavior, isNewTable $isNewTable") {
      val schema = new StructType().add("col", BooleanType.BOOLEAN)
      val metadata = getCompatEnabledMetadata(schema)
      val protocol = getCompatEnabledProtocol(DELETION_VECTORS_RW_FEATURE)

      if (isDeletionVectorsSupported) {
        // Should not throw an exception
        validateAndUpdateIcebergCompatMetadata(isNewTable, metadata, protocol)
      } else {
        val e = intercept[KernelException] {
          validateAndUpdateIcebergCompatMetadata(isNewTable, metadata, protocol)
        }
        assert(e.getMessage.contains(
          s"Table features [deletionVectors] are incompatible " +
            s"with icebergCompat$icebergCompatVersion"))
      }
    }
  }

  // Compat-specific test cases
  test("compatible type widening is allowed") {
    val schema = new StructType()
      .add(
        new StructField(
          "intToLong",
          IntegerType.INTEGER,
          true,
          FieldMetadata.empty()).withTypeChanges(
          Seq(new TypeChange(IntegerType.INTEGER, LongType.LONG)).asJava))
      .add(
        new StructField(
          "decimalToDecimal",
          new DecimalType(10, 2),
          true,
          FieldMetadata.empty()).withTypeChanges(
          Seq(new TypeChange(new DecimalType(5, 2), new DecimalType(10, 2))).asJava))

    val metadata = getCompatEnabledMetadata(schema)
    val protocol = getCompatEnabledProtocol(TYPE_WIDENING_RW_FEATURE)

    // This should not throw an exception
    validateAndUpdateIcebergCompatMetadata(false, metadata, protocol)
  }

  test("incompatible type widening throws exception") {
    val schema = new StructType()
      .add(
        new StructField(
          "dateToTimestamp",
          TimestampNTZType.TIMESTAMP_NTZ,
          true,
          FieldMetadata.empty()).withTypeChanges(
          Seq(new TypeChange(DateType.DATE, TimestampNTZType.TIMESTAMP_NTZ)).asJava))

    val metadata = getCompatEnabledMetadata(schema)
    val protocol = getCompatEnabledProtocol(TYPE_WIDENING_RW_FEATURE)

    val e = intercept[KernelException] {
      validateAndUpdateIcebergCompatMetadata(false, metadata, protocol)
    }

    assert(e.getMessage.contains(
      s"icebergCompat$icebergCompatVersion does not support type widening present in table"))
  }

  Seq("id", "name").foreach { existingCMMode =>
    Seq(true, false).foreach { isNewTable =>
      test(s"existing column mapping mode `$existingCMMode` is preserved " +
        s"when icebergCompat is enabled, isNewTable = $isNewTable") {
        val metadata = testMetadata(cmTestSchema())
          .withMergedConfiguration(
            Map(
              s"delta.enableIcebergCompat$icebergCompatVersion" -> "true",
              "delta.columnMapping.mode" -> existingCMMode).asJava)
        val protocol = getCompatEnabledProtocol()

        assert(metadata.getConfiguration.get("delta.columnMapping.mode") === existingCMMode)

        val updatedMetadata =
          validateAndUpdateIcebergCompatMetadata(isNewTable, metadata, protocol)
        // No metadata update is needed since already compatible column mapping mode
        assert(!updatedMetadata.isPresent)
      }
    }
  }

  Seq(true, false).foreach { isNewTable =>
    test(
      s"can't enable icebergCompat$icebergCompatVersion on a table with icebergCompatv1 enabled, " +
        s"isNewTable = $isNewTable") {
      val schema = new StructType().add("col", BooleanType.BOOLEAN)
      val metadata = getCompatEnabledMetadata(schema)
        .withMergedConfiguration(
          Map("delta.enableIcebergCompatV1" -> "true").asJava)
      val protocol = getCompatEnabledProtocol()

      val ex = intercept[KernelException] {
        validateAndUpdateIcebergCompatMetadata(isNewTable, metadata, protocol)
      }
      assert(ex.getMessage.contains(
        s"icebergCompat$icebergCompatVersion: Only one IcebergCompat version can be enabled. " +
          "Incompatible version enabled: delta.enableIcebergCompatV1"))
    }
  }
}

object IcebergCompatMetadataValidatorAndUpdaterSuiteBase {
  // Allowed simple types as data or partition columns
  val SIMPLE_TYPES: Set[DataType] = Set(
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
  val COMPLEX_TYPES: Set[DataType] = Set(
    new ArrayType(BooleanType.BOOLEAN, true),
    new MapType(IntegerType.INTEGER, LongType.LONG, true),
    new StructType().add("s1", BooleanType.BOOLEAN).add("s2", IntegerType.INTEGER))
}
