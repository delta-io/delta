/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta

import org.apache.spark.sql.delta.DeltaTestUtils.BOOLEAN_DOMAIN
import org.apache.spark.sql.delta.actions.{Metadata, Protocol}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, MetadataBuilder, StringType, StructField, StructType}

/**
 * Unit tests for DeltaProtocolMetadataWrapperV1.
 *
 * Tests that the wrapper correctly delegates to Protocol and Metadata methods.
 */
class DeltaProtocolMetadataWrapperV1Suite
  extends SparkFunSuite
  with SharedSparkSession {

  private def createTestMetadata(
      schema: StructType = new StructType().add("id", IntegerType),
      configuration: Map[String, String] = Map.empty): Metadata = {
    Metadata(
      schemaString = schema.json,
      configuration = configuration)
  }

  private def createTestProtocol(
      minReaderVersion: Int = 1,
      minWriterVersion: Int = 2,
      readerFeatures: Option[Set[String]] = None,
      writerFeatures: Option[Set[String]] = None): Protocol = {
    Protocol(
      minReaderVersion = minReaderVersion,
      minWriterVersion = minWriterVersion,
      readerFeatures = readerFeatures,
      writerFeatures = writerFeatures)
  }

  Seq[(DeltaColumnMappingMode, Map[String, String])](
    (NoMapping, Map.empty),
    (NameMapping, Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> "name")),
    (IdMapping, Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> "id"))
  ).foreach { case (expectedMode, config) =>
    test(s"columnMappingMode with $expectedMode") {
      val metadata = createTestMetadata(configuration = config)
      val protocol = createTestProtocol()

      val wrapper = DeltaProtocolMetadataWrapperV1(protocol, metadata)

      assert(wrapper.columnMappingMode === expectedMode)
    }
  }

  Seq(
    ("empty schema", new StructType()),
    ("simple schema", new StructType().add("id", IntegerType).add("name", StringType)),
    ("nested schema", new StructType()
      .add("user", new StructType()
        .add("id", IntegerType)
        .add("name", StringType))
      .add("timestamp", LongType))
  ).foreach { case (testCaseName, schema) =>
    test(s"getReferenceSchema with $testCaseName") {
      val metadata = createTestMetadata(schema = schema)
      val protocol = createTestProtocol()

      val wrapper = DeltaProtocolMetadataWrapperV1(protocol, metadata)

      assert(wrapper.getReferenceSchema === schema)
    }
  }

  Seq[(String, Boolean, Map[String, String], Option[Set[String]], Option[Set[String]])](
    ("enabled", true, Map(DeltaConfigs.ROW_TRACKING_ENABLED.key -> "true"),
      Some(Set(RowTrackingFeature.name)), Some(Set(RowTrackingFeature.name))),
    ("disabled", false, Map.empty, None, None),
    ("explicitly disabled", false, Map(DeltaConfigs.ROW_TRACKING_ENABLED.key -> "false"),
      None, None)
  ).foreach { case (testCaseName, expected, config, readerFeatures, writerFeatures) =>
    test(s"isRowIdEnabled when $testCaseName") {
      val metadata = createTestMetadata(configuration = config)
      val protocol = createTestProtocol(
        minReaderVersion = if (readerFeatures.isDefined) 3 else 1,
        minWriterVersion = if (writerFeatures.isDefined) 7 else 2,
        readerFeatures = readerFeatures,
        writerFeatures = writerFeatures)

      val wrapper = DeltaProtocolMetadataWrapperV1(protocol, metadata)

      assert(wrapper.isRowIdEnabled === expected)
    }
  }

  Seq[(String, Boolean, Option[Set[String]], Option[Set[String]])](
    ("enabled", true, Some(Set(DeletionVectorsTableFeature.name)),
      Some(Set(DeletionVectorsTableFeature.name))),
    ("disabled", false, None, None)
  ).foreach { case (testCaseName, expected, readerFeatures, writerFeatures) =>
    test(s"isDeletionVectorReadable when $testCaseName") {
      val metadata = createTestMetadata()
      val protocol = createTestProtocol(
        minReaderVersion = if (readerFeatures.isDefined) 3 else 1,
        minWriterVersion = if (writerFeatures.isDefined) 7 else 2,
        readerFeatures = readerFeatures,
        writerFeatures = writerFeatures)

      val wrapper = DeltaProtocolMetadataWrapperV1(protocol, metadata)

      assert(wrapper.isDeletionVectorReadable === expected)
    }
  }

  Seq[(String, Boolean, Map[String, String])](
    ("v1 enabled", true, Map(DeltaConfigs.ICEBERG_COMPAT_V1_ENABLED.key -> "true")),
    ("v2 enabled", true, Map(DeltaConfigs.ICEBERG_COMPAT_V2_ENABLED.key -> "true")),
    ("v3 enabled", true, Map(DeltaConfigs.ICEBERG_COMPAT_V3_ENABLED.key -> "true")),
    ("disabled", false, Map.empty)
  ).foreach { case (testCaseName, expected, config) =>
    test(s"isIcebergCompatAnyEnabled when $testCaseName") {
      val metadata = createTestMetadata(configuration = config)
      val protocol = createTestProtocol()

      val wrapper = DeltaProtocolMetadataWrapperV1(protocol, metadata)

      assert(wrapper.isIcebergCompatAnyEnabled === expected)
    }
  }

  Seq[(String, Map[String, String], Seq[(Int, Boolean)])](
    ("v1 enabled", Map(DeltaConfigs.ICEBERG_COMPAT_V1_ENABLED.key -> "true"),
      Seq((1, true), (2, false), (3, false))),
    ("v2 enabled", Map(DeltaConfigs.ICEBERG_COMPAT_V2_ENABLED.key -> "true"),
      Seq((1, true), (2, true), (3, false))),
    ("v3 enabled", Map(DeltaConfigs.ICEBERG_COMPAT_V3_ENABLED.key -> "true"),
      Seq((1, true), (2, true), (3, true))),
    ("disabled", Map.empty, Seq((1, false), (2, false), (3, false)))
  ).foreach { case (testCaseName, config, versionChecks) =>
    test(s"isIcebergCompatGeqEnabled when $testCaseName") {
      val metadata = createTestMetadata(configuration = config)
      val protocol = createTestProtocol()

      val wrapper = DeltaProtocolMetadataWrapperV1(protocol, metadata)

      versionChecks.foreach { case (version, expected) =>
        assert(wrapper.isIcebergCompatGeqEnabled(version) === expected,
          s"version $version check failed")
      }
    }
  }

  Seq[(String, Option[org.apache.spark.sql.types.Metadata], Boolean, Map[String, String],
    Option[Set[String]], Option[Set[String]])](
    ("readable table", None, true, Map.empty, None, None),
    ("table with unsupported type widening",
      // Unsupported type change: string -> integer
      Some(new MetadataBuilder()
        .putMetadataArray("delta.typeChanges", Array(
          new MetadataBuilder()
            .putLong("tableVersion", 1L)
            .putString("fromType", "string")
            .putString("toType", "integer")
            .build()
        ))
        .build()),
      false,
      Map(DeltaConfigs.ENABLE_TYPE_WIDENING.key -> "true"),
      Some(Set(TypeWideningTableFeature.name)),
      Some(Set(TypeWideningTableFeature.name)))
  ).foreach { case (testCaseName, typeChangeMetadata, tableReadable, config,
    readerFeatures, writerFeatures) =>
    test(s"assertTableReadable with $testCaseName") {
      val schema = typeChangeMetadata match {
        case Some(metadata) =>
          new StructType().add("col1", IntegerType, nullable = true, metadata = metadata)
        case None =>
          new StructType().add("id", IntegerType)
      }

      val metadata = createTestMetadata(schema = schema, configuration = config)
      val protocol = createTestProtocol(
        minReaderVersion = if (readerFeatures.isDefined) 3 else 1,
        minWriterVersion = if (writerFeatures.isDefined) 7 else 2,
        readerFeatures = readerFeatures,
        writerFeatures = writerFeatures
      )

      val wrapper = DeltaProtocolMetadataWrapperV1(protocol, metadata)

      if (tableReadable) {
        // Should not throw
        wrapper.assertTableReadable(spark)
      } else {
        // Should throw exception
        intercept[Exception] {
          wrapper.assertTableReadable(spark)
        }
      }
    }
  }

  Seq[(String, Boolean, Map[String, String], Option[Set[String]], Option[Set[String]],
    Boolean, Boolean)](
    ("row tracking disabled", false, Map.empty, None, None, false, false),
    ("row tracking enabled - nullable constant=false, generated=false", true,
      Map(DeltaConfigs.ROW_TRACKING_ENABLED.key -> "true"),
      Some(Set(RowTrackingFeature.name)), Some(Set(RowTrackingFeature.name)),
      false, false),
    ("row tracking enabled - nullable constant=true, generated=false", true,
      Map(DeltaConfigs.ROW_TRACKING_ENABLED.key -> "true"),
      Some(Set(RowTrackingFeature.name)), Some(Set(RowTrackingFeature.name)),
      true, false),
    ("row tracking enabled - nullable constant=false, generated=true", true,
      Map(DeltaConfigs.ROW_TRACKING_ENABLED.key -> "true"),
      Some(Set(RowTrackingFeature.name)), Some(Set(RowTrackingFeature.name)),
      false, true),
    ("row tracking enabled - nullable constant=true, generated=true", true,
      Map(DeltaConfigs.ROW_TRACKING_ENABLED.key -> "true"),
      Some(Set(RowTrackingFeature.name)), Some(Set(RowTrackingFeature.name)),
      true, true)
  ).foreach { case (testCaseName, isRowTrackingEnabled, config, readerFeatures, writerFeatures,
    nullableRowTrackingConstantFields, nullableRowTrackingGeneratedFields) =>
    test(s"createRowTrackingMetadataFields when $testCaseName") {
      val metadata = createTestMetadata(configuration = config)
      val protocol = createTestProtocol(
        minReaderVersion = if (readerFeatures.isDefined) 3 else 1,
        minWriterVersion = if (writerFeatures.isDefined) 7 else 2,
        readerFeatures = readerFeatures,
        writerFeatures = writerFeatures)

      val wrapper = DeltaProtocolMetadataWrapperV1(protocol, metadata)
      val fields = wrapper.createRowTrackingMetadataFields(
        nullableRowTrackingConstantFields, nullableRowTrackingGeneratedFields)

      if (!isRowTrackingEnabled) {
        // When disabled, should return empty
        assert(fields.isEmpty)
      } else {
        // Manually construct expected fields
        val shouldSetIcebergReservedFieldId = IcebergWriterCompat.isGeqEnabled(metadata, 3)

        val expectedFields =
          RowId.createRowIdField(
            protocol, metadata, nullableRowTrackingGeneratedFields,
            shouldSetIcebergReservedFieldId) ++
          RowId.createBaseRowIdField(protocol, metadata, nullableRowTrackingConstantFields) ++
          DefaultRowCommitVersion.createDefaultRowCommitVersionField(
            protocol, metadata, nullableRowTrackingConstantFields) ++
          RowCommitVersion.createMetadataStructField(
            protocol, metadata, nullableRowTrackingGeneratedFields,
            shouldSetIcebergReservedFieldId)

        assert(fields.toSeq === expectedFields.toSeq)
      }
    }
  }

}
