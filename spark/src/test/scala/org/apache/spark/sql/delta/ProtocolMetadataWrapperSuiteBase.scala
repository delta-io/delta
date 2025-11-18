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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, MetadataBuilder, StringType, StructField, StructType}

/**
 * Abstract base class for testing ProtocolMetadataWrapper implementations.
 */
abstract class ProtocolMetadataWrapperSuiteBase
  extends SparkFunSuite
  with SharedSparkSession {

  /**
   * Creates a wrapper instance based on different parameters.
   *
   * @param minReaderVersion Protocol reader version
   * @param minWriterVersion Protocol writer version
   * @param readerFeatures Optional set of reader features
   * @param writerFeatures Optional set of writer features
   * @param schema Table schema
   * @param configuration Table properties/configuration
   */
  protected def createWrapper(
      minReaderVersion: Int = 1,
      minWriterVersion: Int = 2,
      readerFeatures: Option[Set[String]] = None,
      writerFeatures: Option[Set[String]] = None,
      schema: StructType = new StructType().add("id", IntegerType),
      configuration: Map[String, String] = Map.empty): ProtocolMetadataWrapper

  Seq[(DeltaColumnMappingMode, Map[String, String])](
    (NoMapping, Map.empty),
    (NameMapping, Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> "name")),
    (IdMapping, Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> "id"))
  ).foreach { case (expectedMode, config) =>
    test(s"columnMappingMode with $expectedMode") {
      val wrapper = createWrapper(configuration = config)
      assert(wrapper.columnMappingMode === expectedMode)
    }
  }

  Seq[(String, StructType)](
    // Empty schema: table with no columns
    ("empty schema", new StructType()),
    // Simple schema: flat structure with primitive types
    ("simple schema", new StructType().add("id", IntegerType).add("name", StringType)),
    // Nested schema: struct within struct
    ("nested schema", new StructType()
      .add("user", new StructType()
        .add("id", IntegerType)
        .add("name", StringType))
      .add("timestamp", LongType))
  ).foreach { case (testCaseName, schema) =>
    test(s"getReferenceSchema with $testCaseName") {
      val wrapper = createWrapper(schema = schema)
      assert(wrapper.getReferenceSchema === schema)
    }
  }

  Seq[(String, Boolean, Map[String, String], Option[Set[String]], Option[Set[String]])](
    // Row tracking enabled by setting table features
    ("enabled", true /* expectedRowIdEnabled */,
      Map(DeltaConfigs.ROW_TRACKING_ENABLED.key -> "true"),
      Some(Set(RowTrackingFeature.name)), Some(Set(RowTrackingFeature.name))),
    // Row tracking disabled by default
    ("disabled", false /* expectedRowIdEnabled */, Map.empty, None /* readerFeatures */,
      None /* writerFeatures */),
    // Row tracking explicitly disabled via config
    ("explicitly disabled", false /* expectedRowIdEnabled */,
      Map(DeltaConfigs.ROW_TRACKING_ENABLED.key -> "false"),
      None /* readerFeatures */, None /* writerFeatures */)
  ).foreach { case (testCaseName, expectedRowIdEnabled, config, readerFeatures, writerFeatures) =>
    test(s"isRowIdEnabled when $testCaseName") {
      val wrapper = createWrapper(
        minReaderVersion = if (readerFeatures.isDefined) 3 else 1,
        minWriterVersion = if (writerFeatures.isDefined) 7 else 2,
        readerFeatures = readerFeatures,
        writerFeatures = writerFeatures,
        configuration = config)
      assert(wrapper.isRowIdEnabled === expectedRowIdEnabled)
    }
  }

  Seq[(String, Boolean, Option[Set[String]], Option[Set[String]])](
    // Deletion vectors enabled via table features
    ("enabled", true /* expectedDeletionVectorReadable */,
      Some(Set(DeletionVectorsTableFeature.name)),
      Some(Set(DeletionVectorsTableFeature.name))),
    // Deletion vectors disabled by default
    ("disabled", false /* expectedDeletionVectorReadable */, None /* readerFeatures */,
      None /* writerFeatures */)
  ).foreach { case (testCaseName, expectedDeletionVectorReadable, readerFeatures, writerFeatures) =>
    test(s"isDeletionVectorReadable when $testCaseName") {
      val wrapper = createWrapper(
        minReaderVersion = if (readerFeatures.isDefined) 3 else 1,
        minWriterVersion = if (writerFeatures.isDefined) 7 else 2,
        readerFeatures = readerFeatures,
        writerFeatures = writerFeatures)
      assert(wrapper.isDeletionVectorReadable === expectedDeletionVectorReadable)
    }
  }

  Seq[(String, Boolean, Map[String, String])](
    // IcebergCompat V1 enabled
    ("v1 enabled", true /* expectedIcebergCompatEnabled */,
      Map(DeltaConfigs.ICEBERG_COMPAT_V1_ENABLED.key -> "true")),
    // IcebergCompat V2 enabled
    ("v2 enabled", true /* expectedIcebergCompatEnabled */,
      Map(DeltaConfigs.ICEBERG_COMPAT_V2_ENABLED.key -> "true")),
    // IcebergCompat V3 enabled
    ("v3 enabled", true /* expectedIcebergCompatEnabled */,
    // No IcebergCompat enabled
    ("disabled", false /* expectedIcebergCompatEnabled */, Map.empty)
  ).foreach { case (testCaseName, expectedIcebergCompatEnabled, config) =>
    test(s"isIcebergCompatAnyEnabled when $testCaseName") {
      val wrapper = createWrapper(
        configuration = config)

      assert(wrapper.isIcebergCompatAnyEnabled === expectedIcebergCompatEnabled)
    }
  }

  Seq[(String, Map[String, String], Seq[(Int, Boolean)])](
    // IcebergCompat V1 enabled: only version 1 should return true
    ("v1 enabled", Map(DeltaConfigs.ICEBERG_COMPAT_V1_ENABLED.key -> "true"),
      Seq[(Int, Boolean)]((1 /* version */, true /* expectedEnabled */), (2, false), (3, false))),
    // V2 enabled: version 1 and 2 should return true
    ("v2 enabled", Map(DeltaConfigs.ICEBERG_COMPAT_V2_ENABLED.key -> "true"),
      Seq[(Int, Boolean)]((1 /* version */, true /* expectedEnabled */), (2, true), (3, false))),
    // No version enabled: all versions should return false
    ("disabled", Map.empty,
      Seq[(Int, Boolean)]((1 /* version */, false /* expectedEnabled */), (2, false), (3, false)))
  ).foreach { case (testCaseName, config, versionChecks) =>
    test(s"isIcebergCompatGeqEnabled when $testCaseName") {
      val wrapper = createWrapper(
        configuration = config)

      versionChecks.foreach { case (version, expectedEnabled) =>
        assert(wrapper.isIcebergCompatGeqEnabled(version) === expectedEnabled,
          s"version $version check failed")
      }
    }
  }

  Seq[(String, Option[org.apache.spark.sql.types.Metadata], Boolean, Map[String, String],
    Option[Set[String]], Option[Set[String]])](
    // Table with no special features should be readable
    ("readable table", None /* typeChangeMetadata */, true /* tableReadable */, Map.empty,
      None /* readerFeatures */, None /* writerFeatures */),
    // Table with unsupported type widening (string -> integer) should not be readable
    ("table with unsupported type widening",
      Some(new MetadataBuilder()
        .putMetadataArray("delta.typeChanges", Array(
          new MetadataBuilder()
            .putLong("tableVersion", 1L)
            .putString("fromType", "string")
            .putString("toType", "integer")
            .build()
        ))
        .build()),
      false /* tableReadable */,
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

      val wrapper = createWrapper(
        minReaderVersion = if (readerFeatures.isDefined) 3 else 1,
        minWriterVersion = if (writerFeatures.isDefined) 7 else 2,
        readerFeatures = readerFeatures,
        writerFeatures = writerFeatures,
        schema = schema,
        configuration = config)

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

  Seq[(String, Map[String, String], Option[Set[String]], Option[Set[String]],
    Boolean, Boolean)](
    // Row tracking disabled: should return no fields
    ("row tracking disabled", Map.empty, None /* readerFeatures */, None /* writerFeatures */,
      false /* nullableConstant */, false /* nullableGenerated */),
    // Row tracking enabled with materialized columns
    ("row tracking enabled, nullable constant=false, generated=false",
      Map(
        DeltaConfigs.ROW_TRACKING_ENABLED.key -> "true",
        "delta.rowTracking.materializedRowIdColumnName" -> "_row_id_col",
        "delta.rowTracking.materializedRowCommitVersionColumnName" -> "_row_commit_version_col"),
      Some(Set(RowTrackingFeature.name)), Some(Set(RowTrackingFeature.name)),
      false /* nullableConstant */, false /* nullableGenerated */),
    ("row tracking enabled, nullable constant=true, generated=true",
      Map(
        DeltaConfigs.ROW_TRACKING_ENABLED.key -> "true",
        "delta.rowTracking.materializedRowIdColumnName" -> "_row_id_col",
        "delta.rowTracking.materializedRowCommitVersionColumnName" -> "_row_commit_version_col"),
      Some(Set(RowTrackingFeature.name)), Some(Set(RowTrackingFeature.name)),
      true /* nullableConstant */, true /* nullableGenerated */)
  ).foreach { case (testCaseName, config, readerFeatures, writerFeatures,
    nullableConstant, nullableGenerated) =>
    test(s"createRowTrackingMetadataFields when $testCaseName") {
      val wrapper = createWrapper(
        minReaderVersion = if (readerFeatures.isDefined) 3 else 1,
        minWriterVersion = if (writerFeatures.isDefined) 7 else 2,
        readerFeatures = readerFeatures,
        writerFeatures = writerFeatures,
        configuration = config)

      val fields = wrapper.createRowTrackingMetadataFields(
        nullableConstant, nullableGenerated).toSeq

      val expectedFields =
        if (!config.get(DeltaConfigs.ROW_TRACKING_ENABLED.key).contains("true")) {
          // Row tracking disabled: no fields
          Seq.empty[StructField]
        } else {
          Seq[StructField](
            StructField("row_id", LongType, nullableGenerated),
            StructField("base_row_id", LongType, nullableConstant),
            StructField("default_row_commit_version", LongType, nullableConstant),
            StructField("row_commit_version", LongType, nullableGenerated))
        }

      val actualSimplified = fields.map(f => StructField(f.name, f.dataType, f.nullable))
      assert(actualSimplified === expectedFields)
    }
  }

}

