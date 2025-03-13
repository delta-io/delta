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
package io.delta.kernel.internal.util

import java.util.{Collections, Optional}

import scala.collection.JavaConverters._

import io.delta.kernel.data.{ArrayValue, ColumnVector, MapValue}
import io.delta.kernel.internal.TableConfig
import io.delta.kernel.internal.actions.{Format, Metadata, Protocol}
import io.delta.kernel.internal.tablefeatures.TableFeature
import io.delta.kernel.internal.util.ColumnMapping.{COLUMN_MAPPING_ID_KEY, COLUMN_MAPPING_NESTED_IDS_KEY}
import io.delta.kernel.test.VectorTestUtils
import io.delta.kernel.types.{ArrayType, FieldMetadata, IntegerType, MapType, StringType, StructField, StructType}

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.util.Maps

/**
 * Common utilities for column mapping and iceberg compat v2 related nested column mapping
 * functionality
 */
trait ColumnMappingSuiteBase extends VectorTestUtils {

  /* Asserts that the given field has the expected column mapping info */
  def assertColumnMapping(
      field: StructField,
      expId: Long,
      expPhysicalName: String = "UUID"): Unit = {
    assertThat(field.getMetadata.getEntries)
      .containsEntry(ColumnMapping.COLUMN_MAPPING_ID_KEY, expId.asInstanceOf[AnyRef])
      .hasEntrySatisfying(
        ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY,
        (k: AnyRef) => {
          if (expPhysicalName == "UUID") {
            assertThat(k).asString.startsWith("col-")
          } else {
            assertThat(k).asString.isEqualTo(expPhysicalName)
          }
        })
  }

  implicit class MetadataImplicits(metadata: Metadata) {
    def withIcebergCompatV2Enabled: Metadata = {
      metadata.withMergedConfiguration(
        Maps.newHashMap(TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey, "true"))
    }

    def withColumnMappingEnabled(mode: String = "name"): Metadata = {
      metadata.withMergedConfiguration(
        Maps.newHashMap(TableConfig.COLUMN_MAPPING_MODE.getKey, mode))
    }

    def withIcebergCompatV2AndCMEnabled(): Metadata = {
      metadata.withIcebergCompatV2Enabled.withColumnMappingEnabled()
    }
  }

  def createMetadataWithFieldId(fieldId: Int): FieldMetadata = {
    FieldMetadata.builder.putLong(COLUMN_MAPPING_ID_KEY, fieldId).build()
  }

  /** Test schema containing various different types that test the nested column mapping info */
  def cmTestSchema(): StructType = {
    new StructType()
      .add("a", StringType.STRING)
      .add(
        "b",
        new MapType(
          IntegerType.INTEGER,
          new StructType()
            .add("d", IntegerType.INTEGER)
            .add("e", IntegerType.INTEGER)
            .add(
              "f",
              new ArrayType(
                new StructType()
                  .add("g", IntegerType.INTEGER)
                  .add("h", IntegerType.INTEGER),
                false),
              false),
          false))
      .add("c", IntegerType.INTEGER)
  }

  /**
   * Verify the schema returned by [[cmTestSchema()]] has correct column mapping (including nested)
   * info assigned
   */
  def verifyCMTestSchemaHasValidColumnMappingInfo(
      metadata: Metadata,
      isNewTable: Boolean = true,
      enableIcebergComaptV2: Boolean = true): Unit = {
    var fieldId: Long = 0L

    def nextFieldId: Long = {
      fieldId += 1
      fieldId
    }

    assertColumnMapping(metadata.getSchema.get("a"), nextFieldId, if (isNewTable) "UUID" else "a")
    assertColumnMapping(metadata.getSchema.get("b"), nextFieldId, if (isNewTable) "UUID" else "b")
    val mapType = metadata.getSchema.get("b").getDataType.asInstanceOf[MapType]
    val innerStruct = mapType.getValueField.getDataType.asInstanceOf[StructType]
    assertColumnMapping(innerStruct.get("d"), nextFieldId, if (isNewTable) "UUID" else "d")
    assertColumnMapping(innerStruct.get("e"), nextFieldId, if (isNewTable) "UUID" else "e")
    assertColumnMapping(innerStruct.get("f"), nextFieldId, if (isNewTable) "UUID" else "f")
    val innerArray = innerStruct.get("f").getDataType.asInstanceOf[ArrayType]
    val structInArray = innerArray.getElementField.getDataType.asInstanceOf[StructType]
    assertColumnMapping(structInArray.get("g"), nextFieldId, if (isNewTable) "UUID" else "g")
    assertColumnMapping(structInArray.get("h"), nextFieldId, if (isNewTable) "UUID" else "h")
    assertColumnMapping(metadata.getSchema.get("c"), nextFieldId, if (isNewTable) "UUID" else "c")

    // verify nested ids
    if (enableIcebergComaptV2) {
      assertThat(metadata.getSchema.get("b").getMetadata.getEntries
        .get(COLUMN_MAPPING_NESTED_IDS_KEY).asInstanceOf[FieldMetadata].getEntries)
        .hasSize(2)
        .anySatisfy((k: AnyRef, v: AnyRef) => {
          assertThat(k).asString.startsWith(if (isNewTable) "col-" else "b")
          assertThat(k).asString.endsWith(".key")
          assertThat(v).isEqualTo(nextFieldId)
        })
        .anySatisfy((k: AnyRef, v: AnyRef) => {
          assertThat(k).asString.startsWith(if (isNewTable) "col-" else "b")
          assertThat(k).asString.endsWith(".value")
          assertThat(v).isEqualTo(nextFieldId)
        })

      assertThat(mapType.getKeyField.getMetadata.getEntries)
        .doesNotContainKey(ColumnMapping.COLUMN_MAPPING_ID_KEY)
        .doesNotContainKey(ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY)

      assertThat(mapType.getValueField.getMetadata.getEntries)
        .doesNotContainKey(ColumnMapping.COLUMN_MAPPING_ID_KEY)
        .doesNotContainKey(ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY)

      // verify nested ids
      assertThat(innerStruct.get("f").getMetadata.getEntries
        .get(COLUMN_MAPPING_NESTED_IDS_KEY).asInstanceOf[FieldMetadata].getEntries)
        .hasSize(1)
        .anySatisfy((k: AnyRef, v: AnyRef) => {
          assertThat(k).asString.startsWith(if (isNewTable) "col-" else "f")
          assertThat(k).asString.endsWith(".element")
          assertThat(v).isEqualTo(nextFieldId)
        })
    }

    assertThat(metadata.getConfiguration)
      .containsEntry(ColumnMapping.COLUMN_MAPPING_MAX_COLUMN_ID_KEY, fieldId.toString)
  }

  /** create test metadata object */
  def testMetadata(
      schema: StructType,
      partitionCols: Seq[String] = Seq.empty,
      tblProps: Map[String, String] = Map.empty): Metadata = {
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
