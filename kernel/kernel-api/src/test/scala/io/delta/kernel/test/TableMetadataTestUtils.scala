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
package io.delta.kernel.test
import io.delta.kernel.data.{ArrayValue, ColumnVector, MapValue}
import io.delta.kernel.engine.Engine
import io.delta.kernel.internal.actions.{Format, Metadata, Protocol}
import io.delta.kernel.internal.util.InternalUtils.singletonStringColumnVector
import io.delta.kernel.internal.util.VectorUtils.stringVector
import io.delta.kernel.types.{FieldMetadata, IntegerType, StringType, StructType, TimestampNTZType, TimestampType, VariantType}

import java.util.{Collections, Optional}
import scala.collection.JavaConverters.seqAsJavaListConverter


/**
 * Utilities to provide sample table metadata related object such as protocol, metadata, schema
 * */
trait TableMetadataTestUtils {

  def createTestProtocol(minWriterVersion: Int, writerFeatures: String*): Protocol = {
    new Protocol(
      // minReaderVersion - it doesn't matter as the read fails anyway before the writer check
      0,
      minWriterVersion,
      // reader features - it doesn't matter as the read fails anyway before the writer check
      Collections.emptyList(),
      writerFeatures.toSeq.asJava
    )
  }

  def testMetadata(
                    includeInvaraint: Boolean = false,
                    includeTimestampNtzTypeCol: Boolean = false,
                    includeVariantTypeCol: Boolean = false,
                    includeGeneratedColumn: Boolean = false,
                    includeIdentityColumn: Boolean = false,
                    tblProps: Map[String, String] = Map.empty): Metadata = {
    val testSchema = createTestSchema(
      includeInvaraint,
      includeTimestampNtzTypeCol,
      includeVariantTypeCol,
      includeGeneratedColumn,
      includeIdentityColumn)
    new Metadata(
      "id",
      Optional.of("name"),
      Optional.of("description"),
      new Format("parquet", Collections.emptyMap()),
      testSchema.toJson,
      testSchema,
      new ArrayValue() { // partitionColumns
        override def getSize = 1

        override def getElements: ColumnVector = singletonStringColumnVector("c3")
      },
      Optional.empty(),
      new MapValue() { // conf
        override def getSize = tblProps.size
        override def getKeys: ColumnVector = stringVector(tblProps.toSeq.map(_._1).asJava)
        override def getValues: ColumnVector = stringVector(tblProps.toSeq.map(_._2).asJava)
      }
    )
  }

  def createTestSchema(
                        includeInvariant: Boolean = false,
                        includeTimestampNtzTypeCol: Boolean = false,
                        includeVariantTypeCol: Boolean = false,
                        includeGeneratedColumn: Boolean = false,
                        includeIdentityColumn: Boolean = false): StructType = {
    var structType = new StructType()
      .add("c1", IntegerType.INTEGER)
      .add("c2", StringType.STRING)
    if (includeInvariant) {
      structType = structType.add(
        "c3",
        TimestampType.TIMESTAMP,
        FieldMetadata.builder()
          .putString("delta.invariants", "{\"expression\": { \"expression\": \"x > 3\"} }")
          .build())
    }
    if (includeTimestampNtzTypeCol) {
      structType = structType.add("c4", TimestampNTZType.TIMESTAMP_NTZ)
    }
    if (includeVariantTypeCol) {
      structType = structType.add("c5", VariantType.VARIANT)
    }
    if (includeGeneratedColumn) {
      structType = structType.add(
        "c6",
        IntegerType.INTEGER,
        FieldMetadata.builder()
          .putString("delta.generationExpression", "{\"expression\": \"c1 + 1\"}")
          .build())
    }
    if (includeIdentityColumn) {
      structType = structType.add(
        "c7",
        IntegerType.INTEGER,
        FieldMetadata.builder()
          .putLong("delta.identity.start", 1L)
          .putLong("delta.identity.step", 2L)
          .putBoolean("delta.identity.allowExplicitInsert", true)
          .build())
    }

    structType
  }


}
