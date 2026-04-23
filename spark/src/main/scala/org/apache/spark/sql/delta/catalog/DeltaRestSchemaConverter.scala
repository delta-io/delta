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

package org.apache.spark.sql.delta.catalog

import scala.collection.JavaConverters._

import io.unitycatalog.client.delta.model

import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.spark.sql.types._

object DeltaRestSchemaConverter {

  def toSparkSchema(schema: model.StructType): StructType = {
    StructType(schema.getFields.asScala.map(toSparkField).toSeq)
  }

  private def toSparkField(field: model.StructField): StructField = {
    StructField(
      name = field.getName,
      dataType = toSparkType(field.getType),
      nullable = field.getNullable,
      metadata = toSparkMetadata(field.getMetadata))
  }

  private def toSparkType(deltaType: model.DeltaType): DataType = deltaType match {
    case primitive: model.PrimitiveType =>
      DataType.fromDDL(primitive.getType)
    case decimal: model.DecimalType =>
      org.apache.spark.sql.types.DecimalType(decimal.getPrecision, decimal.getScale)
    case array: model.ArrayType =>
      ArrayType(toSparkType(array.getElementType), array.getContainsNull)
    case map: model.MapType =>
      MapType(
        toSparkType(map.getKeyType),
        toSparkType(map.getValueType),
        map.getValueContainsNull)
    case struct: model.StructType =>
      toSparkSchema(struct)
    case unsupported =>
      throw new IllegalArgumentException(s"Unsupported Delta REST schema type: $unsupported")
  }

  private def toSparkMetadata(metadata: java.util.Map[String, Object]): Metadata = {
    if (metadata == null || metadata.isEmpty) {
      Metadata.empty
    } else {
      Metadata.fromJson(JsonUtils.toJson(metadata.asScala.toMap))
    }
  }
}
