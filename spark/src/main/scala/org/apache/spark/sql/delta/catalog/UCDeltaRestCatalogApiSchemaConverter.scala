/*
 * Copyright (2026) The Delta Lake Project Authors.
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

import java.util.{Collections, List => JList, Map => JMap}

import scala.collection.JavaConverters._

import com.fasterxml.jackson.core.`type`.TypeReference
import io.unitycatalog.client.delta.model

import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.spark.sql.types.{
  ArrayType,
  DataType,
  DecimalType,
  MapType,
  Metadata,
  MetadataBuilder,
  StructField,
  StructType
}

private[catalog] object UCDeltaRestCatalogApiSchemaConverter {
  private val MetadataMapType = new TypeReference[JMap[String, Object]] {}

  def toSparkType(schema: model.StructType): StructType = {
    StructType(schema.getFields.asScala.map(toSparkField).toSeq)
  }

  def toDeltaType(schema: StructType): model.StructType = {
    new model.StructType()
      .fields(schema.fields.map(toDeltaField).toSeq.asJava)
  }

  private def toDeltaField(field: StructField): model.StructField = {
    new model.StructField()
      .name(field.name)
      .`type`(toDeltaType(field.dataType))
      .nullable(field.nullable)
      .metadata(toDeltaMetadata(field.metadata))
  }

  private def toDeltaType(dataType: DataType): model.DeltaType = dataType match {
    case struct: StructType =>
      toDeltaType(struct)
    case ArrayType(elementType, containsNull) =>
      new model.ArrayType()
        .elementType(toDeltaType(elementType))
        .containsNull(containsNull)
    case MapType(keyType, valueType, valueContainsNull) =>
      new model.MapType()
        .keyType(toDeltaType(keyType))
        .valueType(toDeltaType(valueType))
        .valueContainsNull(valueContainsNull)
    case decimal: DecimalType =>
      new model.DecimalType()
        .precision(decimal.precision)
        .scale(decimal.scale)
    case primitive =>
      new model.PrimitiveType().`type`(primitive.typeName)
  }

  private def toDeltaMetadata(metadata: Metadata): JMap[String, Object] = {
    if (metadata == null || metadata.isEmpty) {
      Collections.emptyMap()
    } else {
      JsonUtils.mapper.readValue(metadata.json, MetadataMapType)
    }
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
      DecimalType(decimal.getPrecision, decimal.getScale)
    case array: model.ArrayType =>
      ArrayType(toSparkType(array.getElementType), array.getContainsNull)
    case map: model.MapType =>
      MapType(
        toSparkType(map.getKeyType),
        toSparkType(map.getValueType),
        map.getValueContainsNull)
    case struct: model.StructType =>
      toSparkType(struct)
    case unsupported =>
      throw new IllegalArgumentException(
        s"Unsupported UC Delta Rest Catalog API schema type: $unsupported")
  }

  private def toSparkMetadata(metadata: java.util.Map[String, Object]): Metadata = {
    if (metadata == null || metadata.isEmpty) {
      Metadata.empty
    } else {
      buildSparkMetadata(metadata)
    }
  }

  private def buildSparkMetadata(metadata: JMap[_, _]): Metadata = {
    val builder = new MetadataBuilder
    metadata.asScala.foreach {
      case (key: String, value) => putMetadataValue(builder, key, value)
      case (key, _) =>
        throw new IllegalArgumentException(
          s"Unsupported UC Delta Rest Catalog API metadata key: $key")
    }
    builder.build()
  }

  private def putMetadataValue(builder: MetadataBuilder, key: String, value: Any): Unit = {
    value match {
      case null => builder.putNull(key)
      case v: java.lang.Byte => builder.putLong(key, v.longValue)
      case v: java.lang.Short => builder.putLong(key, v.longValue)
      case v: java.lang.Integer => builder.putLong(key, v.longValue)
      case v: java.lang.Long => builder.putLong(key, v.longValue)
      case v: java.lang.Float => builder.putDouble(key, v.doubleValue)
      case v: java.lang.Double => builder.putDouble(key, v.doubleValue)
      case v: java.lang.Boolean => builder.putBoolean(key, v.booleanValue)
      case v: String => builder.putString(key, v)
      case v: JMap[_, _] => builder.putMetadata(key, buildSparkMetadata(v))
      case v: JList[_] => putMetadataArray(builder, key, v.asScala.toSeq)
      case v: Array[_] => putMetadataArray(builder, key, v.toSeq)
      case unsupported =>
        throw new IllegalArgumentException(
          s"Unsupported UC Delta Rest Catalog API metadata value for key $key: $unsupported")
    }
  }

  private def putMetadataArray(
      builder: MetadataBuilder,
      key: String,
      values: Seq[Any]): Unit = {
    if (values.forall(_.isInstanceOf[String])) {
      builder.putStringArray(key, values.map(_.asInstanceOf[String]).toArray)
    } else if (values.forall(_.isInstanceOf[java.lang.Boolean])) {
      builder.putBooleanArray(
        key,
        values.map(_.asInstanceOf[java.lang.Boolean]).map(_.booleanValue).toArray)
    } else if (values.forall(isIntegral)) {
      builder.putLongArray(key, values.map(toLong).toArray)
    } else if (values.forall(isFloatOrDouble)) {
      builder.putDoubleArray(key, values.map(toDouble).toArray)
    } else if (values.forall(_.isInstanceOf[JMap[_, _]])) {
      builder.putMetadataArray(
        key,
        values.map(v => buildSparkMetadata(v.asInstanceOf[JMap[_, _]])).toArray)
    } else {
      throw new IllegalArgumentException(
        s"Unsupported UC Delta Rest Catalog API metadata array for key $key: $values")
    }
  }

  private def isIntegral(value: Any): Boolean = value match {
    case _: java.lang.Byte | _: java.lang.Short | _: java.lang.Integer | _: java.lang.Long => true
    case _ => false
  }

  private def isFloatOrDouble(value: Any): Boolean = value match {
    case _: java.lang.Float | _: java.lang.Double => true
    case _ => false
  }

  private def toLong(value: Any): Long = value.asInstanceOf[java.lang.Number].longValue

  private def toDouble(value: Any): Double = value.asInstanceOf[java.lang.Number].doubleValue
}
