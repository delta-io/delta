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

import io.unitycatalog.client.delta.model
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.types.{
  ArrayType,
  BinaryType,
  BooleanType,
  ByteType,
  DateType,
  DecimalType,
  DoubleType,
  FloatType,
  IntegerType,
  LongType,
  MapType,
  NullType,
  ShortType,
  StringType,
  StructField,
  StructType,
  TimestampNTZType,
  TimestampType
}

class UCDeltaRestCatalogApiSchemaConverterSuite extends AnyFunSuite {

  test("converts Delta primitive type names to Spark types") {
    Seq(
      "string" -> StringType,
      "long" -> LongType,
      "integer" -> IntegerType,
      "short" -> ShortType,
      "byte" -> ByteType,
      "boolean" -> BooleanType,
      "float" -> FloatType,
      "double" -> DoubleType,
      "binary" -> BinaryType,
      "date" -> DateType,
      "timestamp" -> TimestampType,
      "timestamp_ntz" -> TimestampNTZType,
      "void" -> NullType).foreach { case (deltaType, sparkType) =>
        val schema = UCDeltaRestCatalogApiSchemaConverter.toSparkType(
          deltaSchema(deltaField(deltaType, primitive(deltaType))))
        assert(schema(deltaType).dataType === sparkType)

        val deltaSchemaFromSpark = UCDeltaRestCatalogApiSchemaConverter.toDeltaType(
          StructType(Seq(StructField(deltaType, sparkType))))
        val deltaPrimitive =
          deltaSchemaFromSpark.getFields.get(0).getType.asInstanceOf[model.PrimitiveType]
        assert(deltaPrimitive.getType === deltaType)
        assert(UCDeltaRestCatalogApiSchemaConverter
          .toSparkType(deltaSchemaFromSpark)(deltaType).dataType === sparkType)
    }
  }

  test("converts decimal precision and scale in both directions") {
    val sparkSchema = StructType(Seq(StructField("amount", DecimalType(18, 4), nullable = false)))

    val deltaSchema = UCDeltaRestCatalogApiSchemaConverter.toDeltaType(sparkSchema)
    val deltaDecimal = deltaSchema.getFields.get(0).getType.asInstanceOf[model.DecimalType]

    assert(deltaDecimal.getPrecision === 18)
    assert(deltaDecimal.getScale === 4)
    assert(UCDeltaRestCatalogApiSchemaConverter.toSparkType(deltaSchema) === sparkSchema)
  }

  test("round-trips primitive, decimal, array, map, and nested struct types") {
    val nested = StructType(Seq(
      StructField("nested_long", LongType, nullable = false),
      StructField("nested_decimal", DecimalType(8, 2), nullable = true)))
    val sparkSchema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("amount", DecimalType(10, 3), nullable = true),
      StructField("items", ArrayType(nested, containsNull = false), nullable = true),
      StructField("attributes", MapType(StringType, DoubleType, valueContainsNull = false))))

    val roundTripped = UCDeltaRestCatalogApiSchemaConverter.toSparkType(
      UCDeltaRestCatalogApiSchemaConverter.toDeltaType(sparkSchema))

    assert(roundTripped === sparkSchema)
  }

  test("converts metadata arrays") {
    val metadata = metadataMap(
      "strings" -> jList("a", "b"),
      "booleans" -> jList(Boolean.box(true), Boolean.box(false)),
      "longs" -> jList(Int.box(1), Long.box(2L)),
      "doubles" -> jList(Float.box(1.5f), Double.box(2.5d)),
      "metadata" -> jList(metadataMap("nested" -> "value")))

    val sparkMetadata = UCDeltaRestCatalogApiSchemaConverter.toSparkType(
      deltaSchema(deltaField("id", primitive("long"), metadata)))
      .apply("id")
      .metadata

    assert(sparkMetadata.getStringArray("strings").toSeq === Seq("a", "b"))
    assert(sparkMetadata.getBooleanArray("booleans").toSeq === Seq(true, false))
    assert(sparkMetadata.getLongArray("longs").toSeq === Seq(1L, 2L))
    assert(sparkMetadata.getDoubleArray("doubles").toSeq === Seq(1.5d, 2.5d))
    assert(sparkMetadata.getMetadataArray("metadata").map(_.getString("nested")).toSeq ===
      Seq("value"))
  }

  test("rejects mixed non-numeric metadata arrays") {
    val metadata = metadataMap("mixed" -> jList("a", Int.box(1)))

    val error = intercept[IllegalArgumentException] {
      UCDeltaRestCatalogApiSchemaConverter.toSparkType(
        deltaSchema(deltaField("id", primitive("long"), metadata)))
    }

    assert(error.getMessage.contains(
      "Unsupported UC Delta Rest Catalog API metadata array for key mixed"))
  }

  test("converts scalar metadata values") {
    val metadata = metadataMap(
      "byte" -> Byte.box(1.toByte),
      "short" -> Short.box(2.toShort),
      "int" -> Int.box(3),
      "long" -> Long.box(4L),
      "float" -> Float.box(1.5f),
      "double" -> Double.box(2.5d),
      "boolean" -> Boolean.box(true),
      "string" -> "value",
      "map" -> metadataMap("nested" -> "value"))

    val sparkMetadata = UCDeltaRestCatalogApiSchemaConverter.toSparkType(
      deltaSchema(deltaField("id", primitive("long"), metadata)))
      .apply("id")
      .metadata

    assert(sparkMetadata.getLong("byte") === 1L)
    assert(sparkMetadata.getLong("short") === 2L)
    assert(sparkMetadata.getLong("int") === 3L)
    assert(sparkMetadata.getLong("long") === 4L)
    assert(sparkMetadata.getDouble("float") === 1.5d)
    assert(sparkMetadata.getDouble("double") === 2.5d)
    assert(sparkMetadata.getBoolean("boolean"))
    assert(sparkMetadata.getString("string") === "value")
    assert(sparkMetadata.getMetadata("map").getString("nested") === "value")
  }

  private def primitive(name: String): model.PrimitiveType = {
    new model.PrimitiveType().`type`(name)
  }

  private def deltaField(
      name: String,
      dataType: model.DeltaType,
      metadata: JMap[String, Object] = Collections.emptyMap()): model.StructField = {
    new model.StructField()
      .name(name)
      .`type`(dataType)
      .nullable(true)
      .metadata(metadata)
  }

  private def deltaSchema(fields: model.StructField*): model.StructType = {
    new model.StructType().fields(fields.asJava)
  }

  private def metadataMap(values: (String, Object)*): JMap[String, Object] = {
    values.toMap.asJava
  }

  private def jList(values: Object*): JList[Object] = {
    values.asJava
  }
}
