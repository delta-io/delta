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

package org.apache.spark.sql.delta.uccatalog

import java.util.{Collections => JColl, List => JList}

import scala.collection.JavaConverters._

import io.unitycatalog.client.delta.model.{
  ArrayType => UCArrayType,
  DecimalType => UCDecimalType,
  MapType => UCMapType,
  PrimitiveType => UCPrimitiveType,
  StructField => UCStructField,
  StructType => UCStructType
}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.types._

class DeltaRestSchemaConverterSuite extends AnyFunSuite {
  import DeltaRestSchemaConverterSuite._

  // ---- Read path ----

  test("toSparkSchema: empty/null column list yields an empty StructType") {
    assert(DeltaRestSchemaConverter.toSparkSchema(null) === new StructType())
    assert(DeltaRestSchemaConverter.toSparkSchema(JColl.emptyList()) === new StructType())
  }

  test("toSparkSchema: all supported primitive type identifiers") {
    val cases = Seq(
      "boolean" -> BooleanType,
      "tinyint" -> ByteType,
      "byte" -> ByteType,
      "smallint" -> ShortType,
      "short" -> ShortType,
      "int" -> IntegerType,
      "integer" -> IntegerType,
      "long" -> LongType,
      "bigint" -> LongType,
      "float" -> FloatType,
      "double" -> DoubleType,
      "string" -> StringType,
      "binary" -> BinaryType,
      "date" -> DateType,
      "timestamp" -> TimestampType,
      "timestamp_ntz" -> TimestampNTZType,
      "variant" -> VariantType
    )
    cases.foreach { case (name, expected) =>
      val schema = DeltaRestSchemaConverter.toSparkSchema(
        JColl.singletonList(field("c", prim(name), nullable = true)))
      assert(schema.fields.head.dataType === expected, s"primitive $name")
    }
  }

  test("toSparkSchema: decimal POJO preserves precision and scale") {
    val d = new UCDecimalType().precision(38).scale(18)
    d.setType("decimal")
    val schema = DeltaRestSchemaConverter.toSparkSchema(
      JColl.singletonList(field("d", d, nullable = false)))
    assert(schema.fields.head.dataType === DecimalType(38, 18))
    assert(!schema.fields.head.nullable)
  }

  test("toSparkSchema: bare 'decimal(p,s)' primitive string parses defensively") {
    val schema = DeltaRestSchemaConverter.toSparkSchema(
      JColl.singletonList(field("d", prim("decimal(10,2)"), nullable = true)))
    assert(schema.fields.head.dataType === DecimalType(10, 2))
  }

  test("toSparkSchema: array preserves contains-null") {
    val a = new UCArrayType().elementType(prim("long")).containsNull(false)
    a.setType("array")
    val schema = DeltaRestSchemaConverter.toSparkSchema(
      JColl.singletonList(field("a", a, nullable = true)))
    assert(schema.fields.head.dataType === ArrayType(LongType, containsNull = false))
  }

  test("toSparkSchema: map preserves value-contains-null") {
    val m = new UCMapType()
      .keyType(prim("string"))
      .valueType(prim("int"))
      .valueContainsNull(false)
    m.setType("map")
    val schema = DeltaRestSchemaConverter.toSparkSchema(
      JColl.singletonList(field("m", m, nullable = true)))
    assert(schema.fields.head.dataType ===
      MapType(StringType, IntegerType, valueContainsNull = false))
  }

  test("toSparkSchema: nested struct preserves child field metadata") {
    val c1 = field("c1", prim("long"), nullable = true,
      metadata = Map(
        "comment" -> "first-col",
        "delta.columnMapping.id" -> java.lang.Long.valueOf(7L)))
    val dec = new UCDecimalType().precision(10).scale(2)
    dec.setType("decimal")
    val c2 = field("c2", dec, nullable = false)
    val inner = new UCStructType()
    inner.setFields(jlist(c1, c2))
    inner.setType("struct")

    val outer = field("nested", inner, nullable = true,
      metadata = Map("delta.generationExpression" -> "cast(c1 as int)"))

    val schema = DeltaRestSchemaConverter.toSparkSchema(JColl.singletonList(outer))
    val top = schema.fields.head
    assert(top.name === "nested")
    assert(top.nullable)
    assert(top.metadata.getString("delta.generationExpression") === "cast(c1 as int)")

    val innerSt = top.dataType.asInstanceOf[StructType]
    assert(innerSt.fields.length === 2)
    assert(innerSt.fields(0).name === "c1")
    assert(innerSt.fields(0).dataType === LongType)
    assert(innerSt.fields(0).metadata.getString("comment") === "first-col")
    assert(innerSt.fields(0).metadata.getLong("delta.columnMapping.id") === 7L)
    assert(innerSt.fields(1).dataType === DecimalType(10, 2))
    assert(!innerSt.fields(1).nullable)
  }

  test("toSparkSchema: deeply nested array-of-map-of-decimal round-trips case-correctly") {
    val dec = new UCDecimalType().precision(38).scale(18)
    dec.setType("decimal")
    val m = new UCMapType()
      .keyType(prim("string"))
      .valueType(dec)
      .valueContainsNull(false)
    m.setType("map")
    val a = new UCArrayType().elementType(m).containsNull(false)
    a.setType("array")

    val schema = DeltaRestSchemaConverter.toSparkSchema(
      JColl.singletonList(field("weird", a, nullable = true)))
    val expected = ArrayType(
      MapType(StringType, DecimalType(38, 18), valueContainsNull = false),
      containsNull = false)
    assert(schema.fields.head.dataType === expected)
  }

  test("toSparkSchema: null 'nullable' defaults to true") {
    val sf = new UCStructField().name("c")
    sf.setType(prim("string"))
    val schema = DeltaRestSchemaConverter.toSparkSchema(JColl.singletonList(sf))
    assert(schema.fields.head.nullable)
  }

  test("toSparkSchema: unknown primitive name fails loudly") {
    val sf = field("c", prim("no-such-type"), nullable = true)
    val e = intercept[IllegalArgumentException] {
      DeltaRestSchemaConverter.toSparkSchema(JColl.singletonList(sf))
    }
    assert(e.getMessage.contains("no-such-type"))
  }

  test("toSparkSchema: scalar metadata of multiple types is preserved") {
    val sf = field("c", prim("long"), nullable = true,
      metadata = Map(
        "str" -> "x",
        "bool" -> java.lang.Boolean.TRUE,
        "int" -> java.lang.Integer.valueOf(42),
        "long" -> java.lang.Long.valueOf(42L),
        "dbl" -> java.lang.Double.valueOf(3.14),
        "flt" -> java.lang.Float.valueOf(1.5f)))
    val schema = DeltaRestSchemaConverter.toSparkSchema(JColl.singletonList(sf))
    val md = schema.fields.head.metadata
    assert(md.getString("str") === "x")
    assert(md.getBoolean("bool"))
    assert(md.getLong("int") === 42L)
    assert(md.getLong("long") === 42L)
    assert(md.getDouble("dbl") === 3.14)
    assert(md.getDouble("flt") === 1.5)
  }

  test("toSparkSchema: non-scalar metadata falls back to toString rather than dropping") {
    val complex = new java.util.LinkedHashMap[String, String]()
    complex.put("a", "b")
    val sf = field("c", prim("long"), nullable = true,
      metadata = Map("odd" -> complex))
    val schema = DeltaRestSchemaConverter.toSparkSchema(JColl.singletonList(sf))
    assert(schema.fields.head.metadata.contains("odd"))
  }

  test("toSparkSchema: missing type fails with a readable error") {
    val sf = new UCStructField().name("c").nullable(true)
    val e = intercept[IllegalArgumentException] {
      DeltaRestSchemaConverter.toSparkSchema(JColl.singletonList(sf))
    }
    assert(e.getMessage.contains("column type must not be null"))
  }

  // ---- Write path ----

  test("toDRCColumns: empty/null schema yields empty list") {
    assert(DeltaRestSchemaConverter.toDRCColumns(null).isEmpty)
    assert(DeltaRestSchemaConverter.toDRCColumns(new StructType()).isEmpty)
  }

  test("toDRCColumns: all supported primitive Spark types") {
    val cases = Seq(
      BooleanType -> "boolean",
      ByteType -> "tinyint",
      ShortType -> "smallint",
      IntegerType -> "int",
      LongType -> "long",
      FloatType -> "float",
      DoubleType -> "double",
      StringType -> "string",
      BinaryType -> "binary",
      DateType -> "date",
      TimestampType -> "timestamp",
      TimestampNTZType -> "timestamp_ntz",
      VariantType -> "variant")
    cases.foreach { case (sparkType, expectedName) =>
      val schema = new StructType().add("c", sparkType, nullable = true)
      val out = DeltaRestSchemaConverter.toDRCColumns(schema)
      assert(out.size() === 1)
      val head = out.get(0)
      assert(head.getName === "c")
      assert(head.getNullable.booleanValue())
      assert(head.getType.isInstanceOf[UCPrimitiveType], sparkType)
      assert(head.getType.getType === expectedName, sparkType)
    }
  }

  test("toDRCColumns: DecimalType becomes structured DecimalType POJO") {
    val schema = new StructType().add("d", DecimalType(38, 18), nullable = false)
    val out = DeltaRestSchemaConverter.toDRCColumns(schema).asScala
    val head = out.head
    assert(!head.getNullable.booleanValue())
    val dt = head.getType.asInstanceOf[UCDecimalType]
    assert(dt.getType === "decimal")
    assert(dt.getPrecision.intValue() === 38)
    assert(dt.getScale.intValue() === 18)
  }

  test("toDRCColumns: ArrayType with containsNull=false") {
    val schema = new StructType().add("a", ArrayType(LongType, containsNull = false))
    val out = DeltaRestSchemaConverter.toDRCColumns(schema).asScala.head
    val arr = out.getType.asInstanceOf[UCArrayType]
    assert(arr.getType === "array")
    assert(!arr.getContainsNull.booleanValue())
    assert(arr.getElementType.asInstanceOf[UCPrimitiveType].getType === "long")
  }

  test("toDRCColumns: MapType with valueContainsNull=false") {
    val schema = new StructType().add("m",
      MapType(StringType, IntegerType, valueContainsNull = false))
    val out = DeltaRestSchemaConverter.toDRCColumns(schema).asScala.head
    val map = out.getType.asInstanceOf[UCMapType]
    assert(map.getType === "map")
    assert(!map.getValueContainsNull.booleanValue())
    assert(map.getKeyType.asInstanceOf[UCPrimitiveType].getType === "string")
    assert(map.getValueType.asInstanceOf[UCPrimitiveType].getType === "int")
  }

  test("toDRCColumns: nested StructType") {
    val inner = new StructType()
      .add("c1", LongType, nullable = true)
      .add("c2", DecimalType(10, 2), nullable = false)
    val schema = new StructType().add("nested", inner, nullable = true)
    val out = DeltaRestSchemaConverter.toDRCColumns(schema).asScala.head
    val struct = out.getType.asInstanceOf[UCStructType]
    assert(struct.getType === "struct")
    assert(struct.getFields.size() === 2)
    assert(struct.getFields.get(0).getName === "c1")
    assert(struct.getFields.get(0).getType.asInstanceOf[UCPrimitiveType].getType === "long")
    assert(struct.getFields.get(1).getName === "c2")
    val dec = struct.getFields.get(1).getType.asInstanceOf[UCDecimalType]
    assert(dec.getPrecision.intValue() === 10 && dec.getScale.intValue() === 2)
  }

  test("toDRCColumns: preserves scalar field metadata verbatim") {
    val meta = new MetadataBuilder()
      .putLong("delta.columnMapping.id", 7L)
      .putString("comment", "hello")
      .putBoolean("delta.generatedIdentity", true)
      .putDouble("sampleRate", 0.5)
      .build()
    val schema = new StructType().add("c", LongType, nullable = true, meta)
    val out = DeltaRestSchemaConverter.toDRCColumns(schema).asScala.head
    val md = out.getMetadata
    assert(md.get("delta.columnMapping.id") === java.lang.Long.valueOf(7L))
    assert(md.get("comment") === "hello")
    assert(md.get("delta.generatedIdentity") === java.lang.Boolean.TRUE)
    assert(md.get("sampleRate") === java.lang.Double.valueOf(0.5))
  }

  test("toDRCColumns: rejects unsupported Spark types") {
    // A custom UDT or CalendarIntervalType would hit the default branch.
    val schema = new StructType().add("c",
      org.apache.spark.sql.types.CalendarIntervalType, nullable = true)
    val e = intercept[IllegalArgumentException] {
      DeltaRestSchemaConverter.toDRCColumns(schema)
    }
    assert(e.getMessage.contains("Unsupported Spark DataType"))
  }

  // ---- Round-trip ----

  test("round-trip: StructType -> UC POJO -> StructType preserves shape") {
    val original = new StructType()
      .add("id", LongType, nullable = false)
      .add("name", StringType, nullable = true)
      .add("tags", ArrayType(StringType, containsNull = false), nullable = true)
      .add("props", MapType(StringType, IntegerType, valueContainsNull = false), nullable = true)
      .add("nested",
        new StructType()
          .add("x", DoubleType, nullable = false)
          .add("y", DecimalType(20, 10), nullable = false),
        nullable = true)
      .add("ts_ntz", TimestampNTZType, nullable = true)
      .add("var", VariantType, nullable = true)

    val drc = DeltaRestSchemaConverter.toDRCColumns(original)
    val roundTripped = DeltaRestSchemaConverter.toSparkSchema(drc)

    // Compare structurally: fields, types, nullability. Metadata is compared separately
    // because our reverse writer serializes via JSON so the Metadata object identity differs.
    assert(roundTripped.fields.length === original.fields.length)
    original.fields.zip(roundTripped.fields).foreach { case (a, b) =>
      assert(a.name === b.name)
      assert(a.dataType === b.dataType, s"dataType mismatch on ${a.name}")
      assert(a.nullable === b.nullable, s"nullable mismatch on ${a.name}")
    }
  }

  test("round-trip: scalar metadata survives StructType -> UC -> StructType") {
    val meta = new MetadataBuilder()
      .putLong("delta.columnMapping.id", 42L)
      .putString("comment", "a comment")
      .build()
    val original = new StructType().add("c", LongType, nullable = true, meta)
    val drc = DeltaRestSchemaConverter.toDRCColumns(original)
    val roundTripped = DeltaRestSchemaConverter.toSparkSchema(drc)
    val md = roundTripped.fields.head.metadata
    assert(md.getLong("delta.columnMapping.id") === 42L)
    assert(md.getString("comment") === "a comment")
  }
}

object DeltaRestSchemaConverterSuite {

  def prim(name: String): UCPrimitiveType = {
    val p = new UCPrimitiveType()
    p.setType(name)
    p
  }

  def field(
      name: String,
      dt: io.unitycatalog.client.delta.model.DeltaType,
      nullable: Boolean,
      metadata: Map[String, AnyRef] = Map.empty): UCStructField = {
    val sf = new UCStructField().name(name).nullable(nullable)
    sf.setType(dt)
    metadata.foreach { case (k, v) => sf.putMetadataItem(k, v) }
    sf
  }

  def jlist(fields: UCStructField*): JList[UCStructField] = {
    val l = new java.util.ArrayList[UCStructField]()
    fields.foreach(l.add)
    l
  }
}
