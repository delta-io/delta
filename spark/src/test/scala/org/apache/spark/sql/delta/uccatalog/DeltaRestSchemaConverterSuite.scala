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

import java.util.{Arrays, Collections, HashMap}

import io.unitycatalog.client.delta.model.{
  ArrayType => DrcArrayType,
  DecimalType => DrcDecimalType,
  MapType => DrcMapType,
  PrimitiveType => DrcPrimitiveType,
  StructField => DrcStructField,
  StructType => DrcStructType
}

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.types._

class DeltaRestSchemaConverterSuite extends AnyFunSuite {

  // --- helpers --------------------------------------------------------------

  private def prim(name: String): DrcPrimitiveType = {
    val p = new DrcPrimitiveType(); p.setType(name); p
  }

  private def field(
      name: String,
      dtype: io.unitycatalog.client.delta.model.DeltaType,
      nullable: Boolean,
      metadata: java.util.Map[String, Object] = Collections.emptyMap()): DrcStructField = {
    val f = new DrcStructField()
    f.setName(name); f.setType(dtype); f.setNullable(nullable); f.setMetadata(metadata); f
  }

  private def convert(fields: DrcStructField*): StructType =
    DeltaRestSchemaConverter.toSparkSchema(Arrays.asList(fields: _*))

  // --- primitives -----------------------------------------------------------

  test("all supported primitives map correctly") {
    val expected = Seq(
      ("string", StringType), ("long", LongType), ("integer", IntegerType),
      ("short", ShortType), ("byte", ByteType), ("double", DoubleType),
      ("float", FloatType), ("boolean", BooleanType), ("binary", BinaryType),
      ("date", DateType), ("timestamp", TimestampType), ("timestamp_ntz", TimestampNTZType))
    expected.foreach { case (drcName, sparkType) =>
      val s = convert(field("c", prim(drcName), nullable = true))
      assert(s.fields(0).dataType == sparkType, s"mismatch for $drcName")
    }
  }

  test("unknown primitive name fails loud") {
    val ex = intercept[IllegalArgumentException] {
      convert(field("c", prim("unknown_type_xyz"), nullable = true))
    }
    assert(ex.getMessage.contains("unknown_type_xyz"))
    assert(ex.getMessage.contains("bump the UC pin"))
  }

  test("null primitive name fails loud (rather than silently defaulting)") {
    intercept[IllegalArgumentException] {
      convert(field("c", prim(null), nullable = true))
    }
  }

  // --- decimal --------------------------------------------------------------

  test("decimal carries precision and scale") {
    val d = new DrcDecimalType(); d.setPrecision(10); d.setScale(2)
    val s = convert(field("c", d, nullable = false))
    assert(s.fields(0).dataType == DecimalType(10, 2))
    assert(!s.fields(0).nullable)
  }

  test("decimal without precision or scale fails loud") {
    val d1 = new DrcDecimalType(); d1.setScale(2)
    intercept[IllegalArgumentException] { convert(field("c", d1, nullable = true)) }
    val d2 = new DrcDecimalType(); d2.setPrecision(10)
    intercept[IllegalArgumentException] { convert(field("c", d2, nullable = true)) }
  }

  // --- array / map / nested struct -----------------------------------------

  test("ArrayType carries elementType and containsNull") {
    val a = new DrcArrayType(); a.setElementType(prim("long")); a.setContainsNull(false)
    val s = convert(field("c", a, nullable = true))
    assert(s.fields(0).dataType == ArrayType(LongType, containsNull = false))
  }

  test("MapType carries key, value, and valueContainsNull") {
    val m = new DrcMapType()
    m.setKeyType(prim("string")); m.setValueType(prim("long")); m.setValueContainsNull(true)
    val s = convert(field("c", m, nullable = true))
    assert(s.fields(0).dataType == MapType(StringType, LongType, valueContainsNull = true))
  }

  test("nested StructType recursively converted") {
    val inner = new DrcStructType()
    inner.setFields(Arrays.asList(
      field("x", prim("long"), nullable = false),
      field("y", prim("string"), nullable = true)))
    val s = convert(field("c", inner, nullable = true))
    s.fields(0).dataType match {
      case st: StructType =>
        assert(st.fields.length == 2)
        assert(st.fields(0) == StructField("x", LongType, nullable = false))
        assert(st.fields(1) == StructField("y", StringType, nullable = true))
      case other =>
        fail(s"expected StructType, got $other")
    }
  }

  // --- column metadata ------------------------------------------------------

  test("column metadata carries delta.columnMapping.id (critical for column mapping)") {
    val md = new HashMap[String, Object](); md.put("delta.columnMapping.id", java.lang.Long.valueOf(7L))
    val s = convert(field("c", prim("string"), nullable = true, metadata = md))
    assert(s.fields(0).metadata.getLong("delta.columnMapping.id") == 7L)
  }

  test("empty column metadata round-trips as Metadata.empty") {
    val s = convert(field("c", prim("string"), nullable = true))
    assert(s.fields(0).metadata == Metadata.empty)
  }

  test("null column metadata is treated as empty (no NPE)") {
    val s = convert(field("c", prim("string"), nullable = true, metadata = null))
    assert(s.fields(0).metadata == Metadata.empty)
  }

  test("mixed-type metadata values convert to the right Spark metadata types") {
    val md = new HashMap[String, Object]()
    md.put("s", "hi")
    md.put("b", java.lang.Boolean.TRUE)
    md.put("i", java.lang.Integer.valueOf(3))
    md.put("d", java.lang.Double.valueOf(1.5))
    val s = convert(field("c", prim("long"), nullable = true, metadata = md))
    val m = s.fields(0).metadata
    assert(m.getString("s") == "hi")
    assert(m.getBoolean("b"))
    assert(m.getLong("i") == 3L)
    assert(m.getDouble("d") == 1.5)
  }

  // --- null-safety / fail-loud ---------------------------------------------

  test("null column list fails loud") {
    intercept[IllegalArgumentException] { DeltaRestSchemaConverter.toSparkSchema(null) }
  }

  test("null field name / type / nullable flag all fail loud with the column name") {
    val f1 = new DrcStructField(); f1.setName(null); f1.setType(prim("long"))
    intercept[IllegalArgumentException] {
      DeltaRestSchemaConverter.toSparkSchema(Arrays.asList(f1))
    }
    val f2 = new DrcStructField(); f2.setName("c"); f2.setType(null)
    val ex2 = intercept[IllegalArgumentException] {
      DeltaRestSchemaConverter.toSparkSchema(Arrays.asList(f2))
    }
    assert(ex2.getMessage.contains("'c'"))
    val f3 = new DrcStructField()
    f3.setName("c"); f3.setType(prim("long")); f3.setNullable(null)
    val ex3 = intercept[IllegalArgumentException] {
      DeltaRestSchemaConverter.toSparkSchema(Arrays.asList(f3))
    }
    assert(ex3.getMessage.contains("'c'"))
  }
}
