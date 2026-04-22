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

import java.util.{Collections, List => JList, Map => JMap}

import scala.collection.JavaConverters._

import io.unitycatalog.client.delta.model.{
  ArrayType => DrcArrayType,
  DecimalType => DrcDecimalType,
  DeltaType => DrcDeltaType,
  MapType => DrcMapType,
  PrimitiveType => DrcPrimitiveType,
  StructField => DrcStructField,
  StructType => DrcStructType
}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types._

class DeltaRestSchemaConverterSuite extends SparkFunSuite {

  private def prim(name: String): DrcPrimitiveType = {
    val p = new DrcPrimitiveType(); p.setType(name); p
  }
  private def field(name: String, dt: DrcDeltaType, nullable: Boolean = true,
      meta: JMap[String, Object] = Collections.emptyMap()): DrcStructField = {
    val f = new DrcStructField()
    f.setName(name); f.setType(dt); f.setNullable(nullable); f.setMetadata(meta); f
  }
  private def cols(fs: DrcStructField*): JList[DrcStructField] = fs.toList.asJava

  test("primitive types round-trip through the forward converter") {
    val expected = Seq(
      "string" -> StringType, "long" -> LongType, "integer" -> IntegerType,
      "short" -> ShortType, "byte" -> ByteType, "double" -> DoubleType,
      "float" -> FloatType, "boolean" -> BooleanType, "binary" -> BinaryType,
      "date" -> DateType, "timestamp" -> TimestampType, "timestamp_ntz" -> TimestampNTZType)
    val schema = DeltaRestSchemaConverter.toSparkSchema(
      cols(expected.map { case (n, _) => field(n, prim(n)) }: _*))
    assert(schema.fields.length == expected.length)
    expected.zip(schema.fields).foreach { case ((n, dt), f) =>
      assert(f.name == n); assert(f.dataType == dt); assert(f.nullable)
    }
  }

  test("decimal preserves precision and scale") {
    val d = new DrcDecimalType(); d.setPrecision(18); d.setScale(4)
    val schema = DeltaRestSchemaConverter.toSparkSchema(cols(field("amt", d)))
    assert(schema("amt").dataType == DecimalType(18, 4))
  }

  test("array preserves containsNull") {
    val a = new DrcArrayType(); a.setElementType(prim("string")); a.setContainsNull(false)
    val schema = DeltaRestSchemaConverter.toSparkSchema(cols(field("tags", a)))
    assert(schema("tags").dataType == ArrayType(StringType, containsNull = false))
  }

  test("map preserves valueContainsNull") {
    val m = new DrcMapType()
    m.setKeyType(prim("string")); m.setValueType(prim("long")); m.setValueContainsNull(false)
    val schema = DeltaRestSchemaConverter.toSparkSchema(cols(field("props", m)))
    assert(schema("props").dataType == MapType(StringType, LongType, valueContainsNull = false))
  }

  test("nested struct walks recursively") {
    val inner = new DrcStructType(); inner.setFields(cols(field("x", prim("long"))))
    val schema = DeltaRestSchemaConverter.toSparkSchema(cols(field("outer", inner)))
    val outer = schema("outer").dataType.asInstanceOf[StructType]
    assert(outer("x").dataType == LongType)
  }

  test("nullability and column metadata are preserved") {
    val meta: JMap[String, Object] = Map[String, Object](
      "delta.columnMapping.id" -> java.lang.Long.valueOf(7L),
      "comment" -> "user id").asJava
    val schema = DeltaRestSchemaConverter.toSparkSchema(
      cols(field("id", prim("long"), nullable = false, meta)))
    val f = schema("id")
    assert(!f.nullable)
    assert(f.metadata.getLong("delta.columnMapping.id") == 7L)
    assert(f.metadata.getString("comment") == "user id")
  }

  test("unknown primitive name fails loud") {
    val ex = intercept[IllegalArgumentException] {
      DeltaRestSchemaConverter.toSparkSchema(cols(field("x", prim("variant_v9000"))))
    }
    assert(ex.getMessage.contains("Unknown DRC primitive"))
  }

  test("null type on a field fails loud") {
    val f = new DrcStructField(); f.setName("x"); f.setType(null); f.setNullable(true)
    val ex = intercept[IllegalArgumentException] {
      DeltaRestSchemaConverter.toSparkSchema(cols(f))
    }
    assert(ex.getMessage.contains("null type"))
  }
}
