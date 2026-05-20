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

package io.delta.storage.commit.uccommitcoordinator

import java.util.{Arrays, Collections}

import com.fasterxml.jackson.databind.ObjectMapper
import io.unitycatalog.client.delta.model.{ArrayType, DeltaType, MapType, PrimitiveType}
import io.unitycatalog.client.delta.model.{StructField, StructType}

import org.scalatest.funsuite.AnyFunSuite

class UCDeltaSchemaConverterSuite extends AnyFunSuite {

  private val objectMapper = new ObjectMapper()

  private def prim(t: String): PrimitiveType = new PrimitiveType().`type`(t)
  private def field(name: String, t: DeltaType, nullable: Boolean = true): StructField =
    new StructField().name(name).nullable(nullable).`type`(t)

  // ----------------------------------------
  // serializeSchema
  // ----------------------------------------

  test("serializeSchema: null input returns null") {
    assert(UCDeltaSchemaConverter.serializeSchema(null) === null)
  }

  test("serializeSchema: empty struct produces an empty fields array") {
    val json = UCDeltaSchemaConverter.serializeSchema(new StructType())
    val parsed = objectMapper.readTree(json)
    assert(parsed.get("type").asText() === "struct")
    assert(parsed.get("fields").isArray)
    assert(parsed.get("fields").size() === 0)
  }

  test("serializeSchema: primitives serialize as bare-string types (Delta wire format)") {
    val s = new StructType()
      .addFieldsItem(field("a", prim("integer")))
      .addFieldsItem(field("b", prim("string"), nullable = false))
    val parsed = objectMapper.readTree(UCDeltaSchemaConverter.serializeSchema(s))
    val fields = parsed.get("fields")
    assert(fields.get(0).get("name").asText() === "a")
    assert(fields.get(0).get("nullable").asBoolean() === true)
    // bare string, not {"type":"integer"}
    assert(fields.get(0).get("type").isTextual)
    assert(fields.get(0).get("type").asText() === "integer")
    assert(fields.get(1).get("name").asText() === "b")
    assert(fields.get(1).get("nullable").asBoolean() === false)
    assert(fields.get(1).get("type").asText() === "string")
  }

  test("serializeSchema: decimal serializes as bare-string with parameters") {
    val s = new StructType()
      .addFieldsItem(field("d", prim("decimal(10,2)")))
    val parsed = objectMapper.readTree(UCDeltaSchemaConverter.serializeSchema(s))
    val t = parsed.get("fields").get(0).get("type")
    assert(t.isTextual)
    assert(t.asText() === "decimal(10,2)")
  }

  test("serializeSchema: array uses camelCase keys (elementType, containsNull)") {
    val arr = new ArrayType().elementType(prim("string")).containsNull(true)
    val s = new StructType().addFieldsItem(field("a", arr))
    val parsed = objectMapper.readTree(UCDeltaSchemaConverter.serializeSchema(s))
    val t = parsed.get("fields").get(0).get("type")
    assert(t.get("type").asText() === "array")
    assert(t.get("elementType").asText() === "string")
    assert(t.get("containsNull").asBoolean() === true)
    // kebab-case keys should NOT appear
    assert(!t.has("element-type"))
    assert(!t.has("contains-null"))
  }

  test("serializeSchema: map uses camelCase keys (keyType, valueType, valueContainsNull)") {
    val m = new MapType()
      .keyType(prim("string"))
      .valueType(prim("integer"))
      .valueContainsNull(false)
    val s = new StructType().addFieldsItem(field("m", m))
    val parsed = objectMapper.readTree(UCDeltaSchemaConverter.serializeSchema(s))
    val t = parsed.get("fields").get(0).get("type")
    assert(t.get("type").asText() === "map")
    assert(t.get("keyType").asText() === "string")
    assert(t.get("valueType").asText() === "integer")
    assert(t.get("valueContainsNull").asBoolean() === false)
    assert(!t.has("key-type"))
    assert(!t.has("value-type"))
    assert(!t.has("value-contains-null"))
  }

  test("serializeSchema: nested array of map uses camelCase at every level") {
    val inner = new MapType()
      .keyType(prim("string"))
      .valueType(prim("long"))
      .valueContainsNull(true)
    val outer = new ArrayType().elementType(inner).containsNull(false)
    val s = new StructType().addFieldsItem(field("nested", outer))
    val parsed = objectMapper.readTree(UCDeltaSchemaConverter.serializeSchema(s))
    val arrType = parsed.get("fields").get(0).get("type")
    assert(arrType.get("type").asText() === "array")
    assert(arrType.get("containsNull").asBoolean() === false)
    val mapType = arrType.get("elementType")
    assert(mapType.get("type").asText() === "map")
    assert(mapType.get("keyType").asText() === "string")
    assert(mapType.get("valueType").asText() === "long")
    assert(mapType.get("valueContainsNull").asBoolean() === true)
  }

  // ----------------------------------------
  // parseSchemaString
  // ----------------------------------------

  test("parseSchemaString: throws UnsupportedOperationException (not yet implemented)") {
    val e = intercept[UnsupportedOperationException] {
      UCDeltaSchemaConverter.parseSchemaString("""{"type":"struct","fields":[]}""")
    }
    assert(e.getMessage.contains("not yet implemented"))
  }

  // ----------------------------------------
  // toUCStructType (ColumnDef path)
  // ----------------------------------------

  private def col(
      name: String,
      typeName: String,
      typeText: String = null,
      nullable: Boolean = true): UCClient.ColumnDef =
    new UCClient.ColumnDef(
      name, typeName, if (typeText == null) typeName.toLowerCase else typeText, "{}", nullable, 0)

  test("toUCStructType: null input throws NullPointerException with descriptive message") {
    val e = intercept[NullPointerException] {
      UCDeltaSchemaConverter.toUCStructType(null)
    }
    assert(e.getMessage.contains("columns"))
  }

  test("toUCStructType: empty list returns empty struct") {
    val s = UCDeltaSchemaConverter.toUCStructType(Collections.emptyList())
    assert(s.getFields == null || s.getFields.isEmpty)
  }

  test("toUCStructType: primitive columns map to StructFields with PrimitiveType") {
    val cols = Arrays.asList(
      col("a", "INT", "integer", nullable = false),
      col("b", "STRING", "string", nullable = true),
      col("c", "DECIMAL", "decimal(10,2)", nullable = true))
    val s = UCDeltaSchemaConverter.toUCStructType(cols)
    assert(s.getFields.size() === 3)
    val a = s.getFields.get(0)
    assert(a.getName === "a")
    assert(a.getNullable === false)
    assert(a.getType.isInstanceOf[PrimitiveType])
    assert(a.getType.asInstanceOf[PrimitiveType].getType === "integer")
    val c = s.getFields.get(2)
    assert(c.getType.asInstanceOf[PrimitiveType].getType === "decimal(10,2)")
  }

  test("toUCStructType: complex column type throws UnsupportedOperationException") {
    val cols = Arrays.asList(col("a", "ARRAY", "array<int>"))
    val e = intercept[UnsupportedOperationException] {
      UCDeltaSchemaConverter.toUCStructType(cols)
    }
    assert(e.getMessage.contains("Complex column type 'ARRAY'"))
    assert(e.getMessage.contains("column 'a'"))
  }

  test("toUCStructType: every primitive type name is accepted") {
    val typeNames = Seq(
      "BOOLEAN", "BYTE", "SHORT", "INT", "LONG", "FLOAT", "DOUBLE",
      "DATE", "TIMESTAMP", "TIMESTAMP_NTZ", "STRING", "BINARY", "DECIMAL")
    typeNames.foreach { tn =>
      val s = UCDeltaSchemaConverter.toUCStructType(
        Arrays.asList(col("x", tn)))
      assert(s.getFields.size() === 1)
    }
  }

  // ----------------------------------------
  // End-to-end: a complex schema with every supported shape, fully validated
  // ----------------------------------------

  test("serializeSchema: mixed schema preserves every field's name, type, and nullability") {
    val arr = new ArrayType().elementType(prim("double")).containsNull(false)
    val map = new MapType()
      .keyType(prim("string"))
      .valueType(prim("date"))
      .valueContainsNull(true)
    val arrOfMap = new ArrayType().elementType(map).containsNull(true)

    val s = new StructType()
      .addFieldsItem(field("z_int", prim("integer"), nullable = false))
      .addFieldsItem(field("a_str", prim("string")))
      .addFieldsItem(field("dec", prim("decimal(38,18)"), nullable = false))
      .addFieldsItem(field("arr_dbl", arr))
      .addFieldsItem(field("map_sd", map, nullable = false))
      .addFieldsItem(field("nested", arrOfMap))

    val parsed = objectMapper.readTree(UCDeltaSchemaConverter.serializeSchema(s))
    assert(parsed.get("type").asText() === "struct")
    val fields = parsed.get("fields")
    assert(fields.size() === 6)

    // Field ordering is preserved verbatim from the input list.
    val expectedNames = Seq("z_int", "a_str", "dec", "arr_dbl", "map_sd", "nested")
    for (i <- 0 until fields.size()) {
      assert(fields.get(i).get("name").asText() === expectedNames(i))
    }

    // Primitive fields: bare-string type, correct nullability.
    assert(fields.get(0).get("nullable").asBoolean() === false)
    assert(fields.get(0).get("type").asText() === "integer")
    assert(fields.get(1).get("nullable").asBoolean() === true)
    assert(fields.get(1).get("type").asText() === "string")
    assert(fields.get(2).get("nullable").asBoolean() === false)
    assert(fields.get(2).get("type").asText() === "decimal(38,18)")

    // Array<double>, non-nullable elements.
    val arrJson = fields.get(3).get("type")
    assert(arrJson.get("type").asText() === "array")
    assert(arrJson.get("elementType").asText() === "double")
    assert(arrJson.get("containsNull").asBoolean() === false)

    // Map<string, date>, nullable values, non-nullable field itself.
    val mapJson = fields.get(4).get("type")
    assert(fields.get(4).get("nullable").asBoolean() === false)
    assert(mapJson.get("type").asText() === "map")
    assert(mapJson.get("keyType").asText() === "string")
    assert(mapJson.get("valueType").asText() === "date")
    assert(mapJson.get("valueContainsNull").asBoolean() === true)

    // Array<Map<string, date>> nested two deep, with the inner map's nullability flowing through.
    val nestedArr = fields.get(5).get("type")
    assert(nestedArr.get("type").asText() === "array")
    assert(nestedArr.get("containsNull").asBoolean() === true)
    val nestedMap = nestedArr.get("elementType")
    assert(nestedMap.get("type").asText() === "map")
    assert(nestedMap.get("keyType").asText() === "string")
    assert(nestedMap.get("valueType").asText() === "date")
    assert(nestedMap.get("valueContainsNull").asBoolean() === true)

    // Negative: confirm no kebab-case keys leaked through at any level.
    val json = UCDeltaSchemaConverter.serializeSchema(s)
    val kebabKeys = Seq(
      "element-type", "contains-null", "key-type", "value-type", "value-contains-null")
    kebabKeys.foreach { k =>
      assert(!json.contains("\"" + k + "\""), s"unexpected kebab-case key '$k'")
    }
  }
}
