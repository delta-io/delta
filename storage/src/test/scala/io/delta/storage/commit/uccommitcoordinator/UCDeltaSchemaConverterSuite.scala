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
import io.unitycatalog.client.delta.model.DeltaArrayType
import io.unitycatalog.client.delta.model.DeltaDataType
import io.unitycatalog.client.delta.model.DeltaDecimalType
import io.unitycatalog.client.delta.model.DeltaMapType
import io.unitycatalog.client.delta.model.DeltaPrimitiveType
import io.unitycatalog.client.delta.model.DeltaStructField
import io.unitycatalog.client.delta.model.DeltaStructFieldMetadata
import io.unitycatalog.client.delta.model.DeltaStructType

import org.scalatest.funsuite.AnyFunSuite

class UCDeltaSchemaConverterSuite extends AnyFunSuite {

  private val objectMapper = new ObjectMapper()

  private def prim(t: String): DeltaPrimitiveType = new DeltaPrimitiveType().`type`(t)
  // Delta's wire format requires `metadata` to always be present (even if empty); a real
  // schema parsed from the wire never has a null metadata, so the helper mirrors that
  // shape with an empty DeltaStructFieldMetadata by default.
  private def field(name: String, t: DeltaDataType, nullable: Boolean = true): DeltaStructField =
    new DeltaStructField()
      .name(name).nullable(nullable).`type`(t).metadata(new DeltaStructFieldMetadata())

  /**
   * One example per primitive: (UC `ColumnTypeName`, catalog-side DDL form, Delta wire form).
   * The DDL form and wire form diverge for several primitives (INT/int/integer,
   * LONG/bigint/long, ...); the converter must read the wire form from `typeJson` and ignore
   * the DDL `typeText`.
   */
  private val primitiveExamples: Seq[(String, String, String)] = Seq(
    ("BOOLEAN", "boolean", "boolean"),
    ("BYTE", "tinyint", "byte"),
    ("SHORT", "smallint", "short"),
    ("INT", "int", "integer"),
    ("LONG", "bigint", "long"),
    ("FLOAT", "float", "float"),
    ("DOUBLE", "double", "double"),
    ("DATE", "date", "date"),
    ("TIMESTAMP", "timestamp", "timestamp"),
    ("TIMESTAMP_NTZ", "timestamp_ntz", "timestamp_ntz"),
    ("STRING", "string", "string"),
    ("BINARY", "binary", "binary"),
    ("DECIMAL", "decimal(10,2)", "decimal(10,2)"))

  /** Build the DeltaStructField JSON that UC's `toStructFieldJson` produces. */
  private def fieldJson(name: String, deltaWireType: String, nullable: Boolean): String =
    s"""{"name":"$name","type":"$deltaWireType","nullable":$nullable,"metadata":{}}"""

  // ----------------------------------------
  // serializeSchema
  // ----------------------------------------

  test("serializeSchema: null input returns null") {
    assert(UCDeltaSchemaConverter.serializeSchema(null) === null)
  }

  test("serializeSchema: empty struct produces an empty fields array") {
    val parsed =
      objectMapper.readTree(UCDeltaSchemaConverter.serializeSchema(new DeltaStructType()))
    assert(parsed.get("type").asText() === "struct")
    assert(parsed.get("fields").isArray)
    assert(parsed.get("fields").size() === 0)
  }

  test("serializeSchema: every primitive serializes as bare-string wire form " +
      "inside Array and Map containers") {
    // Covers (a) bare-string emission (DeltaPrimitiveType serializes flat, not {"type":"integer"}),
    // (b) decimal parameter preservation, and (c) wire fidelity for every primitive type at
    // both Array.elementType and Map.valueType (the two camelCase mixin paths).
    primitiveExamples.foreach { case (_, _, p) =>
      val arr = new DeltaArrayType().elementType(prim(p)).containsNull(true)
      val m = new DeltaMapType().keyType(prim("string")).valueType(prim(p)).valueContainsNull(true)
      val s = new DeltaStructType().addFieldsItem(field("a", arr)).addFieldsItem(field("m", m))
      val parsed = objectMapper.readTree(UCDeltaSchemaConverter.serializeSchema(s))
      val arrType = parsed.get("fields").get(0).get("type")
      assert(arrType.get("type").asText() === "array", s"primitive=$p")
      assert(arrType.get("elementType").asText() === p, s"primitive=$p: elementType mismatch")
      val mapType = parsed.get("fields").get(1).get("type")
      assert(mapType.get("type").asText() === "map", s"primitive=$p")
      assert(mapType.get("valueType").asText() === p, s"primitive=$p: valueType mismatch")
    }
  }

  test("serializeSchema: complex schema preserves names, types, nullability, and emits " +
      "camelCase (never kebab-case) at every level") {
    // Kitchen-sink schema exercising the mixins (array, map, nested struct, array-of-map,
    // map-of-struct, array-of-struct) and nullability in every direction.
    val innerStruct = new DeltaStructType()
      .addFieldsItem(field("k", prim("string")))
      .addFieldsItem(field("v", prim("long"), nullable = false))
    val arrOfStruct = new DeltaArrayType().elementType(innerStruct).containsNull(false)
    val mapOfStruct = new DeltaMapType()
      .keyType(prim("string"))
      .valueType(innerStruct)
      .valueContainsNull(true)
    val innerMap = new DeltaMapType()
      .keyType(prim("string"))
      .valueType(prim("date"))
      .valueContainsNull(true)
    val arrOfMap = new DeltaArrayType().elementType(innerMap).containsNull(true)

    val s = new DeltaStructType()
      .addFieldsItem(field("z_int", prim("integer"), nullable = false))
      .addFieldsItem(field("a_str", prim("string")))
      .addFieldsItem(field("arr_of_struct", arrOfStruct))
      .addFieldsItem(field("map_of_struct", mapOfStruct, nullable = false))
      .addFieldsItem(field("nested_arr_of_map", arrOfMap))

    val json = UCDeltaSchemaConverter.serializeSchema(s)
    val parsed = objectMapper.readTree(json)
    val fields = parsed.get("fields")
    assert(parsed.get("type").asText() === "struct")
    val expectedNames = Seq("z_int", "a_str", "arr_of_struct", "map_of_struct",
      "nested_arr_of_map")
    for (i <- 0 until fields.size()) {
      assert(fields.get(i).get("name").asText() === expectedNames(i))
    }
    // Primitives.
    assert(fields.get(0).get("nullable").asBoolean() === false)
    assert(fields.get(0).get("type").asText() === "integer")
    assert(fields.get(1).get("type").asText() === "string")
    // Array-of-struct.
    val arrJson = fields.get(2).get("type")
    assert(arrJson.get("type").asText() === "array")
    assert(arrJson.get("containsNull").asBoolean() === false)
    val arrElem = arrJson.get("elementType")
    assert(arrElem.get("type").asText() === "struct")
    assert(arrElem.get("fields").get(1).get("name").asText() === "v")
    assert(arrElem.get("fields").get(1).get("nullable").asBoolean() === false)
    // Map-of-struct.
    val mapJson = fields.get(3).get("type")
    assert(fields.get(3).get("nullable").asBoolean() === false)
    assert(mapJson.get("type").asText() === "map")
    assert(mapJson.get("keyType").asText() === "string")
    assert(mapJson.get("valueContainsNull").asBoolean() === true)
    val mapVal = mapJson.get("valueType")
    assert(mapVal.get("type").asText() === "struct")
    // Array-of-map.
    val nestedArr = fields.get(4).get("type")
    assert(nestedArr.get("type").asText() === "array")
    val nestedMap = nestedArr.get("elementType")
    assert(nestedMap.get("type").asText() === "map")
    assert(nestedMap.get("valueType").asText() === "date")
    // Kebab-case keys must not appear anywhere.
    Seq("element-type", "contains-null", "key-type", "value-type", "value-contains-null")
      .foreach { k =>
        assert(!json.contains("\"" + k + "\""), s"unexpected kebab-case key '$k' in: $json")
      }
  }

  test("serializeSchema: per-field metadata round-trips; empty metadata emits {} (not omitted)") {
    val metadata = new DeltaStructFieldMetadata()
    metadata.put("comment", "user-facing column doc")
    metadata.put("delta.columnMapping.id", java.lang.Long.valueOf(42L))
    val annotated = new DeltaStructField()
      .name("annotated").nullable(true).`type`(prim("integer")).metadata(metadata)
    val plain = field("plain", prim("integer"))
    val s = new DeltaStructType().addFieldsItem(annotated).addFieldsItem(plain)

    val parsed = objectMapper.readTree(UCDeltaSchemaConverter.serializeSchema(s))
    val annotatedMeta = parsed.get("fields").get(0).get("metadata")
    assert(annotatedMeta.get("comment").asText() === "user-facing column doc")
    assert(annotatedMeta.get("delta.columnMapping.id").asLong() === 42L)
    // Empty metadata must still be present as `{}`; Delta's reader treats omission as ambiguous.
    val plainMeta = parsed.get("fields").get(1).get("metadata")
    assert(plainMeta != null && plainMeta.isObject && plainMeta.size() === 0)
  }

  // ----------------------------------------
  // parseSchemaString
  // ----------------------------------------

  test("parseSchemaString: null input throws NullPointerException") {
    val e = intercept[NullPointerException] {
      UCDeltaSchemaConverter.parseSchemaString(null)
    }
    assert(e.getMessage.contains("schemaString"))
  }

  test("parseSchemaString: empty struct round-trips") {
    val parsed = UCDeltaSchemaConverter.parseSchemaString("""{"type":"struct","fields":[]}""")
    assert(parsed.isInstanceOf[DeltaStructType])
    assert(parsed.getFields == null || parsed.getFields.isEmpty)
  }

  test("parseSchemaString: complex nested schema dispatches to correct subtypes and " +
      "preserves decimal precision/scale plus per-field metadata at every level") {
    val json =
      """{"type":"struct","fields":[
        |{"name":"i","type":"integer","nullable":false,"metadata":{}},
        |{"name":"d","type":"decimal(10,2)","nullable":true,"metadata":{}},
        |{"name":"arr","type":{"type":"array","elementType":"string","containsNull":true},
        |"nullable":true,"metadata":{}},
        |{"name":"m","type":{"type":"map","keyType":"string","valueType":"long",
        |"valueContainsNull":false},"nullable":true,"metadata":{}},
        |{"name":"outer","type":{"type":"struct","fields":[
        |{"name":"inner","type":"integer","nullable":false,
        |"metadata":{"comment":"inner-doc","delta.columnMapping.id":7}}
        |]},"nullable":true,
        |"metadata":{"comment":"outer-doc","delta.columnMapping.id":5}}
        |]}""".stripMargin.replaceAll("\\s+", "")
    val parsed = UCDeltaSchemaConverter.parseSchemaString(json)
    assert(parsed.getFields.size() === 5)

    val i = parsed.getFields.get(0)
    assert(i.getNullable === false)
    assert(i.getType.asInstanceOf[DeltaPrimitiveType].getType === "integer")

    val d = parsed.getFields.get(1).getType.asInstanceOf[DeltaDecimalType]
    assert(d.getPrecision === 10 && d.getScale === 2)

    val arr = parsed.getFields.get(2).getType.asInstanceOf[DeltaArrayType]
    assert(arr.getContainsNull === true)
    assert(arr.getElementType.asInstanceOf[DeltaPrimitiveType].getType === "string")

    val m = parsed.getFields.get(3).getType.asInstanceOf[DeltaMapType]
    assert(m.getValueContainsNull === false)
    assert(m.getKeyType.asInstanceOf[DeltaPrimitiveType].getType === "string")
    assert(m.getValueType.asInstanceOf[DeltaPrimitiveType].getType === "long")

    val outer = parsed.getFields.get(4)
    assert(outer.getMetadata.get("comment") === "outer-doc")
    assert(outer.getMetadata.get("delta.columnMapping.id")
      .asInstanceOf[Number].longValue === 5L)
    val inner = outer.getType.asInstanceOf[DeltaStructType].getFields.get(0)
    assert(inner.getMetadata.get("comment") === "inner-doc")
    assert(inner.getMetadata.get("delta.columnMapping.id")
      .asInstanceOf[Number].longValue === 7L)
  }

  test("parseSchemaString: malformed JSON throws IllegalStateException") {
    val e = intercept[IllegalStateException] {
      UCDeltaSchemaConverter.parseSchemaString("""{"type":"struct","fields":[""")
    }
    assert(e.getMessage.contains("Failed to parse"))
  }

  test("serializeSchema -> parseSchemaString round-trip: complex schema is idempotent on the " +
      "wire") {
    val arr = new DeltaArrayType().elementType(prim("double")).containsNull(false)
    val map = new DeltaMapType()
      .keyType(prim("string"))
      .valueType(prim("date"))
      .valueContainsNull(true)
    val inner = new DeltaStructType()
      .addFieldsItem(field("inner_i", prim("integer"), nullable = false))
      .addFieldsItem(field("inner_s", prim("string")))
    val original = new DeltaStructType()
      .addFieldsItem(field("a", prim("integer"), nullable = false))
      .addFieldsItem(field("arr", arr))
      .addFieldsItem(field("m", map))
      .addFieldsItem(field("nested", inner))

    val json1 = UCDeltaSchemaConverter.serializeSchema(original)
    val json2 = UCDeltaSchemaConverter.serializeSchema(
      UCDeltaSchemaConverter.parseSchemaString(json1))
    assert(json1 === json2, s"round-trip changed JSON:\n  before=$json1\n  after =$json2")
  }

  // ----------------------------------------
  // toUCStructType (ColumnDef adapter)
  // ----------------------------------------

  /**
   * Build a primitive-typed ColumnDef from the (UC typeName, DDL typeText, Delta wire form)
   * triple. The Delta wire form is what flows into `typeJson` (which is the only thing the
   * converter consults).
   */
  private def col(
      name: String,
      typeName: String,
      typeText: String,
      deltaWireType: String,
      nullable: Boolean = true): UCClient.ColumnDef =
    new UCClient.ColumnDef(
      name, typeName, typeText, fieldJson(name, deltaWireType, nullable), nullable, 0)

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

  test("toUCStructType: every primitive's wire form comes from typeJson, not the DDL typeText") {
    // Regression guard: an earlier fast path read `typeText` (DDL form, e.g. "int"/"bigint") and
    // produced wrong wire output for INT/LONG/BYTE/SHORT. typeJson must be authoritative.
    primitiveExamples.foreach { case (typeName, ddl, wire) =>
      val column = col(name = "x", typeName = typeName, typeText = ddl, deltaWireType = wire)
      val s = UCDeltaSchemaConverter.toUCStructType(Collections.singletonList(column))
      val t = s.getFields.get(0).getType
      if (typeName == "DECIMAL") {
        assert(t.isInstanceOf[DeltaDecimalType], s"typeName=$typeName: expected DeltaDecimalType")
      } else {
        assert(
          t.isInstanceOf[DeltaPrimitiveType], s"typeName=$typeName: expected DeltaPrimitiveType")
        val got = t.asInstanceOf[DeltaPrimitiveType].getType
        assert(got === wire, s"typeName=$typeName: expected wire '$wire' got '$got'")
        if (ddl != wire) {
          assert(got !== ddl, s"typeName=$typeName: DDL form '$ddl' leaked into the output")
        }
      }
    }
  }

  test("toUCStructType: missing typeJson throws IllegalArgumentException naming the column") {
    val cols = Arrays.asList(
      new UCClient.ColumnDef("a", "INT", "int", null, true, 0))
    val e = intercept[IllegalArgumentException] {
      UCDeltaSchemaConverter.toUCStructType(cols)
    }
    assert(e.getMessage.contains("column 'a'"))
    assert(e.getMessage.contains("typeJson is empty"))
  }

  test("toUCStructType: malformed typeJson throws IllegalStateException naming the column") {
    val cols = Arrays.asList(
      new UCClient.ColumnDef("a", "STRUCT", "struct<x:int>", "{not valid json", true, 0))
    val e = intercept[IllegalStateException] {
      UCDeltaSchemaConverter.toUCStructType(cols)
    }
    assert(e.getMessage.contains("Failed to parse typeJson"))
    assert(e.getMessage.contains("column 'a'"))
  }

  test("toUCStructType: typeJson without a 'type' field throws IllegalStateException") {
    // E.g. a bare type JSON or an empty object -- neither is DeltaStructField-shaped.
    val cols = Arrays.asList(
      new UCClient.ColumnDef("a", "INT", "int", "{}", true, 0))
    val e = intercept[IllegalStateException] {
      UCDeltaSchemaConverter.toUCStructType(cols)
    }
    assert(e.getMessage.contains("missing the 'type' field"))
    assert(e.getMessage.contains("column 'a'"))
  }

  test("toUCStructType: deeply nested complex column with metadata at every level parses " +
      "through and round-trips unchanged") {
    // Array<Struct< x: Integer, y: Map<String, Array<Long>> >> -- exercises every container
    // (array-of-struct, struct with two fields of different complex types, map with complex
    // value, map-value-is-array, primitive at the leaf) plus metadata at struct levels.
    val fieldJson =
      """{"name":"deep","type":{
        |"type":"array","containsNull":true,"elementType":{
        |"type":"struct","fields":[
        |{"name":"x","type":"integer","nullable":true,
        |"metadata":{"comment":"x-doc","delta.columnMapping.id":1}},
        |{"name":"y","type":{"type":"map","keyType":"string","valueContainsNull":false,
        |"valueType":{"type":"array","elementType":"long","containsNull":true}},
        |"nullable":true,"metadata":{"comment":"y-doc","delta.columnMapping.id":2}}]}
        |},"nullable":true,
        |"metadata":{"comment":"deep-doc","delta.columnMapping.id":3}}""".stripMargin
        .replaceAll("\\s+", "")
    val cols = Arrays.asList(
      new UCClient.ColumnDef("deep", "ARRAY", "array<struct<...>>", fieldJson, true, 0))
    val s = UCDeltaSchemaConverter.toUCStructType(cols)
    assert(s.getFields.size() === 1)

    val outer = s.getFields.get(0)
    assert(outer.getName === "deep")
    assert(outer.getMetadata.get("delta.columnMapping.id").asInstanceOf[Number].longValue === 3L)

    val outerArr = outer.getType.asInstanceOf[DeltaArrayType]
    assert(outerArr.getContainsNull === true)
    val innerStruct = outerArr.getElementType.asInstanceOf[DeltaStructType]
    val innerFields = innerStruct.getFields
    assert(innerFields.size() === 2)
    assert(innerFields.get(0).getType.asInstanceOf[DeltaPrimitiveType].getType === "integer")
    assert(innerFields.get(0).getMetadata.get("comment") === "x-doc")
    val yMap = innerFields.get(1).getType.asInstanceOf[DeltaMapType]
    assert(yMap.getValueContainsNull === false)
    val yArr = yMap.getValueType.asInstanceOf[DeltaArrayType]
    assert(yArr.getContainsNull === true)
    assert(yArr.getElementType.asInstanceOf[DeltaPrimitiveType].getType === "long")

    // Round-trip: parsed DeltaStructField re-serializes to the same JSON tree (order-insensitive).
    val reserialized = UCDeltaSchemaConverter.serializeSchema(s)
    val expectedTree = objectMapper.readTree(s"""{"type":"struct","fields":[$fieldJson]}""")
    val actualTree = objectMapper.readTree(reserialized)
    assert(actualTree === expectedTree,
      s"round-trip changed JSON:\n  expected=$fieldJson\n  actual  =$reserialized")
  }
}
