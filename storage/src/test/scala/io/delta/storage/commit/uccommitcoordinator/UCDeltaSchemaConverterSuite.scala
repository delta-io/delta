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

import java.util.{Arrays, Collections, HashMap => JHashMap}

import com.fasterxml.jackson.databind.ObjectMapper
import io.unitycatalog.client.delta.model.{ArrayType, DecimalType, DeltaType, MapType}
import io.unitycatalog.client.delta.model.{PrimitiveType, StructField, StructType}

import org.scalatest.funsuite.AnyFunSuite

class UCDeltaSchemaConverterSuite extends AnyFunSuite {

  private val objectMapper = new ObjectMapper()

  private def prim(t: String): PrimitiveType = new PrimitiveType().`type`(t)
  private def field(name: String, t: DeltaType, nullable: Boolean = true): StructField =
    new StructField().name(name).nullable(nullable).`type`(t)

  /**
   * One example per primitive: (UC `ColumnTypeName`, Spark DDL form / `catalogString`,
   * Delta wire form). Note the DDL form and wire form diverge for several primitives
   * (`INT`/`int`/`integer`, `LONG`/`bigint`/`long`, etc.); the converter must read the wire
   * form from `typeJson` and ignore the DDL `typeText`.
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

  /** Build the Spark-style StructField JSON that UCSingleCatalog's `toStructFieldJson` produces. */
  private def fieldJson(name: String, deltaWireType: String, nullable: Boolean): String =
    s"""{"name":"$name","type":"$deltaWireType","nullable":$nullable,"metadata":{}}"""

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

  test("parseSchemaString: null input throws NullPointerException") {
    val e = intercept[NullPointerException] {
      UCDeltaSchemaConverter.parseSchemaString(null)
    }
    assert(e.getMessage.contains("schemaString"))
  }

  test("parseSchemaString: empty struct round-trips") {
    val parsed = UCDeltaSchemaConverter.parseSchemaString("""{"type":"struct","fields":[]}""")
    assert(parsed.isInstanceOf[StructType])
    assert(parsed.getFields == null || parsed.getFields.isEmpty)
  }

  test("parseSchemaString: struct with primitive fields round-trips with correct types") {
    val json =
      """{"type":"struct","fields":[
        |{"name":"a","type":"integer","nullable":false,"metadata":{}},
        |{"name":"b","type":"string","nullable":true,"metadata":{}},
        |{"name":"c","type":"decimal(10,2)","nullable":true,"metadata":{}}
        |]}""".stripMargin.replaceAll("\\s+", "")
    val parsed = UCDeltaSchemaConverter.parseSchemaString(json)
    assert(parsed.getFields.size() === 3)
    val a = parsed.getFields.get(0)
    assert(a.getName === "a")
    assert(a.getNullable === false)
    assert(a.getType.isInstanceOf[PrimitiveType])
    assert(a.getType.asInstanceOf[PrimitiveType].getType === "integer")
    // "decimal(p,s)" deserializes via DeltaTypeDeserializer into DecimalType (precision/scale).
    val c = parsed.getFields.get(2)
    assert(c.getType.isInstanceOf[DecimalType])
    assert(c.getType.asInstanceOf[DecimalType].getPrecision === 10)
    assert(c.getType.asInstanceOf[DecimalType].getScale === 2)
  }

  test("parseSchemaString: array, map, and nested struct round-trip") {
    val json =
      """{"type":"struct","fields":[
        |{"name":"arr","type":{"type":"array","elementType":"string","containsNull":true},
        |"nullable":true,"metadata":{}},
        |{"name":"m","type":{"type":"map","keyType":"string","valueType":"long",
        |"valueContainsNull":false},"nullable":true,"metadata":{}},
        |{"name":"nested","type":{"type":"struct","fields":[
        |{"name":"x","type":"integer","nullable":false,"metadata":{}}]},
        |"nullable":true,"metadata":{}}
        |]}""".stripMargin.replaceAll("\\s+", "")
    val parsed = UCDeltaSchemaConverter.parseSchemaString(json)
    assert(parsed.getFields.size() === 3)

    val arr = parsed.getFields.get(0).getType
    assert(arr.isInstanceOf[ArrayType])
    assert(arr.asInstanceOf[ArrayType].getContainsNull === true)
    assert(arr.asInstanceOf[ArrayType].getElementType.asInstanceOf[PrimitiveType].getType
      === "string")

    val m = parsed.getFields.get(1).getType
    assert(m.isInstanceOf[MapType])
    assert(m.asInstanceOf[MapType].getValueContainsNull === false)
    assert(m.asInstanceOf[MapType].getKeyType.asInstanceOf[PrimitiveType].getType === "string")
    assert(m.asInstanceOf[MapType].getValueType.asInstanceOf[PrimitiveType].getType === "long")

    val nested = parsed.getFields.get(2).getType
    assert(nested.isInstanceOf[StructType])
    val innerField = nested.asInstanceOf[StructType].getFields.get(0)
    assert(innerField.getName === "x")
    assert(innerField.getNullable === false)
    assert(innerField.getType.asInstanceOf[PrimitiveType].getType === "integer")
  }

  test("parseSchemaString: preserves per-field metadata at every nesting level") {
    val json =
      """{"type":"struct","fields":[
        |{"name":"outer","type":{"type":"struct","fields":[
        |{"name":"inner","type":"integer","nullable":false,
        |"metadata":{"comment":"inner-doc","delta.columnMapping.id":7}}
        |]},"nullable":true,
        |"metadata":{"comment":"outer-doc","delta.columnMapping.id":5}}
        |]}""".stripMargin.replaceAll("\\s+", "")
    val parsed = UCDeltaSchemaConverter.parseSchemaString(json)

    val outer = parsed.getFields.get(0)
    assert(outer.getMetadata.get("comment") === "outer-doc")
    assert(outer.getMetadata.get("delta.columnMapping.id")
      .asInstanceOf[Number].longValue === 5L)

    val inner = outer.getType.asInstanceOf[StructType].getFields.get(0)
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

  test("serializeSchema -> parseSchemaString round-trip: complex schema is structurally equal") {
    val arr = new ArrayType().elementType(prim("double")).containsNull(false)
    val map = new MapType()
      .keyType(prim("string"))
      .valueType(prim("date"))
      .valueContainsNull(true)
    val struct = new StructType()
      .addFieldsItem(field("inner_i", prim("integer"), nullable = false))
      .addFieldsItem(field("inner_s", prim("string")))
    val original = new StructType()
      .addFieldsItem(field("a", prim("integer"), nullable = false))
      .addFieldsItem(field("arr", arr))
      .addFieldsItem(field("m", map))
      .addFieldsItem(field("nested", struct))

    val json1 = UCDeltaSchemaConverter.serializeSchema(original)
    val reparsed = UCDeltaSchemaConverter.parseSchemaString(json1)
    val json2 = UCDeltaSchemaConverter.serializeSchema(reparsed)
    // Idempotent on the wire: serialize -> parse -> serialize yields the same JSON.
    assert(json1 === json2, s"round-trip changed JSON:\n  before=$json1\n  after =$json2")
  }

  // ----------------------------------------
  // toUCStructType (ColumnDef path)
  // ----------------------------------------

  /**
   * Build a primitive-typed ColumnDef from the (UC typeName, DDL typeText, Delta wire form)
   * triple. The Delta wire form is what flows into `typeJson` (which is the only thing the
   * converter consults); `typeText` is the DDL form a real caller like UCSingleCatalog
   * produces from {@code DataType.catalogString}.
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

  test("toUCStructType: primitive columns map to StructFields with PrimitiveType") {
    // Note: typeText carries DDL form ("int", "bigint"), typeJson carries Delta wire form
    // ("integer", "long"). The converter must consult typeJson, never typeText.
    val cols = Arrays.asList(
      col("a", typeName = "INT", typeText = "int", deltaWireType = "integer", nullable = false),
      col("b", typeName = "STRING", typeText = "string", deltaWireType = "string"),
      col("c", typeName = "DECIMAL", typeText = "decimal(10,2)",
        deltaWireType = "decimal(10,2)"))
    val s = UCDeltaSchemaConverter.toUCStructType(cols)
    assert(s.getFields.size() === 3)
    val a = s.getFields.get(0)
    assert(a.getName === "a")
    assert(a.getNullable === false)
    assert(a.getType.isInstanceOf[PrimitiveType])
    assert(a.getType.asInstanceOf[PrimitiveType].getType === "integer")
    // DECIMAL deserializes into DecimalType via DeltaTypeDeserializer's "decimal(p,s)" regex.
    val c = s.getFields.get(2)
    assert(c.getType.isInstanceOf[DecimalType])
    assert(c.getType.asInstanceOf[DecimalType].getPrecision === 10)
    assert(c.getType.asInstanceOf[DecimalType].getScale === 2)
  }

  test("toUCStructType: complex column with valid typeJson is parsed into the right subtype") {
    // typeJson follows Spark's StructField.json() shape: {name, type, nullable, metadata}.
    val arrayFieldJson =
      """{"name":"a","type":{"type":"array","elementType":"integer","containsNull":true},
        |"nullable":true,"metadata":{}}""".stripMargin.replaceAll("\\s+", "")
    val mapFieldJson =
      """{"name":"m","type":{"type":"map","keyType":"string","valueType":"long",
        |"valueContainsNull":false},"nullable":true,"metadata":{}}""".stripMargin
        .replaceAll("\\s+", "")
    val structFieldJson =
      """{"name":"s","type":{"type":"struct","fields":[{"name":"x","type":"integer",
        |"nullable":true,"metadata":{}}]},"nullable":false,"metadata":{}}""".stripMargin
        .replaceAll("\\s+", "")
    val cols = Arrays.asList(
      new UCClient.ColumnDef("a", "ARRAY", "array<int>", arrayFieldJson, true, 0),
      new UCClient.ColumnDef("m", "MAP", "map<string,bigint>", mapFieldJson, true, 1),
      new UCClient.ColumnDef("s", "STRUCT", "struct<x:int>", structFieldJson, false, 2))
    val s = UCDeltaSchemaConverter.toUCStructType(cols)
    assert(s.getFields.size() === 3)

    val a = s.getFields.get(0)
    assert(a.getType.isInstanceOf[ArrayType])
    assert(a.getType.asInstanceOf[ArrayType].getElementType
      .asInstanceOf[PrimitiveType].getType === "integer")

    val m = s.getFields.get(1)
    assert(m.getType.isInstanceOf[MapType])
    assert(m.getType.asInstanceOf[MapType].getValueContainsNull === false)

    val st = s.getFields.get(2)
    assert(st.getNullable === false)
    assert(st.getType.isInstanceOf[StructType])
    assert(st.getType.asInstanceOf[StructType].getFields.get(0).getName === "x")
  }

  test("toUCStructType: column with missing typeJson throws IllegalArgumentException") {
    val cols = Arrays.asList(
      new UCClient.ColumnDef("a", "INT", "int", null, true, 0))
    val e = intercept[IllegalArgumentException] {
      UCDeltaSchemaConverter.toUCStructType(cols)
    }
    assert(e.getMessage.contains("column 'a'"))
    assert(e.getMessage.contains("typeJson is empty"))
  }

  test("toUCStructType: column with malformed typeJson throws IllegalStateException") {
    val cols = Arrays.asList(
      new UCClient.ColumnDef("a", "STRUCT", "struct<x:int>", "{not valid json", true, 0))
    val e = intercept[IllegalStateException] {
      UCDeltaSchemaConverter.toUCStructType(cols)
    }
    assert(e.getMessage.contains("Failed to parse typeJson"))
    assert(e.getMessage.contains("column 'a'"))
  }

  test("toUCStructType: deeply nested column parses correctly and round-trips unchanged") {
    // Array<Struct< x: Integer, y: Map<String, Array<Long>> >> — exercises array-of-struct,
    // multi-field struct, map-with-complex-value, map-value-is-array, primitive at the leaf.
    val fieldJson =
      """{"name":"deep","type":{
        |"type":"array","containsNull":true,"elementType":{
        |"type":"struct","fields":[
        |{"name":"x","type":"integer","nullable":true,"metadata":{}},
        |{"name":"y","type":{"type":"map","keyType":"string","valueContainsNull":false,
        |"valueType":{"type":"array","elementType":"long","containsNull":true}},
        |"nullable":true,"metadata":{}}]}
        |},"nullable":true,"metadata":{}}""".stripMargin.replaceAll("\\s+", "")
    val cols = Arrays.asList(
      new UCClient.ColumnDef("deep", "ARRAY", "array<struct<...>>", fieldJson, true, 0))
    val s = UCDeltaSchemaConverter.toUCStructType(cols)
    assert(s.getFields.size() === 1)

    val outer = s.getFields.get(0).getType.asInstanceOf[ArrayType]
    assert(outer.getContainsNull === true)

    val elem = outer.getElementType.asInstanceOf[StructType]
    val innerFields = elem.getFields
    assert(innerFields.size() === 2)
    assert(innerFields.get(0).getName === "x")
    assert(innerFields.get(0).getType.asInstanceOf[PrimitiveType].getType === "integer")

    val yMap = innerFields.get(1).getType.asInstanceOf[MapType]
    assert(yMap.getKeyType.asInstanceOf[PrimitiveType].getType === "string")
    assert(yMap.getValueContainsNull === false)

    val yArr = yMap.getValueType.asInstanceOf[ArrayType]
    assert(yArr.getContainsNull === true)
    assert(yArr.getElementType.asInstanceOf[PrimitiveType].getType === "long")

    // Round-trip: parsed StructField must re-serialize to the same JSON tree. Compare as
    // JsonNode so the assertion is order-insensitive.
    val reserialized = UCDeltaSchemaConverter.serializeSchema(s)
    val expectedTree = objectMapper.readTree(s"""{"type":"struct","fields":[$fieldJson]}""")
    val actualTree = objectMapper.readTree(reserialized)
    assert(actualTree === expectedTree,
      s"round-trip changed JSON:\n  expected=$fieldJson\n  actual  =$reserialized")
  }

  test("toUCStructType: deeply nested struct-in-struct-in-struct preserves every level") {
    // Struct<l1: Struct<l2: Struct<l3: integer>>>
    val fieldJson =
      """{"name":"s","type":{
        |"type":"struct","fields":[
        |{"name":"l1","type":{"type":"struct","fields":[
        |{"name":"l2","type":{"type":"struct","fields":[
        |{"name":"l3","type":"integer","nullable":false,"metadata":{}}
        |]},"nullable":true,"metadata":{}}
        |]},"nullable":true,"metadata":{}}
        |]},"nullable":true,"metadata":{}}""".stripMargin.replaceAll("\\s+", "")
    val cols = Arrays.asList(
      new UCClient.ColumnDef("s", "STRUCT", "struct<l1:struct<...>>", fieldJson, true, 0))
    val s = UCDeltaSchemaConverter.toUCStructType(cols)

    val l0 = s.getFields.get(0).getType.asInstanceOf[StructType]
    val l1 = l0.getFields.get(0)
    assert(l1.getName === "l1")
    val l1Struct = l1.getType.asInstanceOf[StructType]
    val l2 = l1Struct.getFields.get(0)
    assert(l2.getName === "l2")
    val l2Struct = l2.getType.asInstanceOf[StructType]
    val l3 = l2Struct.getFields.get(0)
    assert(l3.getName === "l3")
    assert(l3.getNullable === false)
    assert(l3.getType.asInstanceOf[PrimitiveType].getType === "integer")
  }

  test("toUCStructType: Array<Array<Array<int>>> preserves containsNull at each level") {
    val fieldJson =
      """{"name":"aaa","type":{
        |"type":"array","containsNull":true,
        |"elementType":{"type":"array","containsNull":false,
        |"elementType":{"type":"array","containsNull":true,
        |"elementType":"integer"}}},
        |"nullable":true,"metadata":{}}""".stripMargin.replaceAll("\\s+", "")
    val cols = Arrays.asList(
      new UCClient.ColumnDef("aaa", "ARRAY", "array<array<array<int>>>", fieldJson, true, 0))
    val s = UCDeltaSchemaConverter.toUCStructType(cols)

    val outer = s.getFields.get(0).getType.asInstanceOf[ArrayType]
    assert(outer.getContainsNull === true)
    val mid = outer.getElementType.asInstanceOf[ArrayType]
    assert(mid.getContainsNull === false)
    val inner = mid.getElementType.asInstanceOf[ArrayType]
    assert(inner.getContainsNull === true)
    assert(inner.getElementType.asInstanceOf[PrimitiveType].getType === "integer")
  }

  test("toUCStructType: deeply nested Map<String, Map<String, Map<String, long>>>") {
    val fieldJson =
      """{"name":"mmm","type":{
        |"type":"map","keyType":"string","valueContainsNull":false,
        |"valueType":{"type":"map","keyType":"string","valueContainsNull":true,
        |"valueType":{"type":"map","keyType":"string","valueContainsNull":false,
        |"valueType":"long"}}},
        |"nullable":true,"metadata":{}}""".stripMargin.replaceAll("\\s+", "")
    val cols = Arrays.asList(
      new UCClient.ColumnDef("mmm", "MAP", "map<string,map<...>>", fieldJson, true, 0))
    val s = UCDeltaSchemaConverter.toUCStructType(cols)

    val outer = s.getFields.get(0).getType.asInstanceOf[MapType]
    assert(outer.getValueContainsNull === false)
    val mid = outer.getValueType.asInstanceOf[MapType]
    assert(mid.getValueContainsNull === true)
    val inner = mid.getValueType.asInstanceOf[MapType]
    assert(inner.getValueContainsNull === false)
    assert(inner.getValueType.asInstanceOf[PrimitiveType].getType === "long")
  }

  test("toUCStructType: complex schema with metadata preserved at every struct level") {
    // Struct<outer_id: integer (meta=outer_id), s: Struct<inner (meta=inner)>> with metadata
    // on each field at every level.
    val fieldJson =
      """{"name":"top","type":{
        |"type":"struct","fields":[
        |{"name":"outer_id","type":"integer","nullable":false,
        |"metadata":{"comment":"outer-doc","delta.columnMapping.id":1}},
        |{"name":"s","type":{"type":"struct","fields":[
        |{"name":"inner","type":"string","nullable":true,
        |"metadata":{"comment":"inner-doc","delta.columnMapping.id":2}}
        |]},"nullable":true,
        |"metadata":{"comment":"middle-doc","delta.columnMapping.id":3}}
        |]},"nullable":true,
        |"metadata":{"comment":"top-doc","delta.columnMapping.id":4}}""".stripMargin
        .replaceAll("\\s+", "")
    val cols = Arrays.asList(
      new UCClient.ColumnDef("top", "STRUCT", "struct<...>", fieldJson, true, 0))
    val s = UCDeltaSchemaConverter.toUCStructType(cols)

    val top = s.getFields.get(0)
    assert(top.getMetadata.get("comment") === "top-doc")
    assert(top.getMetadata.get("delta.columnMapping.id").asInstanceOf[Number].longValue === 4L)

    val topStruct = top.getType.asInstanceOf[StructType]
    val outerId = topStruct.getFields.get(0)
    assert(outerId.getMetadata.get("comment") === "outer-doc")
    assert(outerId.getMetadata.get("delta.columnMapping.id")
      .asInstanceOf[Number].longValue === 1L)

    val sField = topStruct.getFields.get(1)
    assert(sField.getMetadata.get("comment") === "middle-doc")

    val innerStruct = sField.getType.asInstanceOf[StructType]
    val innerField = innerStruct.getFields.get(0)
    assert(innerField.getMetadata.get("comment") === "inner-doc")
    assert(innerField.getMetadata.get("delta.columnMapping.id")
      .asInstanceOf[Number].longValue === 2L)
  }

  test("toUCStructType: multi-column schema with varied complex types in one call") {
    val arrField =
      """{"name":"a","type":{"type":"array","elementType":"integer","containsNull":true},
        |"nullable":true,"metadata":{}}""".stripMargin.replaceAll("\\s+", "")
    val mapOfStructField =
      """{"name":"m","type":{"type":"map","keyType":"string",
        |"valueType":{"type":"struct","fields":[
        |{"name":"k","type":"string","nullable":true,"metadata":{}}]},
        |"valueContainsNull":false},"nullable":true,"metadata":{}}""".stripMargin
        .replaceAll("\\s+", "")
    val arrOfArrField =
      """{"name":"aa","type":{"type":"array",
        |"elementType":{"type":"array","elementType":"long","containsNull":true},
        |"containsNull":false},"nullable":true,"metadata":{}}""".stripMargin
        .replaceAll("\\s+", "")
    val cols = Arrays.asList(
      new UCClient.ColumnDef("a", "ARRAY", "array<int>", arrField, true, 0),
      new UCClient.ColumnDef("m", "MAP", "map<string,struct<...>>", mapOfStructField, true, 1),
      new UCClient.ColumnDef("aa", "ARRAY", "array<array<long>>", arrOfArrField, true, 2))
    val s = UCDeltaSchemaConverter.toUCStructType(cols)
    assert(s.getFields.size() === 3)

    val a = s.getFields.get(0).getType.asInstanceOf[ArrayType]
    assert(a.getElementType.asInstanceOf[PrimitiveType].getType === "integer")

    val m = s.getFields.get(1).getType.asInstanceOf[MapType]
    val mValue = m.getValueType.asInstanceOf[StructType]
    assert(mValue.getFields.get(0).getName === "k")
    assert(mValue.getFields.get(0).getType.asInstanceOf[PrimitiveType].getType === "string")

    val aa = s.getFields.get(2).getType.asInstanceOf[ArrayType]
    val aaInner = aa.getElementType.asInstanceOf[ArrayType]
    assert(aaInner.getElementType.asInstanceOf[PrimitiveType].getType === "long")
  }

  test("toUCStructType: preserves per-field metadata from typeJson") {
    val fieldJsonWithMeta =
      """{"name":"annotated","type":"integer","nullable":true,
        |"metadata":{"comment":"docs","delta.columnMapping.id":42}}""".stripMargin
        .replaceAll("\\s+", "")
    val cols = Arrays.asList(
      new UCClient.ColumnDef("annotated", "INT", "int", fieldJsonWithMeta, true, 0))
    val s = UCDeltaSchemaConverter.toUCStructType(cols)
    val f = s.getFields.get(0)
    assert(f.getName === "annotated")
    val meta = f.getMetadata
    assert(meta != null && !meta.isEmpty, "metadata must be preserved from typeJson")
    assert(meta.get("comment") === "docs")
    assert(meta.get("delta.columnMapping.id").asInstanceOf[Number].longValue === 42L)
  }

  test("toUCStructType: typeJson without a 'type' field throws IllegalStateException") {
    // E.g. a bare type JSON ("integer") or an empty object — neither is StructField-shaped.
    val cols = Arrays.asList(
      new UCClient.ColumnDef("a", "INT", "int", "{}", true, 0))
    val e = intercept[IllegalStateException] {
      UCDeltaSchemaConverter.toUCStructType(cols)
    }
    assert(e.getMessage.contains("missing the 'type' field"))
    assert(e.getMessage.contains("column 'a'"))
  }

  test("toUCStructType: every primitive's wire form comes from typeJson, not the DDL typeText") {
    primitiveExamples.foreach { case (typeName, ddl, wire) =>
      val column =
        col(name = "x", typeName = typeName, typeText = ddl, deltaWireType = wire)
      val s = UCDeltaSchemaConverter.toUCStructType(Collections.singletonList(column))
      assert(s.getFields.size() === 1, s"typeName=$typeName: expected 1 field")
      val t = s.getFields.get(0).getType
      // DECIMAL goes to DecimalType via the deserializer's decimal regex; all others to
      // PrimitiveType with the bare wire-form string.
      if (typeName == "DECIMAL") {
        assert(t.isInstanceOf[DecimalType], s"typeName=$typeName: expected DecimalType")
      } else {
        assert(t.isInstanceOf[PrimitiveType], s"typeName=$typeName: expected PrimitiveType")
        assert(t.asInstanceOf[PrimitiveType].getType === wire,
          s"typeName=$typeName: wire form mismatch (got the DDL form instead?)")
        // Direct regression guard for the old fast-path bug.
        if (ddl != wire) {
          assert(t.asInstanceOf[PrimitiveType].getType !== ddl,
            s"typeName=$typeName: DDL form '$ddl' leaked into the output")
        }
      }
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

  // ----------------------------------------
  // Nested struct as a field type (Delta's second-most-common shape)
  // ----------------------------------------

  test("serializeSchema: struct as a field type serializes recursively") {
    val inner = new StructType()
      .addFieldsItem(field("x", prim("integer")))
      .addFieldsItem(field("y", prim("string"), nullable = false))
    val s = new StructType().addFieldsItem(field("nested", inner))
    val parsed = objectMapper.readTree(UCDeltaSchemaConverter.serializeSchema(s))
    val innerJson = parsed.get("fields").get(0).get("type")
    assert(innerJson.get("type").asText() === "struct")
    val innerFields = innerJson.get("fields")
    assert(innerFields.size() === 2)
    assert(innerFields.get(0).get("name").asText() === "x")
    assert(innerFields.get(0).get("type").asText() === "integer")
    assert(innerFields.get(1).get("name").asText() === "y")
    assert(innerFields.get(1).get("nullable").asBoolean() === false)
    assert(innerFields.get(1).get("type").asText() === "string")
  }

  test("serializeSchema: array of struct preserves the inner struct's fields") {
    val inner = new StructType()
      .addFieldsItem(field("k", prim("string")))
      .addFieldsItem(field("v", prim("long"), nullable = false))
    val arr = new ArrayType().elementType(inner).containsNull(false)
    val s = new StructType().addFieldsItem(field("arr_of_struct", arr))
    val parsed = objectMapper.readTree(UCDeltaSchemaConverter.serializeSchema(s))
    val arrJson = parsed.get("fields").get(0).get("type")
    assert(arrJson.get("type").asText() === "array")
    assert(arrJson.get("containsNull").asBoolean() === false)
    val innerJson = arrJson.get("elementType")
    assert(innerJson.get("type").asText() === "struct")
    val innerFields = innerJson.get("fields")
    assert(innerFields.size() === 2)
    assert(innerFields.get(0).get("name").asText() === "k")
    assert(innerFields.get(1).get("name").asText() === "v")
    assert(innerFields.get(1).get("nullable").asBoolean() === false)
  }

  test("serializeSchema: map with struct value preserves nested struct fields") {
    val struct = new StructType().addFieldsItem(field("nested_x", prim("date")))
    val map = new MapType()
      .keyType(prim("string"))
      .valueType(struct)
      .valueContainsNull(true)
    val s = new StructType().addFieldsItem(field("m", map))
    val parsed = objectMapper.readTree(UCDeltaSchemaConverter.serializeSchema(s))
    val mapJson = parsed.get("fields").get(0).get("type")
    assert(mapJson.get("type").asText() === "map")
    assert(mapJson.get("keyType").asText() === "string")
    assert(mapJson.get("valueContainsNull").asBoolean() === true)
    val valueStruct = mapJson.get("valueType")
    assert(valueStruct.get("type").asText() === "struct")
    assert(valueStruct.get("fields").get(0).get("name").asText() === "nested_x")
    assert(valueStruct.get("fields").get(0).get("type").asText() === "date")
  }

  test("serializeSchema: map with array value (complex value, not just complex outer)") {
    val arr = new ArrayType().elementType(prim("long")).containsNull(false)
    val map = new MapType().keyType(prim("string")).valueType(arr).valueContainsNull(false)
    val s = new StructType().addFieldsItem(field("m_of_arr", map))
    val parsed = objectMapper.readTree(UCDeltaSchemaConverter.serializeSchema(s))
    val mapJson = parsed.get("fields").get(0).get("type")
    assert(mapJson.get("type").asText() === "map")
    val valueArr = mapJson.get("valueType")
    assert(valueArr.get("type").asText() === "array")
    assert(valueArr.get("elementType").asText() === "long")
    assert(valueArr.get("containsNull").asBoolean() === false)
  }

  test("serializeSchema: three-level nesting (Array<Array<Map<string,long>>>) round-trips") {
    val innermost = new MapType()
      .keyType(prim("string"))
      .valueType(prim("long"))
      .valueContainsNull(true)
    val middle = new ArrayType().elementType(innermost).containsNull(false)
    val outer = new ArrayType().elementType(middle).containsNull(true)
    val s = new StructType().addFieldsItem(field("deep", outer))
    val parsed = objectMapper.readTree(UCDeltaSchemaConverter.serializeSchema(s))
    val outerJson = parsed.get("fields").get(0).get("type")
    assert(outerJson.get("type").asText() === "array")
    assert(outerJson.get("containsNull").asBoolean() === true)
    val middleJson = outerJson.get("elementType")
    assert(middleJson.get("type").asText() === "array")
    assert(middleJson.get("containsNull").asBoolean() === false)
    val innermostJson = middleJson.get("elementType")
    assert(innermostJson.get("type").asText() === "map")
    assert(innermostJson.get("keyType").asText() === "string")
    assert(innermostJson.get("valueType").asText() === "long")
    assert(innermostJson.get("valueContainsNull").asBoolean() === true)
  }

  // ----------------------------------------
  // StructField.metadata round-trip — Delta wire format carries arbitrary per-field metadata
  // (e.g. comment, delta.columnMapping.id) and dropping it would silently corrupt schemas.
  // ----------------------------------------

  test("serializeSchema: per-field metadata flows through to JSON output") {
    val metadata = new JHashMap[String, Object]()
    metadata.put("comment", "user-facing column doc")
    metadata.put("delta.columnMapping.id", java.lang.Long.valueOf(42L))
    val fieldWithMeta = new StructField()
      .name("annotated")
      .nullable(true)
      .`type`(prim("integer"))
      .metadata(metadata)
    val s = new StructType().addFieldsItem(fieldWithMeta)

    val parsed = objectMapper.readTree(UCDeltaSchemaConverter.serializeSchema(s))
    val fields = parsed.get("fields")
    assert(fields.get(0).get("name").asText() === "annotated")
    val metaJson = fields.get(0).get("metadata")
    assert(metaJson != null, "metadata field must be present in JSON output")
    assert(metaJson.isObject)
    assert(metaJson.get("comment").asText() === "user-facing column doc")
    assert(metaJson.get("delta.columnMapping.id").asLong() === 42L)
  }

  test("serializeSchema: empty metadata still emits an empty object (not omitted)") {
    val s = new StructType().addFieldsItem(field("a", prim("integer")))
    val parsed = objectMapper.readTree(UCDeltaSchemaConverter.serializeSchema(s))
    val metaJson = parsed.get("fields").get(0).get("metadata")
    // Delta's reader expects a metadata object on every field; omission can be ambiguous.
    assert(metaJson != null, "metadata field must always be present")
    assert(metaJson.isObject)
    assert(metaJson.size() === 0)
  }

  // ----------------------------------------
  // containsNull / valueContainsNull = null — Delta's reader rejects "containsNull":null,
  // so it matters whether the mapper emits the key or omits it.
  // ----------------------------------------

  test("serializeSchema: array with null containsNull omits the JSON key") {
    val arr = new ArrayType().elementType(prim("string")) // containsNull deliberately unset
    val s = new StructType().addFieldsItem(field("a", arr))
    val parsed = objectMapper.readTree(UCDeltaSchemaConverter.serializeSchema(s))
    val arrJson = parsed.get("fields").get(0).get("type")
    // Pin the Delta-reader contract: the key is either absent or non-null. A literal
    // "containsNull":null breaks Delta's schema reader.
    val node = arrJson.get("containsNull")
    assert(node == null || !node.isNull,
      s"unexpected null value for containsNull in: $arrJson")
  }

  test("serializeSchema: map with null valueContainsNull omits the JSON key") {
    val m = new MapType().keyType(prim("string")).valueType(prim("integer"))
    val s = new StructType().addFieldsItem(field("m", m))
    val parsed = objectMapper.readTree(UCDeltaSchemaConverter.serializeSchema(s))
    val mapJson = parsed.get("fields").get(0).get("type")
    val node = mapJson.get("valueContainsNull")
    assert(node == null || !node.isNull,
      s"unexpected null value for valueContainsNull in: $mapJson")
  }

  // ----------------------------------------
  // Every primitive must round-trip inside both Array and Map containers
  // (the camelCase mixins must not lose type fidelity for any primitive).
  // ----------------------------------------

  test("serializeSchema: every primitive round-trips inside ArrayType") {
    primitiveExamples.foreach { case (_, _, p) =>
      val arr = new ArrayType().elementType(prim(p)).containsNull(true)
      val s = new StructType().addFieldsItem(field("a", arr))
      val parsed = objectMapper.readTree(UCDeltaSchemaConverter.serializeSchema(s))
      val t = parsed.get("fields").get(0).get("type")
      assert(t.get("type").asText() === "array", s"primitive=$p")
      assert(t.get("elementType").asText() === p, s"primitive=$p: elementType wire form mismatch")
    }
  }

  test("serializeSchema: every primitive round-trips as MapType valueType") {
    primitiveExamples.foreach { case (_, _, p) =>
      val m = new MapType().keyType(prim("string")).valueType(prim(p)).valueContainsNull(true)
      val s = new StructType().addFieldsItem(field("m", m))
      val parsed = objectMapper.readTree(UCDeltaSchemaConverter.serializeSchema(s))
      val t = parsed.get("fields").get(0).get("type")
      assert(t.get("type").asText() === "map", s"primitive=$p")
      assert(t.get("valueType").asText() === p, s"primitive=$p: valueType wire form mismatch")
    }
  }
}
