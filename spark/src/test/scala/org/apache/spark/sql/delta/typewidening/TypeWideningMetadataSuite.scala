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

package org.apache.spark.sql.delta.typewidening

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils.propertyKey
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.JsonUtils

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.types._

/**
 * Suite that covers recording type change metadata in the table schema.
 */
class TypeWideningMetadataSuite
  extends QueryTest
    with TypeWideningTestMixin
    with TypeWideningMetadataTests
    with TypeWideningMetadataEndToEndTests

/**
 * Tests covering the [[TypeWideningMetadata]] and [[TypeChange]] classes used to handle the
 * metadata recorded by the Type Widening table feature in the table schema.
 */
trait TypeWideningMetadataTests extends QueryTest with DeltaSQLCommandTest {
  private val testTableName: String = "delta_type_widening_metadata_test"

  /** A dummy transaction to be used by tests covering `addTypeWideningMetadata`. */
  private lazy val txn: OptimisticTransaction = {
    val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier(testTableName))
    DeltaLog.forTable(spark, TableIdentifier(testTableName))
        .startTransaction(catalogTableOpt = Some(table))
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    sql(s"CREATE TABLE $testTableName (a int) USING delta TBLPROPERTIES (" +
      s"'${DeltaConfigs.ENABLE_TYPE_WIDENING.key}' = 'true', " +
      s"'${propertyKey(TypeWideningTableFeature)}' = 'supported')")
  }

  override protected def afterAll(): Unit = {
    sql(s"DROP TABLE IF EXISTS $testTableName")
    super.afterAll()
  }

  /**
   * Short-hand to build the metadata for a type change to cut down on repetition.
   */
  private def typeChangeMetadata(
      fromType: String,
      toType: String,
      path: String = ""): Metadata = {
    val builder = new MetadataBuilder()
      .putString("fromType", fromType)
      .putString("toType", toType)
    if (path.nonEmpty) {
      builder.putString("fieldPath", path)
    }
    builder.build()
  }

  test("toMetadata/fromMetadata with empty path") {
    val typeChange = TypeChange(version = None, IntegerType, LongType, Seq.empty)
    assert(typeChange.toMetadata === typeChangeMetadata("integer", "long"))
    assert(TypeChange.fromMetadata(typeChange.toMetadata) === typeChange)
  }

  test("toMetadata/fromMetadata with non-empty path") {
    val typeChange =
      TypeChange(version = None, DateType, TimestampNTZType, Seq("key", "element"))
    assert(typeChange.toMetadata ===
      typeChangeMetadata("date", "timestamp_ntz", "key.element"))
    assert(TypeChange.fromMetadata(typeChange.toMetadata) === typeChange)
  }

  test("toMetadata/fromMetadata with tableVersion") {
    val typeChange = TypeChange(version = Some(1), ByteType, ShortType, Seq.empty)
    val expectedMetadata = new MetadataBuilder()
      .putLong("tableVersion", 1)
      .putString("fromType", "byte")
      .putString("toType", "short")
      .build()
    assert(typeChange.toMetadata === expectedMetadata)
    assert(TypeChange.fromMetadata(typeChange.toMetadata) === typeChange)
  }

  test("fromField with no type widening metadata") {
    val field = StructField("a", IntegerType)
    assert(TypeWideningMetadata.fromField(field) === None)
  }

  test("fromField with empty type widening metadata") {
    val field = StructField("a", IntegerType, metadata = new MetadataBuilder()
      .putMetadataArray("delta.typeChanges", Array.empty[Metadata])
      .build()
    )
    assert(TypeWideningMetadata.fromField(field) === Some(TypeWideningMetadata(Seq.empty)))
    val otherField = StructField("a", IntegerType)
    // Empty type widening metadata is discarded.
    assert(TypeWideningMetadata.fromField(field).get.appendToField(otherField) ===
      StructField("a", IntegerType))
  }

  test("fromField with single type change") {
    val field = StructField("a", IntegerType, metadata = new MetadataBuilder()
      .putMetadataArray("delta.typeChanges", Array(
        typeChangeMetadata("integer", "long")
      )).build()
    )
    assert(TypeWideningMetadata.fromField(field) ===
      Some(TypeWideningMetadata(Seq(
        TypeChange(version = None, IntegerType, LongType, Seq.empty)))))
    val otherField = StructField("a", IntegerType)
    assert(TypeWideningMetadata.fromField(field).get.appendToField(otherField) === field)
  }

  test("fromField with multiple type changes") {
    val field = StructField("a", IntegerType, metadata = new MetadataBuilder()
      .putMetadataArray("delta.typeChanges", Array(
        typeChangeMetadata("integer", "long"),
        typeChangeMetadata("decimal(5,0)", "decimal(10,2)", "element.element")
      )).build()
    )
    assert(TypeWideningMetadata.fromField(field) ===
      Some(TypeWideningMetadata(Seq(
        TypeChange(version = None, IntegerType, LongType, Seq.empty),
        TypeChange(
          version = None, DecimalType(5, 0), DecimalType(10, 2), Seq("element", "element"))))))
    val otherField = StructField("a", IntegerType)
    assert(TypeWideningMetadata.fromField(field).get.appendToField(otherField) === field)
  }

  test("fromField with tableVersion") {
    val typeChange = new MetadataBuilder()
      .putLong("tableVersion", 1)
      .putString("fromType", "integer")
      .putString("toType", "long")
      .build()
    val field = StructField("a", IntegerType, metadata = new MetadataBuilder()
      .putMetadataArray("delta.typeChanges", Array(typeChange))
      .build()
    )
    assert(TypeWideningMetadata.fromField(field) ===
      Some(TypeWideningMetadata(Seq(
        TypeChange(version = Some(1), IntegerType, LongType, Seq.empty)))))
    val otherField = StructField("a", IntegerType)
    assert(TypeWideningMetadata.fromField(field).get.appendToField(otherField) === field)
  }

  test("appendToField on field with no type widening metadata") {
    val field = StructField("a", IntegerType)
    // Adding empty type widening metadata should not change the field.
    val emptyMetadata = TypeWideningMetadata(Seq.empty)
    assert(emptyMetadata.appendToField(field) === field)
    assert(TypeWideningMetadata.fromField(emptyMetadata.appendToField(field)).isEmpty)

    // Adding single type change should add the metadata to the field and not otherwise change it.
    val singleMetadata = TypeWideningMetadata(Seq(
      TypeChange(version = None, IntegerType, LongType, Seq.empty)))
    assert(singleMetadata.appendToField(field) === field.copy(metadata =
      new MetadataBuilder()
        .putMetadataArray("delta.typeChanges", Array(
          typeChangeMetadata("integer", "long")
        )).build()
      )
    )
    val singleMetadataFromField =
      TypeWideningMetadata.fromField(singleMetadata.appendToField(field))
    assert(singleMetadataFromField.contains(singleMetadata))

    // Adding multiple type changes should add the metadata to the field and not otherwise change
    // it.
    val multipleMetadata = TypeWideningMetadata(Seq(
      TypeChange(version = None, IntegerType, LongType, Seq.empty),
      TypeChange(version = None, FloatType, DoubleType, Seq("value"))))
    assert(multipleMetadata.appendToField(field) === field.copy(metadata =
      new MetadataBuilder()
        .putMetadataArray("delta.typeChanges", Array(
          typeChangeMetadata("integer", "long"),
          typeChangeMetadata("float", "double", "value")
        )).build()
      )
    )

    val multipleMetadataFromField =
      TypeWideningMetadata.fromField(multipleMetadata.appendToField(field))
    assert(multipleMetadataFromField.contains(multipleMetadata))
  }

  test("appendToField on field with existing type widening metadata") {
    val field = StructField("a", IntegerType,
      metadata = new MetadataBuilder()
        .putMetadataArray("delta.typeChanges", Array(
          typeChangeMetadata("integer", "long")
        )).build()
    )
    // Adding empty type widening metadata should not change the field.
    val emptyMetadata = TypeWideningMetadata(Seq.empty)
    assert(emptyMetadata.appendToField(field) === field)
    assert(TypeWideningMetadata.fromField(emptyMetadata.appendToField(field)).contains(
      TypeWideningMetadata(Seq(
        TypeChange(version = None, IntegerType, LongType, Seq.empty)))
    ))

    // Adding single type change should add the metadata to the field and not otherwise change it.
    val singleMetadata = TypeWideningMetadata(Seq(
      TypeChange(version = None, DecimalType(18, 0), DecimalType(19, 0), Seq.empty)))

    assert(singleMetadata.appendToField(field) === field.copy(
      metadata = new MetadataBuilder()
        .putMetadataArray("delta.typeChanges", Array(
          typeChangeMetadata("integer", "long"),
          typeChangeMetadata("decimal(18,0)", "decimal(19,0)")
        )).build()
    ))
    val singleMetadataFromField =
      TypeWideningMetadata.fromField(singleMetadata.appendToField(field))

    assert(singleMetadataFromField.contains(TypeWideningMetadata(Seq(
      TypeChange(version = None, IntegerType, LongType, Seq.empty),
      TypeChange(version = None, DecimalType(18, 0), DecimalType(19, 0), Seq.empty)))
    ))

    // Adding multiple type changes should add the metadata to the field and not otherwise change
    // it.
    val multipleMetadata = TypeWideningMetadata(Seq(
      TypeChange(version = None, DecimalType(18, 0), DecimalType(19, 0), Seq.empty),
      TypeChange(version = None, FloatType, DoubleType, Seq("value"))))

    assert(multipleMetadata.appendToField(field) === field.copy(
      metadata = new MetadataBuilder()
        .putMetadataArray("delta.typeChanges", Array(
          typeChangeMetadata("integer", "long"),
          typeChangeMetadata("decimal(18,0)", "decimal(19,0)"),
          typeChangeMetadata("float", "double", "value")
        )).build()
    ))
    val multipleMetadataFromField =
      TypeWideningMetadata.fromField(multipleMetadata.appendToField(field))

    assert(multipleMetadataFromField.contains(TypeWideningMetadata(Seq(
      TypeChange(version = None, IntegerType, LongType, Seq.empty),
      TypeChange(version = None, DecimalType(18, 0), DecimalType(19, 0), Seq.empty),
      TypeChange(version = None, FloatType, DoubleType, Seq("value"))))
    ))
  }

  test("addTypeWideningMetadata/removeTypeWideningMetadata with no type changes") {
    for {
      (oldSchema, newSchema) <- Seq(
        ("a short", "a short"),
        ("a short", "a short NOT NULL"),
        ("a short NOT NULL", "a short"),
        ("a short NOT NULL", "a short COMMENT 'a comment'"),
        ("a string, b int", "b int, a string"),
        ("a struct<s1: date, s2: long>", "a struct<s2: long, s1: date>"),
        ("a struct<s1: short COMMENT 'a comment'>", "a struct<s1: short>"),
        ("a struct<s1: short>", "a struct<s1: short COMMENT 'a comment'>"),
        ("a map<int, long>", "m map<int, long>"),
        ("a array<timestamp>", "a array<timestamp>"),
        ("a map<array<timestamp>, int>", "a map<array<timestamp>, int>"),
        ("a array<struct<s1: byte>>", "a array<struct<s1: byte>>")
      ).map { case (oldStr, newStr) => StructType.fromDDL(oldStr) -> StructType.fromDDL(newStr) }
    } {
      withClue(s"oldSchema = $oldSchema, newSchema = $newSchema") {
        val schema = TypeWideningMetadata.addTypeWideningMetadata(txn, newSchema, oldSchema)
        assert(schema === newSchema)
        assert(TypeWideningMetadata.removeTypeWideningMetadata(schema) === schema -> Seq.empty)
      }
    }
  }

  test("addTypeWideningMetadata/removeTypeWideningMetadata on top-level fields") {
    val schemaWithoutMetadata =
      StructType.fromDDL("i long, d decimal(15, 4), a array<double>, m map<short, int>")
    val firstOldSchema =
      StructType.fromDDL("i short, d decimal(6, 2), a array<byte>, m map<byte, int>")
    val secondOldSchema =
      StructType.fromDDL("i int, d decimal(10, 4), a array<int>, m map<short, byte>")

    var schema =
      TypeWideningMetadata.addTypeWideningMetadata(txn, schemaWithoutMetadata, firstOldSchema)

    assert(schema("i") === StructField("i", LongType,
      metadata = new MetadataBuilder()
        .putMetadataArray("delta.typeChanges", Array(
          typeChangeMetadata("short", "long")
        )).build()
    ))

    assert(schema("d") === StructField("d", DecimalType(15, 4),
      metadata = new MetadataBuilder()
        .putMetadataArray("delta.typeChanges", Array(
          typeChangeMetadata("decimal(6,2)", "decimal(15,4)")
        )).build()
    ))

    assert(schema("a") === StructField("a", ArrayType(DoubleType),
      metadata = new MetadataBuilder()
        .putMetadataArray("delta.typeChanges", Array(
          typeChangeMetadata("byte", "double", "element")
        )).build()
    ))

    assert(schema("m") === StructField("m", MapType(ShortType, IntegerType),
      metadata = new MetadataBuilder()
        .putMetadataArray("delta.typeChanges", Array(
          typeChangeMetadata("byte", "short", "key")
        )).build()
    ))

    assert(TypeWideningMetadata.removeTypeWideningMetadata(schema) ===
      schemaWithoutMetadata -> Seq(
        Seq.empty -> schema("i"),
        Seq.empty -> schema("d"),
        Seq.empty -> schema("a"),
        Seq.empty -> schema("m")
      ))
    // Second type change on all fields.
    schema = TypeWideningMetadata.addTypeWideningMetadata(txn, schema, secondOldSchema)

    assert(schema("i") === StructField("i", LongType,
      metadata = new MetadataBuilder()
        .putMetadataArray("delta.typeChanges", Array(
          typeChangeMetadata("short", "long"),
          typeChangeMetadata("integer", "long")
        )).build()
    ))

    assert(schema("d") === StructField("d", DecimalType(15, 4),
      metadata = new MetadataBuilder()
        .putMetadataArray("delta.typeChanges", Array(
          typeChangeMetadata("decimal(6,2)", "decimal(15,4)"),
          typeChangeMetadata("decimal(10,4)", "decimal(15,4)")
        )).build()
    ))

    assert(schema("a") === StructField("a", ArrayType(DoubleType),
      metadata = new MetadataBuilder()
        .putMetadataArray("delta.typeChanges", Array(
          typeChangeMetadata("byte", "double", "element"),
          typeChangeMetadata("integer", "double", "element")
        )).build()
    ))

    assert(schema("m") === StructField("m", MapType(ShortType, IntegerType),
      metadata = new MetadataBuilder()
        .putMetadataArray("delta.typeChanges", Array(
          typeChangeMetadata("byte", "short", "key"),
          typeChangeMetadata("byte", "integer", "value")
        )).build()
    ))

    assert(TypeWideningMetadata.removeTypeWideningMetadata(schema) ===
      schemaWithoutMetadata -> Seq(
        Seq.empty -> schema("i"),
        Seq.empty -> schema("d"),
        Seq.empty -> schema("a"),
        Seq.empty -> schema("m")
      ))
  }

  test("addTypeWideningMetadata/removeTypeWideningMetadata on nested fields") {
    val schemaWithoutMetadata = StructType.fromDDL(
      "s struct<i: long, a: array<map<int, long>>, m: map<map<long, int>, array<long>>>")
    val firstOldSchema = StructType.fromDDL(
      "s struct<i: short, a: array<map<byte, long>>, m: map<map<int, int>, array<long>>>")
    val secondOldSchema = StructType.fromDDL(
      "s struct<i: int, a: array<map<int, int>>, m: map<map<long, int>, array<int>>>")

    // First type change on all struct fields.
    var schema =
      TypeWideningMetadata.addTypeWideningMetadata(txn, schemaWithoutMetadata, firstOldSchema)
    var struct = schema("s").dataType.asInstanceOf[StructType]

    assert(struct("i") === StructField("i", LongType,
      metadata = new MetadataBuilder()
        .putMetadataArray("delta.typeChanges", Array(
          typeChangeMetadata("short", "long")
        )).build()
    ))

    assert(struct("a") === StructField("a", ArrayType(MapType(IntegerType, LongType)),
      metadata = new MetadataBuilder()
        .putMetadataArray("delta.typeChanges", Array(
          typeChangeMetadata("byte", "integer", "element.key")
        )).build()
    ))

    assert(struct("m") === StructField("m",
      MapType(MapType(LongType, IntegerType), ArrayType(LongType)),
      metadata = new MetadataBuilder()
        .putMetadataArray("delta.typeChanges", Array(
          typeChangeMetadata("integer", "long", "key.key")
        )).build()
    ))

    assert(TypeWideningMetadata.removeTypeWideningMetadata(schema) ===
      schemaWithoutMetadata -> Seq(
        Seq("s") -> struct("i"),
        Seq("s") -> struct("a"),
        Seq("s") -> struct("m")
      ))

    // Second type change on all struct fields.
    schema = TypeWideningMetadata.addTypeWideningMetadata(txn, schema, secondOldSchema)
    struct = schema("s").dataType.asInstanceOf[StructType]

    assert(struct("i") === StructField("i", LongType,
      metadata = new MetadataBuilder()
        .putMetadataArray("delta.typeChanges", Array(
          typeChangeMetadata("short", "long"),
          typeChangeMetadata("integer", "long")
        )).build()
    ))

    assert(struct("a") === StructField("a", ArrayType(MapType(IntegerType, LongType)),
      metadata = new MetadataBuilder()
        .putMetadataArray("delta.typeChanges", Array(
          typeChangeMetadata("byte", "integer", "element.key"),
          typeChangeMetadata("integer", "long", "element.value")
        )).build()
    ))

    assert(struct("m") === StructField("m",
      MapType(MapType(LongType, IntegerType), ArrayType(LongType)),
      metadata = new MetadataBuilder()
        .putMetadataArray("delta.typeChanges", Array(
          typeChangeMetadata("integer", "long", "key.key"),
          typeChangeMetadata("integer", "long", "value.element")
        )).build()
    ))
    assert(TypeWideningMetadata.removeTypeWideningMetadata(schema) ===
      schemaWithoutMetadata -> Seq(
        Seq("s") -> struct("i"),
        Seq("s") -> struct("a"),
        Seq("s") -> struct("m")
      ))
  }

  test("addTypeWideningMetadata/removeTypeWideningMetadata with added and removed fields") {
    val newSchema = StructType.fromDDL("a int, b long, d int")
    val oldSchema = StructType.fromDDL("a int, b int, c int")

    val schema = TypeWideningMetadata.addTypeWideningMetadata(txn, newSchema, oldSchema)
    assert(schema("a") === StructField("a", IntegerType))
    assert(schema("d") === StructField("d", IntegerType))
    assert(!schema.contains("c"))

    assert(schema("b") === StructField("b", LongType,
      metadata = new MetadataBuilder()
        .putMetadataArray("delta.typeChanges", Array(
          typeChangeMetadata("integer", "long")
        )).build()
    ))
    assert(TypeWideningMetadata.removeTypeWideningMetadata(schema) ===
      newSchema -> Seq(Seq.empty -> schema("b"))
    )
  }

  test("addTypeWideningMetadata/removeTypeWideningMetadata with different field position") {
    val newSchema = StructType.fromDDL("a short, b int, s struct<c: int, d: long>")
    val oldSchema = StructType.fromDDL("b int, a short, s struct<d: long, c: int>")

    val schema = TypeWideningMetadata.addTypeWideningMetadata(txn, newSchema, oldSchema)
    // No type widening metadata is added.
    assert(schema("a") === StructField("a", ShortType))
    assert(schema("b") === StructField("b", IntegerType))
    assert(schema("s") ===
      StructField("s", new StructType()
        .add("c", IntegerType)
        .add("d", LongType)))
    assert(TypeWideningMetadata.removeTypeWideningMetadata(schema) === newSchema -> Seq.empty)
  }

  test("updateTypeChangeVersion with no type changes") {
    val schema = new StructType().add("a", IntegerType)
    assert(TypeWideningMetadata.updateTypeChangeVersion(schema, 1, 4) === schema)
  }

  test("updateTypeChangeVersion with field with single type change") {
    val schema = new StructType()
      .add("a", IntegerType, nullable = true, metadata = new MetadataBuilder()
        .putMetadataArray("delta.typeChanges", Array(
          typeChangeMetadata("integer", "long")
        ))
        .build()
      )

    assert(TypeWideningMetadata.updateTypeChangeVersion(schema, 1, 4) ===
      new StructType()
        .add("a", IntegerType, nullable = true, metadata = new MetadataBuilder()
          .putMetadataArray("delta.typeChanges", Array(
            typeChangeMetadata("integer", "long")
          ))
          .build()
        )
    )
  }

  test("updateTypeChangeVersion with field with multiple type changes") {
    val schema = new StructType()
      .add("a", IntegerType, nullable = true, metadata = new MetadataBuilder()
          .putMetadataArray("delta.typeChanges", Array(
            typeChangeMetadata("integer", "long"),
            typeChangeMetadata("float", "double", "value")
          ))
        .build()
      )

    // Update matching one of the type changes.
    assert(TypeWideningMetadata.updateTypeChangeVersion(schema, 1, 4) ===
      new StructType()
      .add("a", IntegerType, nullable = true, metadata = new MetadataBuilder()
          .putMetadataArray("delta.typeChanges", Array(
            typeChangeMetadata("integer", "long"),
            typeChangeMetadata("float", "double", "value")
          ))
        .build()
      )
    )

    // Update doesn't match any of the recorded type changes.
    assert(
      TypeWideningMetadata.updateTypeChangeVersion(schema, 3, 4) === schema
    )
  }

  test("updateTypeChangeVersion with multiple fields with a type change") {
    val schema = new StructType()
      .add("a", IntegerType, nullable = true, metadata = new MetadataBuilder()
          .putMetadataArray("delta.typeChanges", Array(
            typeChangeMetadata("integer", "long")
          ))
        .build())
      .add("b", ArrayType(IntegerType), nullable = true, metadata = new MetadataBuilder()
          .putMetadataArray("delta.typeChanges", Array(
            typeChangeMetadata("short", "integer", "element")
          ))
        .build())

    // Update both type changes.
    assert(TypeWideningMetadata.updateTypeChangeVersion(schema, 1, 4) ===
      new StructType()
      .add("a", IntegerType, nullable = true, metadata = new MetadataBuilder()
          .putMetadataArray("delta.typeChanges", Array(
            typeChangeMetadata("integer", "long")
          ))
        .build())
      .add("b", ArrayType(IntegerType), nullable = true, metadata = new MetadataBuilder()
          .putMetadataArray("delta.typeChanges", Array(
            typeChangeMetadata("short", "integer", "element")
          ))
        .build())
    )

    // Update doesn't match any of the recorded type changes.
    assert(
      TypeWideningMetadata.updateTypeChangeVersion(schema, 3, 4) === schema
    )
  }
}

/**
 * Tests that covers recording type change information as metadata in the table schema. For
 * lower-level tests, see [[TypeWideningMetadataTests]].
 */
trait TypeWideningMetadataEndToEndTests {
  self: QueryTest with TypeWideningTestMixin =>

  def testTypeWideningMetadata(name: String)(
      initialSchema: String,
      typeChanges: Seq[(String, String)],
      expectedJsonSchema: String): Unit =
    test(name) {
      sql(s"CREATE TABLE delta.`$tempPath` ($initialSchema) USING DELTA")
      typeChanges.foreach { case (fieldName, newType) =>
        sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN $fieldName TYPE $newType")
      }

      // Parse the schemas as JSON to ignore whitespaces and field order.
      val actualSchema = JsonUtils.fromJson[Map[String, Any]](readDeltaTable(tempPath).schema.json)
      val expectedSchema = JsonUtils.fromJson[Map[String, Any]](expectedJsonSchema)
      assert(actualSchema === expectedSchema,
        s"${readDeltaTable(tempPath).schema.prettyJson} did not equal $expectedJsonSchema"
      )
    }

  testTypeWideningMetadata("change top-level column type short->int")(
    initialSchema = "a short",
    typeChanges = Seq("a" -> "int"),
    expectedJsonSchema =
      """{
      "type": "struct",
      "fields": [{
        "name": "a",
        "type": "integer",
        "nullable": true,
        "metadata": {
          "delta.typeChanges": [{
            "toType": "integer",
            "fromType": "short",
            "tableVersion": 1
          }]
        }
      }]}""".stripMargin)

  testTypeWideningMetadata("change top-level column type twice byte->short->int")(
    initialSchema = "a byte",
    typeChanges = Seq("a" -> "short", "a" -> "int"),
    expectedJsonSchema =
      """{
      "type": "struct",
      "fields": [{
        "name": "a",
        "type": "integer",
        "nullable": true,
        "metadata": {
          "delta.typeChanges": [{
            "toType": "short",
            "fromType": "byte",
            "tableVersion": 1
          },{
            "toType": "integer",
            "fromType": "short",
            "tableVersion": 2
          }]
        }
      }]}""".stripMargin)

  testTypeWideningMetadata("change type in map key and in struct in map value")(
    initialSchema = "a map<byte, struct<b: byte>>",
    typeChanges = Seq("a.key" -> "int", "a.value.b" -> "short"),
    expectedJsonSchema =
      """{
      "type": "struct",
      "fields": [{
        "name": "a",
        "type": {
          "type": "map",
          "keyType": "integer",
          "valueType": {
            "type": "struct",
            "fields": [{
              "name": "b",
              "type": "short",
              "nullable": true,
              "metadata": {
                "delta.typeChanges": [{
                  "toType": "short",
                  "fromType": "byte",
                  "tableVersion": 2
                }]
              }
            }]
          },
          "valueContainsNull": true
        },
        "nullable": true,
        "metadata": {
          "delta.typeChanges": [{
            "toType": "integer",
            "fromType": "byte",
            "tableVersion": 1,
            "fieldPath": "key"
          }]
        }
      }
    ]}""".stripMargin)


  testTypeWideningMetadata("change type in array and in struct in array")(
    initialSchema = "a array<byte>, b array<struct<c: short>>",
    typeChanges = Seq("a.element" -> "short", "b.element.c" -> "int"),
    expectedJsonSchema =
      """{
      "type": "struct",
      "fields": [{
        "name": "a",
        "type": {
          "type": "array",
          "elementType": "short",
          "containsNull": true
        },
        "nullable": true,
        "metadata": {
          "delta.typeChanges": [{
            "toType": "short",
            "fromType": "byte",
            "tableVersion": 1,
            "fieldPath": "element"
          }]
        }
      },
      {
        "name": "b",
        "type": {
          "type": "array",
          "elementType":{
            "type": "struct",
            "fields": [{
              "name": "c",
              "type": "integer",
              "nullable": true,
              "metadata": {
                "delta.typeChanges": [{
                  "toType": "integer",
                  "fromType": "short",
                  "tableVersion": 2
                }]
              }
            }]
          },
          "containsNull": true
        },
        "nullable": true,
        "metadata": { }
      }
    ]}""".stripMargin)
}
