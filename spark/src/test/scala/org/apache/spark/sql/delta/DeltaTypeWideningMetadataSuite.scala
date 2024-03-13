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

package org.apache.spark.sql.delta

import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.types._

/**
 * Suite covering the[[TypeWideningMetadata]] and [[TypeChange]] classes used to handle the metadata
 * recorded by the Type Widening table feature in the table schema.
 */
class DeltaTypeWideningMetadataSuite extends QueryTest with DeltaSQLCommandTest {
  private val testTableName: String = "delta_type_widening_metadata_test"

  /** A dummy transaction to be used by tests covering `addTypeWideningMetadata`. */
  private lazy val txn: OptimisticTransaction = {
    val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier(testTableName))
    DeltaLog.forTable(spark, TableIdentifier(testTableName))
        .startTransaction(catalogTableOpt = Some(table))
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    sql(s"CREATE TABLE $testTableName (a int) USING delta " +
      s"TBLPROPERTIES ('${DeltaConfigs.ENABLE_TYPE_WIDENING.key}' = 'true')")
  }

  override protected def afterAll(): Unit = {
    sql(s"DROP TABLE IF EXISTS $testTableName")
    super.afterAll()
  }

  /**
   * Short-hand to build the metadata for a type change to cut down on repetition.
   */
  private def typeChangeMetadata(
      version: Long,
      fromType: String,
      toType: String,
      path: String = ""): Metadata = {
    val builder = new MetadataBuilder()
      .putLong("tableVersion", version)
      .putString("fromType", fromType)
      .putString("toType", toType)
    if (path.nonEmpty) {
      builder.putString("fieldPath", path)
    }
    builder.build()
  }

  test("toMetadata/fromMetadata with empty path") {
    val typeChange = TypeChange(version = 1, IntegerType, LongType, Seq.empty)
    assert(typeChange.toMetadata === typeChangeMetadata(version = 1, "integer", "long"))
    assert(TypeChange.fromMetadata(typeChange.toMetadata) === typeChange)
  }

  test("toMetadata/fromMetadata with non-empty path") {
    val typeChange = TypeChange(10, DateType, TimestampNTZType, Seq("key", "element"))
    assert(typeChange.toMetadata ===
      typeChangeMetadata(version = 10, "date", "timestamp_ntz", "key.element"))
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
        typeChangeMetadata(version = 1, "integer", "long")
      )).build()
    )
    assert(TypeWideningMetadata.fromField(field) ===
      Some(TypeWideningMetadata(Seq(TypeChange(1, IntegerType, LongType, Seq.empty)))))
    val otherField = StructField("a", IntegerType)
    assert(TypeWideningMetadata.fromField(field).get.appendToField(otherField) === field)
  }

  test("fromField with multiple type changes") {
    val field = StructField("a", IntegerType, metadata = new MetadataBuilder()
      .putMetadataArray("delta.typeChanges", Array(
        typeChangeMetadata(version = 1, "integer", "long"),
        typeChangeMetadata(version = 10, "decimal(5,0)", "decimal(10,2)", "element.element")
      )).build()
    )
    assert(TypeWideningMetadata.fromField(field) ===
      Some(TypeWideningMetadata(Seq(
        TypeChange(1, IntegerType, LongType, Seq.empty),
        TypeChange(10, DecimalType(5, 0), DecimalType(10, 2), Seq("element", "element"))))))
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
      TypeChange(1, IntegerType, LongType, Seq.empty)))
    assert(singleMetadata.appendToField(field) === field.copy(metadata =
      new MetadataBuilder()
        .putMetadataArray("delta.typeChanges", Array(
          typeChangeMetadata(version = 1, "integer", "long")
        )).build()
      )
    )
    val singleMetadataFromField =
      TypeWideningMetadata.fromField(singleMetadata.appendToField(field))
    assert(singleMetadataFromField.contains(singleMetadata))

    // Adding multiple type changes should add the metadata to the field and not otherwise change
    // it.
    val multipleMetadata = TypeWideningMetadata(Seq(
      TypeChange(1, IntegerType, LongType, Seq.empty),
      TypeChange(6, FloatType, DoubleType, Seq("value"))))
    assert(multipleMetadata.appendToField(field) === field.copy(metadata =
      new MetadataBuilder()
        .putMetadataArray("delta.typeChanges", Array(
          typeChangeMetadata(version = 1, "integer", "long"),
          typeChangeMetadata(version = 6, "float", "double", "value")
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
          typeChangeMetadata(version = 1, "integer", "long")
        )).build()
    )
    // Adding empty type widening metadata should not change the field.
    val emptyMetadata = TypeWideningMetadata(Seq.empty)
    assert(emptyMetadata.appendToField(field) === field)
    assert(TypeWideningMetadata.fromField(emptyMetadata.appendToField(field)).contains(
      TypeWideningMetadata(Seq(
        TypeChange(1, IntegerType, LongType, Seq.empty)))
    ))

    // Adding single type change should add the metadata to the field and not otherwise change it.
    val singleMetadata = TypeWideningMetadata(Seq(
      TypeChange(5, DecimalType(18, 0), DecimalType(19, 0), Seq.empty)))

    assert(singleMetadata.appendToField(field) === field.copy(
      metadata = new MetadataBuilder()
        .putMetadataArray("delta.typeChanges", Array(
          typeChangeMetadata(version = 1, "integer", "long"),
          typeChangeMetadata(version = 5, "decimal(18,0)", "decimal(19,0)")
        )).build()
    ))
    val singleMetadataFromField =
      TypeWideningMetadata.fromField(singleMetadata.appendToField(field))

    assert(singleMetadataFromField.contains(TypeWideningMetadata(Seq(
      TypeChange(1, IntegerType, LongType, Seq.empty),
      TypeChange(5, DecimalType(18, 0), DecimalType(19, 0), Seq.empty)))
    ))

    // Adding multiple type changes should add the metadata to the field and not otherwise change
    // it.
    val multipleMetadata = TypeWideningMetadata(Seq(
      TypeChange(5, DecimalType(18, 0), DecimalType(19, 0), Seq.empty),
      TypeChange(6, FloatType, DoubleType, Seq("value"))))

    assert(multipleMetadata.appendToField(field) === field.copy(
      metadata = new MetadataBuilder()
        .putMetadataArray("delta.typeChanges", Array(
          typeChangeMetadata(version = 1, "integer", "long"),
          typeChangeMetadata(version = 5, "decimal(18,0)", "decimal(19,0)"),
          typeChangeMetadata(version = 6, "float", "double", "value")
        )).build()
    ))
    val multipleMetadataFromField =
      TypeWideningMetadata.fromField(multipleMetadata.appendToField(field))

    assert(multipleMetadataFromField.contains(TypeWideningMetadata(Seq(
      TypeChange(1, IntegerType, LongType, Seq.empty),
      TypeChange(5, DecimalType(18, 0), DecimalType(19, 0), Seq.empty),
      TypeChange(6, FloatType, DoubleType, Seq("value"))))
    ))
  }

  test("addTypeWideningMetadata with no type changes") {
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
      }
    }
  }

  test("addTypeWideningMetadata on top-level fields") {
    var schema =
      StructType.fromDDL("i long, d decimal(15, 4), a array<double>, m map<short, int>")
    val firstOldSchema =
      StructType.fromDDL("i short, d decimal(6, 2), a array<byte>, m map<byte, int>")
    val secondOldSchema =
      StructType.fromDDL("i int, d decimal(10, 4), a array<int>, m map<short, byte>")

    schema = TypeWideningMetadata.addTypeWideningMetadata(txn, schema, firstOldSchema)

    assert(schema("i") === StructField("i", LongType,
      metadata = new MetadataBuilder()
        .putMetadataArray("delta.typeChanges", Array(
          typeChangeMetadata(version = 1, "short", "long")
        )).build()
    ))

    assert(schema("d") === StructField("d", DecimalType(15, 4),
      metadata = new MetadataBuilder()
        .putMetadataArray("delta.typeChanges", Array(
          typeChangeMetadata(version = 1, "decimal(6,2)", "decimal(15,4)")
        )).build()
    ))

    assert(schema("a") === StructField("a", ArrayType(DoubleType),
      metadata = new MetadataBuilder()
        .putMetadataArray("delta.typeChanges", Array(
          typeChangeMetadata(version = 1, "byte", "double", "element")
        )).build()
    ))

    assert(schema("m") === StructField("m", MapType(ShortType, IntegerType),
      metadata = new MetadataBuilder()
        .putMetadataArray("delta.typeChanges", Array(
          typeChangeMetadata(version = 1, "byte", "short", "key")
        )).build()
    ))

    // Second type change on all fields.
    schema = TypeWideningMetadata.addTypeWideningMetadata(txn, schema, secondOldSchema)

    assert(schema("i") === StructField("i", LongType,
      metadata = new MetadataBuilder()
        .putMetadataArray("delta.typeChanges", Array(
          typeChangeMetadata(version = 1, "short", "long"),
          typeChangeMetadata(version = 1, "integer", "long")
        )).build()
    ))

    assert(schema("d") === StructField("d", DecimalType(15, 4),
      metadata = new MetadataBuilder()
        .putMetadataArray("delta.typeChanges", Array(
          typeChangeMetadata(version = 1, "decimal(6,2)", "decimal(15,4)"),
          typeChangeMetadata(version = 1, "decimal(10,4)", "decimal(15,4)")
        )).build()
    ))

    assert(schema("a") === StructField("a", ArrayType(DoubleType),
      metadata = new MetadataBuilder()
        .putMetadataArray("delta.typeChanges", Array(
          typeChangeMetadata(version = 1, "byte", "double", "element"),
          typeChangeMetadata(version = 1, "integer", "double", "element")
        )).build()
    ))

    assert(schema("m") === StructField("m", MapType(ShortType, IntegerType),
      metadata = new MetadataBuilder()
        .putMetadataArray("delta.typeChanges", Array(
          typeChangeMetadata(version = 1, "byte", "short", "key"),
          typeChangeMetadata(version = 1, "byte", "integer", "value")
        )).build()
    ))
  }

  test("addTypeWideningMetadata on nested fields") {
    var schema = StructType.fromDDL(
      "s struct<i: long, a: array<map<int, long>>, m: map<map<long, int>, array<long>>>")
    val firstOldSchema = StructType.fromDDL(
      "s struct<i: short, a: array<map<byte, long>>, m: map<map<int, int>, array<long>>>")
    val secondOldSchema = StructType.fromDDL(
      "s struct<i: int, a: array<map<int, int>>, m: map<map<long, int>, array<int>>>")

    // First type change on all struct fields.
    schema = TypeWideningMetadata.addTypeWideningMetadata(txn, schema, firstOldSchema)
    var struct = schema("s").dataType.asInstanceOf[StructType]

    assert(struct("i") === StructField("i", LongType,
      metadata = new MetadataBuilder()
        .putMetadataArray("delta.typeChanges", Array(
          typeChangeMetadata(version = 1, "short", "long")
        )).build()
    ))

    assert(struct("a") === StructField("a", ArrayType(MapType(IntegerType, LongType)),
      metadata = new MetadataBuilder()
        .putMetadataArray("delta.typeChanges", Array(
          typeChangeMetadata(version = 1, "byte", "integer", "element.key")
        )).build()
    ))

    assert(struct("m") === StructField("m",
      MapType(MapType(LongType, IntegerType), ArrayType(LongType)),
      metadata = new MetadataBuilder()
        .putMetadataArray("delta.typeChanges", Array(
          typeChangeMetadata(version = 1, "integer", "long", "key.key")
        )).build()
    ))

    // Second type change on all struct fields.
    schema = TypeWideningMetadata.addTypeWideningMetadata(txn, schema, secondOldSchema)
    struct = schema("s").dataType.asInstanceOf[StructType]

    assert(struct("i") === StructField("i", LongType,
      metadata = new MetadataBuilder()
        .putMetadataArray("delta.typeChanges", Array(
          typeChangeMetadata(version = 1, "short", "long"),
          typeChangeMetadata(version = 1, "integer", "long")
        )).build()
    ))

    assert(struct("a") === StructField("a", ArrayType(MapType(IntegerType, LongType)),
      metadata = new MetadataBuilder()
        .putMetadataArray("delta.typeChanges", Array(
          typeChangeMetadata(version = 1, "byte", "integer", "element.key"),
          typeChangeMetadata(version = 1, "integer", "long", "element.value")
        )).build()
    ))

    assert(struct("m") === StructField("m",
      MapType(MapType(LongType, IntegerType), ArrayType(LongType)),
      metadata = new MetadataBuilder()
        .putMetadataArray("delta.typeChanges", Array(
          typeChangeMetadata(version = 1, "integer", "long", "key.key"),
          typeChangeMetadata(version = 1, "integer", "long", "value.element")
        )).build()
    ))
  }

  test("addTypeWideningMetadata with added and removed fields") {
    val newSchema = StructType.fromDDL("a int, b long, d int")
    val oldSchema = StructType.fromDDL("a int, b int, c int")

    val schema = TypeWideningMetadata.addTypeWideningMetadata(txn, newSchema, oldSchema)
    assert(schema("a") === StructField("a", IntegerType))
    assert(schema("d") === StructField("d", IntegerType))
    assert(!schema.contains("c"))

    assert(schema("b") === StructField("b", LongType,
      metadata = new MetadataBuilder()
        .putMetadataArray("delta.typeChanges", Array(
          typeChangeMetadata(version = 1, "integer", "long")
        )).build()
    ))
  }

  test("addTypeWideningMetadata with different field position") {
    val initialSchema = StructType.fromDDL("a short, b int, s struct<c: int, d: long>")
    val secondSchema = StructType.fromDDL("b int, a short, s struct<d: long, c: int>")

    val schema = TypeWideningMetadata.addTypeWideningMetadata(txn, initialSchema, secondSchema)
    // No type widening metadata is added.
    assert(schema("a") === StructField("a", ShortType))
    assert(schema("b") === StructField("b", IntegerType))
    assert(schema("s") ===
      StructField("s", new StructType()
        .add("c", IntegerType)
        .add("d", LongType)))
  }

  test("updateTypeChangeVersion with no type changes") {
    val schema = new StructType().add("a", IntegerType)
    assert(TypeWideningMetadata.updateTypeChangeVersion(schema, 1, 4) === schema)
  }

  test("updateTypeChangeVersion with field with single type change") {
    val schema = new StructType()
      .add("a", IntegerType, nullable = true, metadata = new MetadataBuilder()
        .putMetadataArray("delta.typeChanges", Array(
          typeChangeMetadata(version = 1, "integer", "long")
        ))
        .build()
      )

    assert(TypeWideningMetadata.updateTypeChangeVersion(schema, 1, 4) ===
      new StructType()
        .add("a", IntegerType, nullable = true, metadata = new MetadataBuilder()
          .putMetadataArray("delta.typeChanges", Array(
            typeChangeMetadata(version = 4, "integer", "long")
          ))
          .build()
        )
    )
  }

  test("updateTypeChangeVersion with field with multiple type changes") {
    val schema = new StructType()
      .add("a", IntegerType, nullable = true, metadata = new MetadataBuilder()
          .putMetadataArray("delta.typeChanges", Array(
            typeChangeMetadata(version = 1, "integer", "long"),
            typeChangeMetadata(version = 6, "float", "double", "value")
          ))
        .build()
      )

    // Update matching one of the type changes.
    assert(TypeWideningMetadata.updateTypeChangeVersion(schema, 1, 4) ===
      new StructType()
      .add("a", IntegerType, nullable = true, metadata = new MetadataBuilder()
          .putMetadataArray("delta.typeChanges", Array(
            typeChangeMetadata(version = 4, "integer", "long"),
            typeChangeMetadata(version = 6, "float", "double", "value")
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
            typeChangeMetadata(version = 1, "integer", "long")
          ))
        .build())
      .add("b", ArrayType(IntegerType), nullable = true, metadata = new MetadataBuilder()
          .putMetadataArray("delta.typeChanges", Array(
            typeChangeMetadata(version = 1, "short", "integer", "element")
          ))
        .build())

    // Update both type changes.
    assert(TypeWideningMetadata.updateTypeChangeVersion(schema, 1, 4) ===
      new StructType()
      .add("a", IntegerType, nullable = true, metadata = new MetadataBuilder()
          .putMetadataArray("delta.typeChanges", Array(
            typeChangeMetadata(version = 4, "integer", "long")
          ))
        .build())
      .add("b", ArrayType(IntegerType), nullable = true, metadata = new MetadataBuilder()
          .putMetadataArray("delta.typeChanges", Array(
            typeChangeMetadata(version = 4, "short", "integer", "element")
          ))
        .build())
    )

    // Update doesn't match any of the recorded type changes.
    assert(
      TypeWideningMetadata.updateTypeChangeVersion(schema, 3, 4) === schema
    )
  }
}
