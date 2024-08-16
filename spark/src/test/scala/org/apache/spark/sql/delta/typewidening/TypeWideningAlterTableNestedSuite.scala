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

import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.execution.datasources.parquet.ParquetTest
import org.apache.spark.sql.types._

/**
 * Suite providing additional coverage for widening nested fields using ALTER TABLE CHANGE COLUMN
 * TYPE.
 */
class TypeWideningAlterTableNestedSuite
  extends QueryTest
    with ParquetTest
    with TypeWideningTestMixin
    with TypeWideningAlterTableNestedTests

trait TypeWideningAlterTableNestedTests {
  self: QueryTest with ParquetTest with TypeWideningTestMixin =>

  import testImplicits._

  /** Create a table with a struct, map and array for each test. */
  protected def createNestedTable(): Unit = {
    sql(s"CREATE TABLE delta.`$tempPath` " +
      "(s struct<a: byte>, m map<byte, short>, a array<short>) USING DELTA")
    append(Seq((1, 2, 3, 4))
      .toDF("a", "b", "c", "d")
      .selectExpr(
        "named_struct('a', cast(a as byte)) as s",
        "map(cast(b as byte), cast(c as short)) as m",
        "array(cast(d as short)) as a"))

    assert(readDeltaTable(tempPath).schema === new StructType()
      .add("s", new StructType().add("a", ByteType))
      .add("m", MapType(ByteType, ShortType))
      .add("a", ArrayType(ShortType)))
  }

  test("unsupported ALTER TABLE CHANGE COLUMN on non-leaf fields") {
    createNestedTable()
    // Running ALTER TABLE CHANGE COLUMN on non-leaf fields is invalid.
    var alterTableSql = s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN s TYPE struct<a: short>"
    checkError(
      exception = intercept[AnalysisException] { sql(alterTableSql) },
      errorClass = "CANNOT_UPDATE_FIELD.STRUCT_TYPE",
      parameters = Map(
        "table" -> s"`spark_catalog`.`delta`.`$tempPath`",
        "fieldName" -> "`s`"
      ),
      context = ExpectedContext(
        fragment = alterTableSql,
        start = 0,
        stop = alterTableSql.length - 1)
    )

    alterTableSql = s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN m TYPE map<int, int>"
    checkError(
      exception = intercept[AnalysisException] { sql(alterTableSql) },
      errorClass = "CANNOT_UPDATE_FIELD.MAP_TYPE",
      parameters = Map(
        "table" -> s"`spark_catalog`.`delta`.`$tempPath`",
        "fieldName" -> "`m`"
      ),
      context = ExpectedContext(
        fragment = alterTableSql,
        start = 0,
        stop = alterTableSql.length - 1)
    )

    alterTableSql = s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN a TYPE array<int>"
    checkError(
      exception = intercept[AnalysisException] { sql(alterTableSql) },
      errorClass = "CANNOT_UPDATE_FIELD.ARRAY_TYPE",
      parameters = Map(
        "table" -> s"`spark_catalog`.`delta`.`$tempPath`",
        "fieldName" -> "`a`"
      ),
      context = ExpectedContext(
        fragment = alterTableSql,
        start = 0,
        stop = alterTableSql.length - 1)
    )
  }

  test("type widening with ALTER TABLE on nested fields") {
    createNestedTable()
    sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN s.a TYPE short")
    sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN m.key TYPE int")
    sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN m.value TYPE int")
    sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN a.element TYPE int")

    assert(readDeltaTable(tempPath).schema === new StructType()
      .add("s", new StructType()
        .add("a", ShortType, nullable = true, metadata = new MetadataBuilder()
          .putMetadataArray("delta.typeChanges", Array(
            new MetadataBuilder()
              .putString("toType", "short")
              .putString("fromType", "byte")
              .putLong("tableVersion", 2)
              .build()
          )).build()))
      .add("m", MapType(IntegerType, IntegerType), nullable = true, metadata = new MetadataBuilder()
          .putMetadataArray("delta.typeChanges", Array(
            new MetadataBuilder()
              .putString("toType", "integer")
              .putString("fromType", "byte")
              .putLong("tableVersion", 3)
              .putString("fieldPath", "key")
              .build(),
            new MetadataBuilder()
              .putString("toType", "integer")
              .putString("fromType", "short")
              .putLong("tableVersion", 4)
              .putString("fieldPath", "value")
              .build()
          )).build())
      .add("a", ArrayType(IntegerType), nullable = true, metadata = new MetadataBuilder()
          .putMetadataArray("delta.typeChanges", Array(
            new MetadataBuilder()
              .putString("toType", "integer")
              .putString("fromType", "short")
              .putLong("tableVersion", 5)
              .putString("fieldPath", "element")
              .build()
          )).build()))

    append(Seq((5, 6, 7, 8))
      .toDF("a", "b", "c", "d")
        .selectExpr("named_struct('a', cast(a as short)) as s", "map(b, c) as m", "array(d) as a"))

    checkAnswer(
      readDeltaTable(tempPath),
      Seq((1, 2, 3, 4), (5, 6, 7, 8))
        .toDF("a", "b", "c", "d")
        .selectExpr("named_struct('a', cast(a as short)) as s", "map(b, c) as m", "array(d) as a"))
  }

  test("type widening using ALTER TABLE REPLACE COLUMNS on nested fields") {
    createNestedTable()
    sql(s"ALTER TABLE delta.`$tempPath` REPLACE COLUMNS " +
      "(s struct<a: short>, m map<int, int>, a array<int>)")
    assert(readDeltaTable(tempPath).schema === new StructType()
      .add("s", new StructType()
        .add("a", ShortType, nullable = true, metadata = new MetadataBuilder()
          .putMetadataArray("delta.typeChanges", Array(
            new MetadataBuilder()
              .putString("toType", "short")
              .putString("fromType", "byte")
              .putLong("tableVersion", 2)
              .build()
          )).build()))
      .add("m", MapType(IntegerType, IntegerType), nullable = true, metadata = new MetadataBuilder()
          .putMetadataArray("delta.typeChanges", Array(
            new MetadataBuilder()
              .putString("toType", "integer")
              .putString("fromType", "byte")
              .putLong("tableVersion", 2)
              .putString("fieldPath", "key")
              .build(),
            new MetadataBuilder()
              .putString("toType", "integer")
              .putString("fromType", "short")
              .putLong("tableVersion", 2)
              .putString("fieldPath", "value")
              .build()
          )).build())
      .add("a", ArrayType(IntegerType), nullable = true, metadata = new MetadataBuilder()
          .putMetadataArray("delta.typeChanges", Array(
            new MetadataBuilder()
              .putString("toType", "integer")
              .putString("fromType", "short")
              .putLong("tableVersion", 2)
              .putString("fieldPath", "element")
              .build()
          )).build()))

    append(Seq((5, 6, 7, 8))
      .toDF("a", "b", "c", "d")
        .selectExpr("named_struct('a', cast(a as short)) as s", "map(b, c) as m", "array(d) as a"))

    checkAnswer(
      readDeltaTable(tempPath),
      Seq((1, 2, 3, 4), (5, 6, 7, 8))
        .toDF("a", "b", "c", "d")
        .selectExpr("named_struct('a', cast(a as short)) as s", "map(b, c) as m", "array(d) as a"))
  }
}
