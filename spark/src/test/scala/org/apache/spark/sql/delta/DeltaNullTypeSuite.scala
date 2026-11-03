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

import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.{DeltaSQLCommandTest, DeltaSQLTestUtils}
import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.execution.datasources.parquet.ParquetTest
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * Test suite for various NullType (VOID) column tests in Delta.
 */
class DeltaNullTypeSuite
  extends QueryTest
  with DeltaTestUtilsBase
  with DeltaSQLTestUtils
  with DeltaSQLCommandTest
  with ParquetTest {
  import testImplicits._

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)
      (implicit pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      assume(DeltaTestUtilsBase.nullTypeColumnsSupported)
      testFun
    }
  }

  test("Create and read table with NullType column") {
    val table_name = "test_table"
    withTable(table_name) {
      withSQLConf(DeltaSQLConf.DELTA_CREATE_DATAFRAME_DROP_NULL_COLUMNS.key -> "false") {
        Seq((1, null)).toDF("id", "value").write.format("delta").saveAsTable(table_name)

        withAllParquetReaders {
          val df = spark.read.format("delta").table(table_name)
          checkAnswer(df, Row(1, null) :: Nil)
          checkAnswer(df.where("value is NULL"), Row(1, null) :: Nil)
          checkAnswer(df.where("value is not NULL"), Nil)
          checkAnswer(df.select("value"), Row(null) :: Nil)
        }
      }
    }
  }

  test("Create and read table with NullType column in a struct") {
    val table_name = "test_table"
    withTable(table_name) {
      withSQLConf(DeltaSQLConf.DELTA_CREATE_DATAFRAME_DROP_NULL_COLUMNS.key -> "false") {
        Seq((1, (1, null))).toDF("id", "value").write.format("delta").saveAsTable(table_name)

        withAllParquetReaders {
          val df = spark.read.format("delta").table(table_name)
          checkAnswer(df, Row(1, Row(1, null)) :: Nil)
          checkAnswer(df.where("value._2 is NULL"), Row(1, Row(1, null)) :: Nil)
          checkAnswer(df.where("value._2 is not NULL"), Nil)
          checkAnswer(df.select("value._2"), Row(null) :: Nil)
        }
      }
    }
  }

  test("null struct with NullType field kept as null") {
    withTempTable(createTable = false) { tableName =>
      Seq(((null, 2), 1), (null, 2)).toDF("key", "value")
        .write.format("delta").saveAsTable(tableName)

      // Confirm struct value stays as null (fields are not set to null).
      val rowWithNullStruct = spark.read.format("delta").table(tableName).filter($"value" === 2)
      checkAnswer(rowWithNullStruct, Row(null, 2) :: Nil)
    }
  }

  test("null struct with NullType field, with backticks in the column name, kept as null") {
    withTempTable(createTable = false) { tableName =>
      Seq(((null, 2), 1), (null, 2)).toDF("key`", "val`ue")
        .write.format("delta").saveAsTable(tableName)

      // Confirm struct value stays as null (fields are not set to null).
      val rowWithNullStruct = spark.read.format("delta").table(tableName).filter($"`val``ue`" === 2)
      checkAnswer(rowWithNullStruct, Row(null, 2) :: Nil)
    }
  }


  test("Cannot create table with NullType UDT column") {
    val table_name = "test_table_with_udt_fails"
    withTable(table_name) {
      checkError(
        intercept[DeltaAnalysisException] {
          Seq((1, new NullData())).toDF("id", "value")
            .write.format("delta").saveAsTable(table_name)
        },
        "DELTA_USER_DEFINED_TYPE_COLUMN_CONTAINS_NULL_TYPE",
        sqlState = Some("22005"),
        parameters = Map("columnName" -> "value", "userClass" -> classOf[NullData].getName)
      )
    }
  }

  test("Cannot create table with NullType in a complex UDT column") {
    val table_name = "test_table_with_complex_udt"
    withTable(table_name) {
      checkError(
        intercept[DeltaAnalysisException] {
          Seq((1, new ComplexData())).toDF("id", "value")
            .write.format("delta").saveAsTable(table_name)
        },
        "DELTA_USER_DEFINED_TYPE_COLUMN_CONTAINS_NULL_TYPE",
        sqlState = Some("22005"),
        parameters = Map("columnName" -> "value", "userClass" -> classOf[ComplexData].getName)
      )
    }
  }

  for (schema <- Seq(
    new StructType().add("a", ArrayType(NullType)),
    new StructType().add("a", MapType(StringType, NullType)),
    new StructType().add("a", MapType(
      new StructType().add("x", IntegerType).add("v", NullType),
      StringType))
  )) {
    test(s"Empty write into VOID in array/map should fail: ${schema.sql}") {
      val tableName = "empty_test_table"
      withTable(tableName) {
        // Even empty insert should fail in these cases, since we check the schema during analysis.
        val df = spark.createDataFrame(
          spark.sparkContext.emptyRDD[org.apache.spark.sql.Row],
          schema
        )

        checkError(
          intercept[DeltaAnalysisException] {
            df.write.format("delta").saveAsTable(tableName)
          },
          condition = "DELTA_COMPLEX_TYPE_COLUMN_CONTAINS_NULL_TYPE",
          parameters = Map(
            "columName" -> "a",
            "dataType" -> schema.fields.head.dataType.getClass.getSimpleName
          )
        )
      }
    }
  }

  for ((schema, expectedErrorSubClass, columnPath) <- Seq(
    (new StructType().add("a", NullType), "TABLE_ALL_VOID_COLUMNS", None),
    (new StructType().add("a", new StructType()), "STRUCT_NO_FIELDS", Some("a")),
    (new StructType().add("a", ArrayType(new StructType())), "STRUCT_NO_FIELDS", Some("a.element")),
    (new StructType().add("a", MapType(StringType, new StructType())),
      "STRUCT_NO_FIELDS", Some("a.value")),
    (new StructType().add("a", new StructType().add("v", NullType)),
      "STRUCT_ALL_VOID_FIELDS", Some("a"))
  )) {
    test(s"Non-empty write into all VOID or empty schema/struct should fail: ${schema.sql}") {
      val tableName = "test_table"
      withTable(tableName) {
        val sqlTableSchema = schema.toDDL
        sql(s"CREATE TABLE $tableName ($sqlTableSchema) USING delta")

        val data = if (expectedErrorSubClass == "TABLE_NO_COLUMNS") Seq(Row()) else Seq(Row(null))
        val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

        withAllParquetWriters {
          checkError(
            intercept[DeltaAnalysisException] {
              df.write.format("delta").mode("append").saveAsTable(tableName)
            },
            condition = s"DELTA_CANNOT_WRITE_EMPTY_SCHEMA.$expectedErrorSubClass",
            parameters = columnPath.map(p => Map("columnPath" -> p)).getOrElse(Map.empty)
          )
        }
      }
    }
  }
}

@SQLUserDefinedType(udt = classOf[NullUDT])
class NullData extends Serializable

class NullUDT extends UserDefinedType[NullData] {
  override def sqlType: DataType = NullType
  override def userClass: Class[NullData] = classOf[NullData]
  override def serialize(obj: NullData): Any = null
  override def deserialize(datum: Any): NullData = new NullData()
}

@SQLUserDefinedType(udt = classOf[ComplexUDT])
class ComplexData extends Serializable

class ComplexUDT extends UserDefinedType[ComplexData] {
  override def sqlType: DataType = new MapType(
    StringType,
    new ArrayType(
      new StructType().add("a", IntegerType).add("b", new NullUDT), containsNull = true),
    valueContainsNull = true)
  override def userClass: Class[ComplexData] = classOf[ComplexData]
  override def serialize(obj: ComplexData): Any = null
  override def deserialize(datum: Any): ComplexData = new ComplexData()
}
