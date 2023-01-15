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

import scala.collection.JavaConverters._

import org.scalatest.GivenWhenThen

import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, IntegerType, MapType, StringType, StructType}

trait DeltaArbitraryColumnNameSuiteBase extends DeltaColumnMappingSuiteUtils {

  protected val simpleNestedSchema = new StructType()
    .add("a", StringType, true)
    .add("b",
      new StructType()
        .add("c", StringType, true)
        .add("d", IntegerType, true))
    .add("map", MapType(StringType, StringType), true)
    .add("arr", ArrayType(IntegerType), true)

  protected val simpleNestedSchemaWithDuplicatedNestedColumnName = new StructType()
    .add("a",
      new StructType()
        .add("c", StringType, true)
        .add("d", IntegerType, true), true)
    .add("b",
      new StructType()
        .add("c", StringType, true)
        .add("d", IntegerType, true), true)
    .add("map", MapType(StringType, StringType), true)
    .add("arr", ArrayType(IntegerType), true)

  protected val nestedSchema = new StructType()
    .add(colName("a"), StringType, true)
    .add(colName("b"),
      new StructType()
        .add(colName("c"), StringType, true)
        .add(colName("d"), IntegerType, true))
    .add(colName("map"), MapType(StringType, StringType), true)
    .add(colName("arr"), ArrayType(IntegerType), true)

  protected def simpleNestedData =
    spark.createDataFrame(
      Seq(
        Row("str1", Row("str1.1", 1), Map("k1" -> "v1"), Array(1, 11)),
        Row("str2", Row("str1.2", 2), Map("k2" -> "v2"), Array(2, 22))).asJava,
      simpleNestedSchema)

  protected def simpleNestedDataWithDuplicatedNestedColumnName =
    spark.createDataFrame(
      Seq(
        Row(Row("str1", 1), Row("str1.1", 1), Map("k1" -> "v1"), Array(1, 11)),
        Row(Row("str2", 2), Row("str1.2", 2), Map("k2" -> "v2"), Array(2, 22))).asJava,
      simpleNestedSchemaWithDuplicatedNestedColumnName)

  protected def nestedData =
    spark.createDataFrame(
      Seq(
        Row("str1", Row("str1.1", 1), Map("k1" -> "v1"), Array(1, 11)),
        Row("str2", Row("str1.2", 2), Map("k2" -> "v2"), Array(2, 22))).asJava,
      nestedSchema)

  // TODO: Refactor DeltaColumnMappingSuite and consolidate these table creation methods between
  // the two suites.
  protected def createTableWithSQLCreateOrReplaceAPI(
      tableName: String,
      data: DataFrame,
      props: Map[String, String] = Map.empty,
      partCols: Seq[String] = Nil): Unit = {
    withTable("source") {
      createTableWithDataFrameWriterV2API(
        "source",
        data,
        Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> mode(props)))

      spark.sql(
        s"""
           |CREATE OR REPLACE TABLE $tableName
           |USING DELTA
           |${partitionStmt(partCols)}
           |${propString(props)}
           |AS SELECT * FROM source
           |""".stripMargin)
    }
  }

  protected def createTableWithSQLAPI(
      tableName: String,
      data: DataFrame,
      props: Map[String, String] = Map.empty,
      partCols: Seq[String] = Nil): Unit = {
    withTable("source") {
      spark.sql(
        s"""
           |CREATE TABLE $tableName (${data.schema.toDDL})
           |USING DELTA
           |${partitionStmt(partCols)}
           |${propString(props)}
           |""".stripMargin)
      data.write.format("delta").mode("append").saveAsTable(tableName)
    }
  }

  protected def createTableWithCTAS(
      tableName: String,
      data: DataFrame,
      props: Map[String, String] = Map.empty,
      partCols: Seq[String] = Nil): Unit = {
    withTable("source") {
      createTableWithDataFrameWriterV2API(
        "source",
        data,
        Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> mode(props)))

      spark.sql(
        s"""
           |CREATE TABLE $tableName
           |USING DELTA
           |${partitionStmt(partCols)}
           |${propString(props)}
           |AS SELECT * FROM source
           |""".stripMargin)
    }
  }

  protected def createTableWithDataFrameAPI(
      tableName: String,
      data: DataFrame,
      props: Map[String, String] = Map.empty,
      partCols: Seq[String]): Unit = {
    val sqlConfs = props.map { case (key, value) =>
      "spark.databricks.delta.properties.defaults." + key.stripPrefix("delta.") -> value
    }
    withSQLConf(sqlConfs.toList: _*) {
      if (partCols.nonEmpty) {
        data.write.format("delta")
          .partitionBy(partCols.map(name => s"`$name`"): _*).saveAsTable(tableName)
      } else {
        data.write.format("delta").saveAsTable(tableName)
      }
    }
  }

  protected def createTableWithDataFrameWriterV2API(
      tableName: String,
      data: DataFrame,
      props: Map[String, String] = Map.empty,
      partCols: Seq[String] = Seq.empty): Unit = {

    val writer = data.writeTo(tableName).using("delta")
    props.foreach(prop => writer.tableProperty(prop._1, prop._2))
    val partColumns = partCols.map(name => expr(s"`$name`"))
    if (partCols.nonEmpty) writer.partitionedBy(partColumns.head, partColumns.tail: _*)
    writer.create()
  }

  protected def assertException(message: String)(block: => Unit): Unit = {
    val e = intercept[Exception](block)

    assert(e.getMessage.contains(message))
  }

  protected def assertExceptionOneOf(messages: Seq[String])(block: => Unit): Unit = {
    val e = intercept[Exception](block)
    assert(messages.exists(x => e.getMessage.contains(x)))
  }
}

class DeltaArbitraryColumnNameSuite extends QueryTest
  with DeltaArbitraryColumnNameSuiteBase
  with GivenWhenThen {

  private def testCreateTable(): Unit = {
    val allProps = supportedModes
      .map(mode => Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> mode)) ++
      // none mode
      Seq(Map.empty[String, String])

    def withProps(props: Map[String, String])(createFunc: => Unit) = {
      withTable("t1") {
        if (mode(props) != "none") {
          createFunc
          checkAnswer(spark.table("t1"), nestedData)
        } else {
          val e = intercept[AnalysisException] {
            createFunc
          }
          assert(e.getMessage.contains("Found invalid character(s)"))
        }
      }
    }

    allProps.foreach { props =>
      withProps(props) {
        Given(s"with SQL CREATE TABLE API, mode ${mode(props)}")
        createTableWithSQLAPI("t1",
          nestedData,
          props,
          partCols = Seq(colName("a")))
      }

      withProps(props) {
        Given(s"with SQL CTAS API, mode ${mode(props)}")
        createTableWithCTAS("t1",
          nestedData,
          props,
          partCols = Seq(colName("a"))
        )
      }

      withProps(props) {
        Given(s"with SQL CREATE OR REPLACE TABLE API, mode ${mode(props)}")
        createTableWithSQLCreateOrReplaceAPI("t1",
          nestedData,
          props,
          partCols = Seq(colName("a")))
      }

      withProps(props) {
        Given(s"with DataFrame API, mode ${mode(props)}")
        createTableWithDataFrameAPI("t1",
          nestedData,
          props,
          partCols = Seq(colName("a")))
      }

      withProps(props) {
        Given(s"with DataFrameWriterV2 API, mode ${mode(props)}")
        createTableWithDataFrameWriterV2API("t1",
          nestedData,
          props,
          // TODO: make DataFrameWriterV2 work with arbitrary partition column names
          partCols = Seq.empty)
      }
    }
  }

  test("create table") {
    testCreateTable()
  }

  testColumnMapping("schema evolution and simple query") { mode =>
    withTable("t1") {
      createTableWithSQLAPI("t1",
        nestedData,
        Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> mode),
        partCols = Seq(colName("a"))
      )
      val newNestedData =
        spark.createDataFrame(
          Seq(Row("str3", Row("str1.3", 3), Map("k3" -> "v3"), Array(3, 33), "new value")).asJava,
          nestedSchema.add(colName("e"), StringType))
      newNestedData.write.format("delta")
        .option("mergeSchema", "true")
        .mode("append").saveAsTable("t1")
      checkAnswer(
        spark.table("t1"),
        Seq(
          Row("str1", Row("str1.1", 1), Map("k1" -> "v1"), Array(1, 11), null),
          Row("str2", Row("str1.2", 2), Map("k2" -> "v2"), Array(2, 22), null),
          Row("str3", Row("str1.3", 3), Map("k3" -> "v3"), Array(3, 33), "new value")))

      val colA = colName("a")
      val colB = colName("b")
      val colC = colName("c")
      val colD = colName("d")
      checkAnswer(
        spark.table("t1")
          .where(s"`$colA` > 'str1'")
          .where(s"`$colB`.`$colD` < 3")
          .select(s"`$colB`.`$colC`"),
        Row("str1.2"))
    }
  }

  testColumnMapping("alter table add and replace columns") { mode =>
    withTable("t1") {
      createTableWithSQLAPI("t1",
        nestedData,
        Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> mode),
        partCols = Seq(colName("a"))
      )
      spark.sql(s"alter table t1 add columns (`${colName("e")}` string)")
      spark.sql("insert into t1 " +
        "values ('str3', struct('str1.3', 3), map('k3', 'v3'), array(3, 33), 'new value')")

      checkAnswer(
        spark.table("t1"),
        Seq(
          Row("str1", Row("str1.1", 1), Map("k1" -> "v1"), Array(1, 11), null),
          Row("str2", Row("str1.2", 2), Map("k2" -> "v2"), Array(2, 22), null),
          Row("str3", Row("str1.3", 3), Map("k3" -> "v3"), Array(3, 33), "new value")))

    }
  }
}
