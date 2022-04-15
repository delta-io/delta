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

import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.types.{ArrayType, IntegerType, MapType, StringType, StructType}

class DeltaDropColumnSuite extends QueryTest with DeltaArbitraryColumnNameSuiteBase {

  override protected val sparkConf: SparkConf =
    super.sparkConf.set(DeltaSQLConf.DELTA_ALTER_TABLE_DROP_COLUMN_ENABLED.key, "true")

  test("drop column") {
    withTable("t1") {
      createTableWithSQLAPI("t1",
        simpleNestedData,
        Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> "name"))

      val schemaAfterDelete = StructType(simpleNestedSchema.fields.filter(_.name != "arr"))

      // alter table replace columns
      spark.sql(s"alter table t1 replace columns (${schemaAfterDelete.toDDL})")
      checkAnswer(spark.table("t1"), simpleNestedData.drop("arr"))

      // alter table drop column
      spark.sql("alter table t1 drop columns (a, b.c)")
      checkAnswer(spark.table("t1"),
        Seq(
          Row(Row(1), Map("k1" -> "v1")),
          Row(Row(2), Map("k2" -> "v2"))))

      // column cannot be queried anymore - even metadata-only queries
      assert(intercept[AnalysisException] {
        spark.table("t1").where("a = 'str1'").collect()
      }.getMessage.contains("does not exist"))

      assert(intercept[AnalysisException] {
        spark.table("t1").select("min(a)").collect()
      }.getMessage.contains("does not exist"))

      checkAnswer(
        spark.sql("describe history t1")
          .select("operation", "operationParameters")
          .where("version = 3"),
        Seq(
          Row("DROP COLUMNS", Map("columns" -> """["a","b.c"]"""))))

      // dropping non-existent field would fail
      assertException("Missing field a") {
        spark.sql("alter table t1 drop column (a)")
      }
      // cannot drop the last nested field
      val e = intercept[AnalysisException] {
        spark.sql("alter table t1 drop columns (b.d)")
      }
      assert(e.getMessage.contains("Cannot drop column from a struct type with a single field"))

      // can drop the parent column
      spark.sql("alter table t1 drop columns b")

      // cannot drop the last top-level field
      val e2 = intercept[AnalysisException] {
        spark.sql("alter table t1 drop columns map")
      }
      assert(e2.getMessage.contains("Cannot drop column from a struct type with a single field"))

      spark.sql("alter table t1 add column (e struct<e1 string, e2 string>)")

      // rename column to contain arbitrary chars
      spark.sql(s"alter table t1 rename column map to `${colName("map")}`")

      // try drop map after rename to arbitrary chars now
      spark.sql(s"alter table t1 drop columns `${colName("map")}`")

      // corner-case test - can drop a nested column when the top-level column is the only column
      spark.sql("alter table t1 drop column e.e1")
    }
  }

  test("drop column with constraints") {
    withTable("t1") {
      val schemaWithNotNull =
        simpleNestedData.schema.toDDL.replace("c: STRING", "c: STRING NOT NULL")

      withTable("source") {
        spark.sql(
          s"""
             |CREATE TABLE t1 ($schemaWithNotNull)
             |USING DELTA
             |${propString(Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> "name"))}
             |""".stripMargin)
        simpleNestedData.write.format("delta").mode("append").saveAsTable("t1")
      }

      spark.sql("alter table t1 add constraint rangeABC check (concat(a, a) > 'str')")
      spark.sql("alter table t1 add constraint rangeBD check (`b`.`d` > 0)")
      spark.sql("alter table t1" +
        " add constraint mapValue check (map['k1'] = 'v1' or map['k1'] is null)")

      spark.sql("alter table t1 add constraint arrValue check (arr[0] > 0)")

      assertException("Cannot drop column a") {
        spark.sql("alter table t1 drop column a")
      }

      assertException("Cannot drop column arr") {
        spark.sql("alter table t1 drop column arr")
      }

      assertException("Cannot drop column map") {
        spark.sql("alter table t1 drop column map")
      }

      // cannot drop b because its child is referenced
      assertException("Cannot drop column b") {
        spark.sql("alter table t1 drop column b")
      }

      // can still drop b.c because it's referenced by a null constraint
      spark.sql("alter table t1 drop column b.c")

      // this is a safety flag - it won't error when you turn it off
      withSQLConf(DeltaSQLConf.DELTA_ALTER_TABLE_CHANGE_COLUMN_CHECK_EXPRESSIONS.key -> "false") {
        spark.sql("alter table t1 drop columns (b, arr)")
      }
    }
  }

  test("drop with generated column") {
    withTable("t1") {
      withSQLConf(DeltaSQLConf.DELTA_ALTER_TABLE_DROP_COLUMN_ENABLED.key -> "true") {
        val tableBuilder = io.delta.tables.DeltaTable.create(spark).tableName("t1")
        tableBuilder.property("delta.columnMapping.mode", "name")

        // add existing columns
        simpleNestedSchema.map(field => (field.name, field.dataType)).foreach(col => {
          val (colName, dataType) = col
          val columnBuilder = io.delta.tables.DeltaTable.columnBuilder(spark, colName)
          columnBuilder.dataType(dataType.sql)
          tableBuilder.addColumn(columnBuilder.build())
        })

        // add generated columns
        val genCol1 = io.delta.tables.DeltaTable.columnBuilder(spark, "genCol1")
          .dataType("int")
          .generatedAlwaysAs("length(a)")
          .build()

        val genCol2 = io.delta.tables.DeltaTable.columnBuilder(spark, "genCol2")
          .dataType("int")
          .generatedAlwaysAs("b.d * 100 + arr[0]")
          .build()

        tableBuilder
          .addColumn(genCol1)
          .addColumn(genCol2)
          .execute()

        simpleNestedData.write.format("delta").mode("append").saveAsTable("t1")

        assertException("Cannot drop column a") {
          spark.sql("alter table t1 drop column a")
        }

        assertException("Cannot drop column b") {
          spark.sql("alter table t1 drop column b")
        }

        assertException("Cannot drop column b.d") {
          spark.sql("alter table t1 drop column b.d")
        }

        assertException("Cannot drop column arr") {
          spark.sql("alter table t1 drop column arr")
        }

        // you can still drop b.c as it has no dependent gen col
        spark.sql("alter table t1 drop column b.c")

        // you can also drop a generated column itself
        spark.sql("alter table t1 drop column genCol1")

        // add new data after dropping
        spark.createDataFrame(
          Seq(Row("str3", Row(3), Map("k3" -> "v3"), Array(3, 33))).asJava,
          new StructType()
            .add("a", StringType, true)
            .add("b",
              new StructType()
                .add("d", IntegerType, true))
            .add("map", MapType(StringType, StringType), true)
            .add("arr", ArrayType(IntegerType), true))
          .write.format("delta").mode("append").saveAsTable("t1")

        checkAnswer(spark.table("t1"),
          Seq(
            Row("str1", Row(1), Map("k1" -> "v1"), Array(1, 11), 101),
            Row("str2", Row(2), Map("k2" -> "v2"), Array(2, 22), 202),
            Row("str3", Row(3), Map("k3" -> "v3"), Array(3, 33), 303)))

        // this is a safety flag - if you turn it off, it will still error but msg is not as helpful
        withSQLConf(DeltaSQLConf.DELTA_ALTER_TABLE_CHANGE_COLUMN_CHECK_EXPRESSIONS.key -> "false") {
          assertException("A generated column cannot use a non-existent column") {
            spark.sql("alter table t1 drop column arr")
          }
        }
      }
    }
  }

  test("dropping all columns is not allowed") {
    withTable("t1") {
      createTableWithSQLAPI("t1",
        simpleNestedData,
        Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> "name")
      )
      val e = intercept[AnalysisException] {
        sql("alter table t1 drop columns (a, b, map, arr)")
      }
      assert(e.getMessage.contains("Cannot drop column"))
    }
  }

  test("dropping partition columns is not allowed") {
    withTable("t1") {
      createTableWithSQLAPI("t1",
        simpleNestedData,
        Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> "name"),
        partCols = Seq("a")
      )
      val e = intercept[AnalysisException] {
        sql("alter table t1 drop columns (a)")
      }
      assert(e.getMessage.contains("Dropping partition columns is not allowed"))
    }
  }
}
