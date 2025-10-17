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
import org.scalatest.GivenWhenThen

import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.types._

class DeltaColumnRenameSuite extends QueryTest
  with DeltaArbitraryColumnNameSuiteBase
  with GivenWhenThen {

  testColumnMapping("rename in column mapping mode") { mode =>
    withTable("t1") {
      createTableWithSQLAPI("t1",
        simpleNestedData,
        Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> mode),
        partCols = Seq("a"))

        spark.sql(s"Alter table t1 RENAME COLUMN b to b1")

      // insert data after rename
      spark.sql("insert into t1 " +
        "values ('str3', struct('str1.3', 3), map('k3', 'v3'), array(3, 33))")

      // some queries
      checkAnswer(
        spark.table("t1"),
        Seq(
          Row("str1", Row("str1.1", 1), Map("k1" -> "v1"), Array(1, 11)),
          Row("str2", Row("str1.2", 2), Map("k2" -> "v2"), Array(2, 22)),
          Row("str3", Row("str1.3", 3), Map("k3" -> "v3"), Array(3, 33))))

      checkAnswer(
        spark.table("t1").select("b1"),
        Seq(Row(Row("str1.1", 1)), Row(Row("str1.2", 2)), Row(Row("str1.3", 3))))

      checkAnswer(
        spark.table("t1").select("a", "b1.c").where("b1.c = 'str1.2'"),
        Seq(Row("str2", "str1.2")))

      // b is no longer visible
      val e = intercept[AnalysisException] {
        spark.table("t1").select("b").collect()
      }
      // The error class is renamed in Spark 3.4
      assert(e.getErrorClass == "UNRESOLVED_COLUMN.WITH_SUGGESTION"
        || e.getErrorClass == "MISSING_COLUMN" )

      // rename partition column
      spark.sql(s"Alter table t1 RENAME COLUMN a to a1")
      // rename nested column
      spark.sql(s"Alter table t1 RENAME COLUMN b1.c to c1")

      // rename and verify rename history
      val renameHistoryDf = sql("DESCRIBE HISTORY t1")
          .where("operation = 'RENAME COLUMN'")
          .select("version", "operationParameters")

      checkAnswer(renameHistoryDf,
        Row(2, Map("oldColumnPath" -> "b", "newColumnPath" -> "b1")) ::
          Row(4, Map("oldColumnPath" -> "a", "newColumnPath" -> "a1")) ::
          Row(5, Map("oldColumnPath" -> "b1.c", "newColumnPath" -> "b1.c1")) :: Nil)

      // cannot rename column to the same name
      assert(
        intercept[AnalysisException] {
          spark.sql(s"Alter table t1 RENAME COLUMN map to map")
        }.getMessage.contains("already exists"))

      // cannot rename to a different casing
      assert(
        intercept[AnalysisException] {
          spark.sql("Alter table t1 RENAME COLUMN arr to Arr")
        }.getMessage.contains("already exists"))

      // a is no longer visible
      val e2 = intercept[AnalysisException] {
        spark.table("t1").select("a").collect()
      }
      // The error class is renamed in Spark 3.4
      assert(e2.getErrorClass == "UNRESOLVED_COLUMN.WITH_SUGGESTION"
        || e2.getErrorClass == "MISSING_COLUMN" )

      // b1.c is no longer visible
      val e3 = intercept[AnalysisException] {
        spark.table("t1").select("b1.c").collect()
      }
      assert(e3.getMessage.contains("No such struct field"))

      // insert data after rename
      spark.sql("insert into t1 " +
        "values ('str4', struct('str1.4', 4), map('k4', 'v4'), array(4, 44))")

      checkAnswer(
        spark.table("t1").select("a1", "b1.c1", "map")
          .where("b1.c1 = 'str1.4'"),
        Seq(Row("str4", "str1.4", Map("k4" -> "v4"))))
    }
  }

  test("rename workflow: error, upgrade to name mode and then rename") {
    // error when not in the correct protocol and mode
    withTable("t1") {
      createTableWithSQLAPI("t1",
        simpleNestedData,
        partCols = Seq("a"))
       val e = intercept[AnalysisException] {
        spark.sql(s"Alter table t1 RENAME COLUMN map to map1")
       }
      assert(e.getMessage.contains("enable Column Mapping") &&
        e.getMessage.contains("mapping mode 'name'"))

      alterTableWithProps("t1", Map(
        DeltaConfigs.COLUMN_MAPPING_MODE.key -> "name",
        DeltaConfigs.MIN_READER_VERSION.key -> "2",
        DeltaConfigs.MIN_WRITER_VERSION.key -> "5"))

      // rename a column to have arbitrary chars
      spark.sql(s"Alter table t1 RENAME COLUMN a to `${colName("a")}`")

      // rename a column that already has arbitrary chars
      spark.sql(s"Alter table t1" +
        s" RENAME COLUMN `${colName("a")}` to `${colName("a1")}`")

      // rename partition column
      spark.sql(s"Alter table t1 RENAME COLUMN map to `${colName("map")}`")

      // insert data after rename
      spark.sql("insert into t1 " +
        "values ('str3', struct('str1.3', 3), map('k3', 'v3'), array(3, 33))")

      checkAnswer(
        spark.table("t1").select(colName("a1"), "b.d", colName("map"))
          .where("b.c >= 'str1.2'"),
        Seq(Row("str2", 2, Map("k2" -> "v2")),
          Row("str3", 3, Map("k3" -> "v3"))))

      // add old column back?
      spark.sql(s"alter table t1 add columns (a string, map map<string, string>)")

      // insert data after rename
      spark.sql("insert into t1 " +
        "values ('str4', struct('str1.4', 4), map('k4', 'v4'), array(4, 44)," +
        " 'new_str4', map('new_k4', 'new_v4'))")

      checkAnswer(
        spark.table("t1").select(colName("a1"), "a", colName("map"), "map")
          .where("b.c >= 'str1.2'"),
        Seq(
          Row("str2", null, Map("k2" -> "v2"), null),
          Row("str3", null, Map("k3" -> "v3"), null),
          Row("str4", "new_str4", Map("k4" -> "v4"), Map("new_k4" -> "new_v4"))))
    }
  }

  test("rename workflow: error, upgrade to name mode and then rename - " +
    "nested data with duplicated column name") {
    withTable("t1") {
      createTableWithSQLAPI("t1", simpleNestedDataWithDuplicatedNestedColumnName)
       val e = intercept[AnalysisException] {
        spark.sql(s"Alter table t1 RENAME COLUMN map to map1")
       }
      assert(e.getMessage.contains("enable Column Mapping") &&
        e.getMessage.contains("mapping mode 'name'"))

      // Upgrading this schema shouldn't cause any errors even if there are leaf column name
      // duplications such as a.c, b.c.
      alterTableWithProps("t1", Map(
        DeltaConfigs.COLUMN_MAPPING_MODE.key -> "name",
        DeltaConfigs.MIN_READER_VERSION.key -> "2",
        DeltaConfigs.MIN_WRITER_VERSION.key -> "5"))

      // rename shouldn't cause duplicates in column names
      Seq(("a", "b"), ("arr", "map")).foreach { case (from, to) =>
        val e = intercept[AnalysisException] {
          spark.sql(s"Alter table t1 RENAME COLUMN $from to $to")
        }
        assert(e.getMessage.contains("Cannot rename column"))
      }

      // spice things up by changing name to arbitrary chars
      spark.sql(s"Alter table t1 RENAME COLUMN a to `${colName("a")}`")
      // rename partition column
      spark.sql(s"Alter table t1 RENAME COLUMN map to `${colName("map")}`")

      // insert data after rename
      spark.sql("insert into t1 " +
        "values (struct('str3', 3), struct('str1.3', 3), map('k3', 'v3'), array(3, 33))")

      checkAnswer(
        spark.table("t1").select(colName("a"), "b.d", colName("map"))
          .where("b.c >= 'str1.2'"),
        Seq(Row(Row("str2", 2), 2, Map("k2" -> "v2")),
          Row(Row("str3", 3), 3, Map("k3" -> "v3"))))

      // add old column back?
      spark.sql(s"alter table t1 add columns (a string, map map<string, string>)")

      // insert data after rename
      spark.sql("insert into t1 " +
        "values (struct('str4', 4), struct('str1.4', 4), map('k4', 'v4'), array(4, 44)," +
        " 'new_str4', map('new_k4', 'new_v4'))")

      checkAnswer(
        spark.table("t1").select(colName("a"), "a", colName("map"), "map")
          .where("b.c >= 'str1.2'"),
        Seq(
          Row(Row("str2", 2), null, Map("k2" -> "v2"), null),
          Row(Row("str3", 3), null, Map("k3" -> "v3"), null),
          Row(Row("str4", 4), "new_str4", Map("k4" -> "v4"), Map("new_k4" -> "new_v4"))))
    }
  }

  test("rename with constraints") {
    withTable("t1") {
      val schemaWithNotNull =
        simpleNestedData.schema.toDDL.replace("c: STRING", "c: STRING NOT NULL")
          .replace("`c`: STRING", "`c`: STRING NOT NULL")

      withTable("source") {
        spark.sql(
          s"""
             |CREATE TABLE t1 ($schemaWithNotNull)
             |USING DELTA
             |${partitionStmt(Seq("a"))}
             |${propString(Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> "name"))}
             |""".stripMargin)
        simpleNestedData.write.format("delta").mode("append").saveAsTable("t1")
      }

      spark.sql("alter table t1 add constraint rangeABC check (concat(a, a) > 'str')")
      spark.sql("alter table t1 add constraint rangeBD check (`b`.`d` > 0)")

      spark.sql("alter table t1 add constraint arrValue check (arr[0] > 0)")

      assertException("Cannot alter column a") {
        spark.sql("alter table t1 rename column a to a1")
      }

      assertException("Cannot alter column arr") {
        spark.sql("alter table t1 rename column arr to arr1")
      }


      // cannot rename b because its child is referenced
      assertException("Cannot alter column b") {
        spark.sql("alter table t1 rename column b to b1")
      }

      // can still rename b.c because it's referenced by a null constraint
      spark.sql("alter table t1 rename column b.c to c1")

      spark.sql("insert into t1 " +
        "values ('str3', struct('str1.3', 3), map('k3', 'v3'), array(3, 33))")

      assertException("CHECK constraint rangeabc (concat(a, a) > 'str')") {
        spark.sql("insert into t1 " +
          "values ('fail constraint', struct('str1.3', 3), map('k3', 'v3'), array(3, 33))")
      }

      assertException("CHECK constraint rangebd (b.d > 0)") {
        spark.sql("insert into t1 " +
          "values ('str3', struct('str1.3', -1), map('k3', 'v3'), array(3, 33))")
      }

      assertException("NOT NULL constraint violated for column: b.c1") {
        spark.sql("insert into t1 " +
          "values ('str3', struct(null, 3), map('k3', 'v3'), array(3, 33))")
      }

      // this is a safety flag - it won't error when you turn it off
      withSQLConf(DeltaSQLConf.DELTA_ALTER_TABLE_CHANGE_COLUMN_CHECK_EXPRESSIONS.key -> "false") {
        spark.sql("alter table t1 rename column a to a1")
        spark.sql("alter table t1 rename column arr to arr1")
        spark.sql("alter table t1 rename column b to b1")
      }
    }
  }

  test("rename with constraints - map element") {
    withTable("t1") {
      val schemaWithNotNull =
        simpleNestedData.schema.toDDL.replace("c: STRING", "c: STRING NOT NULL")
          .replace("`c`: STRING", "`c`: STRING NOT NULL")

      withTable("source") {
        spark.sql(
          s"""
             |CREATE TABLE t1 ($schemaWithNotNull)
             |USING DELTA
             |${partitionStmt(Seq("a"))}
             |${propString(Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> "name"))}
             |""".stripMargin)
        simpleNestedData.write.format("delta").mode("append").saveAsTable("t1")
      }

      spark.sql("alter table t1 add constraint" +
        " mapValue check (not array_contains(map_keys(map), 'k1') or map['k1'] = 'v1')")

      assertException("Cannot alter column map") {
        spark.sql("alter table t1 rename column map to map1")
      }

      spark.sql("insert into t1 " +
        "values ('str3', struct('str1.3', 3), map('k3', 'v3'), array(3, 33))")
    }
  }

  test("rename with generated column") {
    withTable("t1") {
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

      val genCol3 = io.delta.tables.DeltaTable.columnBuilder(spark, "genCol3")
        .dataType("string")
        .generatedAlwaysAs("concat(a, a)")
        .build()

      tableBuilder
        .addColumn(genCol1)
        .addColumn(genCol2)
        .addColumn(genCol3)
        .partitionedBy("genCol2")
        .execute()

      simpleNestedData.write.format("delta").mode("append").saveAsTable("t1")

      assertException("Cannot alter column a") {
        spark.sql("alter table t1 rename column a to a1")
      }

      assertException("Cannot alter column b") {
        spark.sql("alter table t1 rename column b to b1")
      }

      assertException("Cannot alter column b.d") {
        spark.sql("alter table t1 rename column b.d to d1")
      }

      assertException("Cannot alter column arr") {
        spark.sql("alter table t1 rename column arr to arr1")
      }

      // you can still rename b.c
      spark.sql("alter table t1 rename column b.c to c1")

      // The following is just to show generated columns are actually there

      // add new data (without data for generated columns so that they are auto populated)
      spark.createDataFrame(
        Seq(Row("str3", Row("str1.3", 3), Map("k3" -> "v3"), Array(3, 33))).asJava,
        new StructType()
         .add("a", StringType, true)
          .add("b",
        new StructType()
          .add("c1", StringType, true)
          .add("d", IntegerType, true))
          .add("map", MapType(StringType, StringType), true)
          .add("arr", ArrayType(IntegerType), true))
      .write.format("delta").mode("append").saveAsTable("t1")

      checkAnswer(spark.table("t1"),
        Seq(
            Row("str1", Row("str1.1", 1), Map("k1" -> "v1"), Array(1, 11), 4, 101, "str1str1"),
            Row("str2", Row("str1.2", 2), Map("k2" -> "v2"), Array(2, 22), 4, 202, "str2str2"),
            Row("str3", Row("str1.3", 3), Map("k3" -> "v3"), Array(3, 33), 4, 303, "str3str3")))

      // this is a safety flag - if you turn it off, it will still error but msg is not as helpful
      withSQLConf(DeltaSQLConf.DELTA_ALTER_TABLE_CHANGE_COLUMN_CHECK_EXPRESSIONS.key -> "false") {
        assertException("A generated column cannot use a non-existent column") {
          spark.sql("alter table t1 rename column arr to arr1")
        }
        assertExceptionOneOf(Seq("No such struct field d in c1, d1",
          "No such struct field `d` in `c1`, `d1`")) {
          spark.sql("alter table t1 rename column b.d to d1")
        }
      }
    }
  }

  /**
   * Covers renaming a nested field using the ALTER TABLE command.
   * @param initialColumnType Type of the single column used to create the initial test table.
   * @param fieldToRename     Old and new name of the field to rename.
   * @param updatedColumnType Expected type of the single column after renaming the nested field.
   */
  def testRenameNestedField(testName: String)(
      initialColumnType: String,
      fieldToRename: (String, String),
      updatedColumnType: String): Unit =
    testColumnMapping(s"ALTER TABLE RENAME COLUMN - nested $testName") { mode =>
      withTempDir { dir =>
        withTable("delta_test") {
          sql(
            s"""
               |CREATE TABLE delta_test (data $initialColumnType)
               |USING delta
               |TBLPROPERTIES (${DeltaConfigs.COLUMN_MAPPING_MODE.key} = '${mode}')
               |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)

          val expectedInitialType = initialColumnType.filterNot(_.isWhitespace)
          val expectedUpdatedType = updatedColumnType.filterNot(_.isWhitespace)
          val fieldName = s"data.${fieldToRename._1}"

          def columnType: DataFrame =
            sql("DESCRIBE TABLE delta_test")
              .filter("col_name = 'data'")
              .select("data_type")
          checkAnswer(columnType, Row(expectedInitialType))

          sql(s"ALTER TABLE delta_test RENAME COLUMN $fieldName TO ${fieldToRename._2}")
          checkAnswer(columnType, Row(expectedUpdatedType))
        }
      }
    }

  testRenameNestedField("struct in map key")(
    initialColumnType = "map<struct<a: int, b: string>, int>",
    fieldToRename = "key.b" -> "c",
    updatedColumnType = "map<struct<a: int, c: string>, int>")

  testRenameNestedField("struct in map value")(
    initialColumnType = "map<int, struct<a: int, b: string>>",
    fieldToRename = "value.b" -> "c",
    updatedColumnType = "map<int, struct<a: int, c: string>>")

  testRenameNestedField("struct in array")(
    initialColumnType = "array<struct<a: int, b: string>>",
    fieldToRename = "element.b" -> "c",
    updatedColumnType = "array<struct<a: int, c: string>>")

  testRenameNestedField("struct in nested map keys")(
    initialColumnType = "map<map<struct<a: int, b: string>, int>, int>",
    fieldToRename = "key.key.b" -> "c",
    updatedColumnType = "map<map<struct<a: int, c: string>, int>, int>")

  testRenameNestedField("struct in nested map values")(
    initialColumnType = "map<int, map<int, struct<a: int, b: string>>>",
    fieldToRename = "value.value.b" -> "c",
    updatedColumnType = "map<int, map<int, struct<a: int, c: string>>>")

  testRenameNestedField("struct in nested arrays")(
    initialColumnType = "array<array<struct<a: int, b: string>>>",
    fieldToRename = "element.element.b" -> "c",
    updatedColumnType = "array<array<struct<a: int, c: string>>>")

  testRenameNestedField("struct in nested array and map")(
    initialColumnType = "array<map<int, struct<a: int, b: string>>>",
    fieldToRename = "element.value.b" -> "c",
    updatedColumnType = "array<map<int, struct<a: int, c: string>>>")

  testRenameNestedField("struct in nested map key and array")(
    initialColumnType = "map<array<struct<a: int, b: string>>, int>",
    fieldToRename = "key.element.b" -> "c",
    updatedColumnType = "map<array<struct<a: int, c: string>>, int>")

  testRenameNestedField("struct in nested map value and array")(
    initialColumnType = "map<int, array<struct<a: int, b: string>>>",
    fieldToRename = "value.element.b" -> "c",
    updatedColumnType = "map<int, array<struct<a: int, c: string>>>")

  testColumnMapping("ALTER TABLE RENAME COLUMN - rename fields nested in maps") { mode =>
    withTable("t1") {
      val rows = Seq(
        Row(Map(Row(1) -> Map(Row(10) -> Row(11)))),
        Row(Map(Row(2) -> Map(Row(20) -> Row(21)))))

      val df = spark.createDataFrame(
        rows = rows.asJava,
        schema = new StructType()
          .add("a", MapType(
            new StructType().add("x", IntegerType),
            MapType(
              new StructType().add("y", IntegerType),
              new StructType().add("z", IntegerType)))))

      createTableWithSQLAPI("t1", df, Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> mode))

      spark.sql(s"ALTER TABLE t1 RENAME COLUMN a.key.x to x1")
      checkAnswer(spark.table("t1"), rows)

      spark.sql(s"ALTER TABLE t1 RENAME COLUMN a.value.key.y to y1")
      checkAnswer(spark.table("t1"), rows)

      spark.sql(s"ALTER TABLE t1 RENAME COLUMN a.value.value.z to z1")
      checkAnswer(spark.table("t1"), rows)

      // Insert data after rename.
      spark.sql("INSERT INTO t1 " +
        "VALUES (map(named_struct('x', 3), map(named_struct('y', 30), named_struct('z', 31))))")
      checkAnswer(spark.table("t1"), rows :+ Row(Map(Row(3) -> Map(Row(30) -> Row(31)))))
    }
  }

  testColumnMapping("ALTER TABLE RENAME COLUMN - rename fields nested in arrays") { mode =>
    withTable("t1") {
      val rows = Seq(
        Row(Array(Array(Row(10, 11), Row(12, 13)), Array(Row(14, 15), Row(16, 17)))),
        Row(Array(Array(Row(20, 21), Row(22, 23)), Array(Row(24, 25), Row(26, 27)))))

      val schema = new StructType()
        .add("a", ArrayType(ArrayType(
          new StructType()
            .add("x", IntegerType)
            .add("y", IntegerType))))
      val df = spark.createDataFrame(rows.asJava, schema)

      createTableWithSQLAPI("t1", df, Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> mode))

      spark.sql(s"ALTER TABLE t1 RENAME COLUMN a.element.element.x to x1")
      checkAnswer(spark.table("t1"), df)

      spark.sql(s"ALTER TABLE t1 RENAME COLUMN a.element.element.y to y1")
      checkAnswer(spark.table("t1"), df)

      // Insert data after rename.
      spark.sql(
        """
          |INSERT INTO t1 VALUES (
          |array(
          |  array(named_struct('x', 30, 'y', 31), named_struct('x', 32, 'y', 33)),
          |  array(named_struct('x', 34, 'y', 35), named_struct('x', 36, 'y', 37))))
          """.stripMargin)

      val expDf3 = spark.createDataFrame(
        (rows :+ Row(Array(Array(Row(30, 31), Row(32, 33)), Array(Row(34, 35), Row(36, 37)))))
          .asJava,
        schema)
      checkAnswer(spark.table("t1"), expDf3)
    }
  }

  testColumnMapping("rename column with special characters and data skipping stats") { mode =>
    withTable("t1") {
      spark.sql(
        s"""
           |CREATE TABLE t1 (c int, d string)
           |USING DELTA
           |TBLPROPERTIES (
           |  '${DeltaConfigs.COLUMN_MAPPING_MODE.key}' = '$mode',
           |  '${DeltaConfigs.DATA_SKIPPING_STATS_COLUMNS.key}' = 'c,d'
           |)
           |""".stripMargin)
      spark.sql("INSERT INTO t1 VALUES (1, 'value1'), (2, 'value2'), (3, 'value3')")

      // Verify stats are collected before rename
      val deltaLog = DeltaLog.forTable(spark, spark.sessionState.catalog.getTableMetadata(
        spark.sessionState.sqlParser.parseTableIdentifier("t1")))
      val statsBefore = deltaLog.update().allFiles.collect().head.stats
      assert(statsBefore != null && statsBefore.contains("numRecords"))

      // Rename column c to a name with special characters
      spark.sql("ALTER TABLE t1 RENAME COLUMN c TO `c#2`")

      // Verify the rename worked
      checkAnswer(
        spark.table("t1"),
        Seq(Row(1, "value1"), Row(2, "value2"), Row(3, "value3")))

      // Verify we can query using the new column name
      checkAnswer(
        spark.sql("SELECT `c#2` FROM t1 WHERE `c#2` > 1"),
        Seq(Row(2), Row(3)))

      // Insert data after rename to ensure stats collection still works
      spark.sql("INSERT INTO t1 VALUES (4, 'value4'), (5, 'value5')")

      checkAnswer(
        spark.table("t1"),
        Seq(
          Row(1, "value1"),
          Row(2, "value2"),
          Row(3, "value3"),
          Row(4, "value4"),
          Row(5, "value5")))

      // Verify stats are still being collected after rename
      val statsAfter = deltaLog.update().allFiles.collect().last.stats
      assert(statsAfter != null && statsAfter.contains("numRecords"))

      // Verify the rename history includes the escaped column name
      val renameHistoryDf = sql("DESCRIBE HISTORY t1")
        .where("operation = 'RENAME COLUMN'")
        .select("operationParameters")

      val operationParams = renameHistoryDf.head().getMap[String, String](0)
      assert(operationParams("oldColumnPath") == "c")
      assert(operationParams("newColumnPath").contains("c#2"))

      // Rename c#2 back to c before renaming column d
      spark.sql("ALTER TABLE t1 RENAME COLUMN `c#2` TO c")

      // Verify rename back worked
      checkAnswer(
        spark.sql("SELECT c FROM t1 WHERE c = 3"),
        Seq(Row(3)))
    }
  }
}
