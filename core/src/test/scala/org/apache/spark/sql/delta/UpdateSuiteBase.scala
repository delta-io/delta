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

// scalastyle:off import.ordering.noEmptyLine
import java.io.File
import java.util.Locale

import scala.language.implicitConversions

import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.hadoop.fs.Path
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

abstract class UpdateSuiteBase
  extends QueryTest
  with SharedSparkSession
  with BeforeAndAfterEach  with SQLTestUtils
  with DeltaTestUtilsForTempViews {
  import testImplicits._

  var tempDir: File = _

  var deltaLog: DeltaLog = _

  protected def tempPath = tempDir.getCanonicalPath

  protected def readDeltaTable(table: String): DataFrame = {
    spark.read.format("delta").table(table)
  }

  protected def readDeltaTableByPath(path: String): DataFrame = {
    spark.read.format("delta").load(path)
  }

  override def beforeEach() {
    super.beforeEach()
    // Using a space in path to provide coverage for special characters.
    tempDir = Utils.createTempDir(namePrefix = "spark test")
    deltaLog = DeltaLog.forTable(spark, new Path(tempPath))
  }

  override def afterEach() {
    try {
      Utils.deleteRecursively(tempDir)
      DeltaLog.clearCache()
    } finally {
      super.afterEach()
    }
  }

  protected def executeUpdate(target: String, set: Seq[String], where: String): Unit = {
    executeUpdate(target, set.mkString(", "), where)
  }

  protected def executeUpdate(target: String, set: String, where: String = null): Unit

  protected def append(df: DataFrame, partitionBy: Seq[String] = Nil): Unit = {
    val writer = df.write.format("delta").mode("append")
    if (partitionBy.nonEmpty) {
      writer.partitionBy(partitionBy: _*)
    }
    writer.save(deltaLog.dataPath.toString)
  }

  implicit def jsonStringToSeq(json: String): Seq[String] = json.split("\n")

  val fileFormat: String = "parquet"

  protected def checkUpdate(
      condition: Option[String],
      setClauses: String,
      expectedResults: Seq[Row],
      tableName: Option[String] = None,
      prefix: String = ""): Unit = {
    executeUpdate(tableName.getOrElse(s"delta.`$tempPath`"), setClauses, where = condition.orNull)
    checkAnswer(
      tableName
        .map(readDeltaTable(_))
        .getOrElse(readDeltaTableByPath(tempPath))
        .select(s"${prefix}key", s"${prefix}value"),
      expectedResults)
  }

  test("basic case") {
    append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"))
    checkUpdate(condition = None, setClauses = "key = 1, value = 2",
      expectedResults = Row(1, 2) :: Row(1, 2) :: Row(1, 2) :: Row(1, 2) :: Nil)
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"basic update - Delta table by path - Partition=$isPartitioned") {
      withTable("deltaTable") {
        val partitions = if (isPartitioned) "key" :: Nil else Nil
        append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"), partitions)

        checkUpdate(
          condition = Some("key >= 1"),
          setClauses = "value = key + value, key = key + 1",
          expectedResults = Row(0, 3) :: Row(2, 5) :: Row(2, 2) :: Row(3, 4) :: Nil,
          tableName = Some(s"delta.`$tempPath`"))
      }
    }
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"basic update - Delta table by name - Partition=$isPartitioned") {
      withTable("delta_table") {
        val partitionByClause = if (isPartitioned) "PARTITIONED BY (key)" else ""
        sql(s"""
             |CREATE TABLE delta_table(key INT, value INT)
             |USING delta
             |OPTIONS('path'='$tempPath')
             |$partitionByClause
           """.stripMargin)

        append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"))

        checkUpdate(
          condition = Some("key >= 1"),
          setClauses = "value = key + value, key = key + 1",
          expectedResults = Row(0, 3) :: Row(2, 5) :: Row(2, 2) :: Row(3, 4) :: Nil,
          tableName = Some("delta_table"))
      }
    }
  }

  Seq(true, false).foreach { skippingEnabled =>
    Seq(true, false).foreach { isPartitioned =>
      test(s"data and partition predicates - Partition=$isPartitioned Skipping=$skippingEnabled") {
        withSQLConf(DeltaSQLConf.DELTA_STATS_SKIPPING.key -> skippingEnabled.toString) {
          val partitions = if (isPartitioned) "key" :: Nil else Nil
          append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"), partitions)

          checkUpdate(condition = Some("key >= 1 and value != 4"),
            setClauses = "value = key + value, key = key + 5",
            expectedResults = Row(0, 3) :: Row(7, 4) :: Row(1, 4) :: Row(6, 2) :: Nil)
        }
      }
    }
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"SC-12276: table has null values - partitioned=$isPartitioned") {
      val partitions = if (isPartitioned) "key" :: Nil else Nil
      append(Seq(("a", 1), (null, 2), (null, 3), ("d", 4)).toDF("key", "value"), partitions)

      // predicate evaluates to null; no-op
      checkUpdate(condition = Some("key = null"),
        setClauses = "value = -1",
        expectedResults = Row("a", 1) :: Row(null, 2) :: Row(null, 3) :: Row("d", 4) :: Nil)

      checkUpdate(condition = Some("key = 'a'"),
        setClauses = "value = -1",
        expectedResults = Row("a", -1) :: Row(null, 2) :: Row(null, 3) :: Row("d", 4) :: Nil)

      checkUpdate(condition = Some("key is null"),
        setClauses = "value = -2",
        expectedResults = Row("a", -1) :: Row(null, -2) :: Row(null, -2) :: Row("d", 4) :: Nil)

      checkUpdate(condition = Some("key is not null"),
        setClauses = "value = -3",
        expectedResults = Row("a", -3) :: Row(null, -2) :: Row(null, -2) :: Row("d", -3) :: Nil)

      checkUpdate(condition = Some("key <=> null"),
        setClauses = "value = -4",
        expectedResults = Row("a", -3) :: Row(null, -4) :: Row(null, -4) :: Row("d", -3) :: Nil)
    }
  }

  test("basic case - condition is false") {
    append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"))
    checkUpdate(condition = Some("1 != 1"), setClauses = "key = 1, value = 2",
      expectedResults = Row(2, 2) :: Row(1, 4) :: Row(1, 1) :: Row(0, 3) :: Nil)
  }

  test("basic case - condition is true") {
    append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"))
    checkUpdate(condition = Some("1 = 1"), setClauses = "key = 1, value = 2",
      expectedResults = Row(1, 2) :: Row(1, 2) :: Row(1, 2) :: Row(1, 2) :: Nil)
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"basic update - without where - Partition=$isPartitioned") {
      val partitions = if (isPartitioned) "key" :: Nil else Nil
      append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"), partitions)

      checkUpdate(condition = None, setClauses = "key = 1, value = 2",
        expectedResults = Row(1, 2) :: Row(1, 2) :: Row(1, 2) :: Row(1, 2) :: Nil)
    }
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"basic update - without where and partial columns - Partition=$isPartitioned") {
      val partitions = if (isPartitioned) "key" :: Nil else Nil
      append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"), partitions)

      checkUpdate(condition = None, setClauses = "key = 1",
        expectedResults = Row(1, 1) :: Row(1, 2) :: Row(1, 3) :: Row(1, 4) :: Nil)
    }
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"basic update - without where and out-of-order columns - Partition=$isPartitioned") {
      val partitions = if (isPartitioned) "key" :: Nil else Nil
      append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"), partitions)

      checkUpdate(condition = None, setClauses = "value = 3, key = 1",
        expectedResults = Row(1, 3) :: Row(1, 3) :: Row(1, 3) :: Row(1, 3) :: Nil)
    }
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"basic update - without where and complex input - Partition=$isPartitioned") {
      val partitions = if (isPartitioned) "key" :: Nil else Nil
      append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"), partitions)

      checkUpdate(condition = None, setClauses = "value = key + 3, key = key + 1",
        expectedResults = Row(1, 3) :: Row(2, 4) :: Row(2, 4) :: Row(3, 5) :: Nil)
    }
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"basic update - with where - Partition=$isPartitioned") {
      val partitions = if (isPartitioned) "key" :: Nil else Nil
      append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"), partitions)

      checkUpdate(condition = Some("key = 1"), setClauses = "value = 3, key = 1",
        expectedResults = Row(1, 3) :: Row(2, 2) :: Row(0, 3) :: Row(1, 3) :: Nil)
    }
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"basic update - with where and complex input - Partition=$isPartitioned") {
      val partitions = if (isPartitioned) "key" :: Nil else Nil
      append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"), partitions)

      checkUpdate(condition = Some("key >= 1"), setClauses = "value = key + value, key = key + 1",
        expectedResults = Row(0, 3) :: Row(2, 5) :: Row(2, 2) :: Row(3, 4) :: Nil)
    }
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"basic update - with where and no row matched - Partition=$isPartitioned") {
      val partitions = if (isPartitioned) "key" :: Nil else Nil
      append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"), partitions)

      checkUpdate(condition = Some("key >= 10"), setClauses = "value = key + value, key = key + 1",
        expectedResults = Row(0, 3) :: Row(1, 1) :: Row(1, 4) :: Row(2, 2) :: Nil)
    }
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"type mismatch - Partition=$isPartitioned") {
      val partitions = if (isPartitioned) "key" :: Nil else Nil
      append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"), partitions)

      checkUpdate(condition = Some("key >= 1"),
        setClauses = "value = key + cast(value as double), key = cast(key as double) + 1",
        expectedResults = Row(0, 3) :: Row(2, 5) :: Row(3, 4) :: Row(2, 2) :: Nil)
    }
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"set to null - Partition=$isPartitioned") {
      val partitions = if (isPartitioned) "key" :: Nil else Nil
      append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"), partitions)

      checkUpdate(condition = Some("key >= 1"),
        setClauses = "value = key, key = null + 1D",
        expectedResults = Row(0, 3) :: Row(null, 1) :: Row(null, 1) :: Row(null, 2) :: Nil)
    }
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"basic update - TypeCoercion twice - Partition=$isPartitioned") {
      val partitions = if (isPartitioned) "key" :: Nil else Nil
      append(Seq((99, 2), (100, 4), (101, 3)).toDF("key", "value"), partitions)

      checkUpdate(
        condition = Some("cast(key as long) * cast('1.0' as decimal(38, 18)) > 100"),
        setClauses = "value = -3",
        expectedResults = Row(100, 4) :: Row(101, -3) :: Row(99, 2) :: Nil)
    }
  }

  test("update cached table") {
    Seq((2, 2), (1, 4)).toDF("key", "value")
      .write.mode("overwrite").format("delta").save(tempPath)

    spark.read.format("delta").load(tempPath).cache()
    spark.read.format("delta").load(tempPath).collect()

    executeUpdate(s"delta.`$tempPath`", set = "key = 3")
    checkAnswer(spark.read.format("delta").load(tempPath), Row(3, 2) :: Row(3, 4) :: Nil)
  }

  test("different variations of column references") {
    append(Seq((99, 2), (100, 4), (101, 3), (102, 5)).toDF("key", "value"))

    spark.read.format("delta").load(tempPath).createOrReplaceTempView("tblName")

    checkUpdate(condition = Some("key = 99"), setClauses = "value = -1",
      Row(99, -1) :: Row(100, 4) :: Row(101, 3) :: Row(102, 5) :: Nil)
    checkUpdate(condition = Some("`key` = 100"), setClauses = "`value` = -1",
      Row(99, -1) :: Row(100, -1) :: Row(101, 3) :: Row(102, 5) :: Nil)
    checkUpdate(condition = Some("tblName.key = 101"), setClauses = "tblName.value = -1",
      Row(99, -1) :: Row(100, -1) :: Row(101, -1) :: Row(102, 5) :: Nil, Some("tblName"))
    checkUpdate(condition = Some("`tblName`.`key` = 102"), setClauses = "`tblName`.`value` = -1",
      Row(99, -1) :: Row(100, -1) :: Row(101, -1) :: Row(102, -1) :: Nil, Some("tblName"))
  }

  test("target columns can have db and table qualifiers") {
    withTable("target") {
      spark.read.json("""
          {"a": {"b.1": 1, "c.e": 'random'}, "d": 1}
          {"a": {"b.1": 3, "c.e": 'string'}, "d": 2}"""
        .split("\n").toSeq.toDS()).write.format("delta").saveAsTable("`target`")

      executeUpdate(
        target = "target",
        set = "`default`.`target`.a.`b.1` = -1, target.a.`c.e` = 'RANDOM'",
        where = "d = 1")

      checkAnswer(spark.table("target"),
        spark.read.json("""
            {"a": {"b.1": -1, "c.e": 'RANDOM'}, "d": 1}
            {"a": {"b.1": 3, "c.e": 'string'}, "d": 2}"""
          .split("\n").toSeq.toDS()))
    }
  }

  test("Negative case - non-delta target") {
    Seq((1, 1), (0, 3), (1, 5)).toDF("key1", "value")
      .write.mode("overwrite").format("parquet").save(tempPath)
    val e = intercept[DeltaAnalysisException] {
      executeUpdate(target = s"delta.`$tempPath`", set = "key1 = 3")
    }.getMessage
    assert(e.contains("UPDATE destination only supports Delta sources") ||
      e.contains("is not a Delta table") || e.contains("doesn't exist") ||
      e.contains("Incompatible format"))
  }

  test("Negative case - check target columns during analysis") {
    withTable("table") {
      sql("CREATE TABLE table (s int, t string) USING delta PARTITIONED BY (s)")
      var ae = intercept[AnalysisException] {
        executeUpdate("table", set = "column_doesnt_exist = 'San Francisco'", where = "t = 'a'")
      }
      // The error class is renamed from MISSING_COLUMN to UNRESOLVED_COLUMN in Spark 3.4
      assert(ae.getErrorClass == "UNRESOLVED_COLUMN.WITH_SUGGESTION"
        || ae.getErrorClass == "MISSING_COLUMN" )

      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
        executeUpdate(target = "table", set = "S = 1, T = 'b'", where = "T = 'a'")
        ae = intercept[AnalysisException] {
          executeUpdate(target = "table", set = "S = 1, s = 'b'", where = "s = 1")
        }
        assert(ae.message.contains("There is a conflict from these SET columns"))
      }

      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        ae = intercept[AnalysisException] {
          executeUpdate(target = "table", set = "S = 1", where = "t = 'a'")
        }
        // The error class is renamed from MISSING_COLUMN to UNRESOLVED_COLUMN in Spark 3.4
        assert(ae.getErrorClass == "UNRESOLVED_COLUMN.WITH_SUGGESTION"
          || ae.getErrorClass == "MISSING_COLUMN" )

        ae = intercept[AnalysisException] {
          executeUpdate(target = "table", set = "S = 1, s = 'b'", where = "s = 1")
        }
        // The error class is renamed from MISSING_COLUMN to UNRESOLVED_COLUMN in Spark 3.4
        assert(ae.getErrorClass == "UNRESOLVED_COLUMN.WITH_SUGGESTION"
          || ae.getErrorClass == "MISSING_COLUMN" )

        // unresolved column in condition
        ae = intercept[AnalysisException] {
          executeUpdate(target = "table", set = "s = 1", where = "T = 'a'")
        }
        // The error class is renamed from MISSING_COLUMN to UNRESOLVED_COLUMN in Spark 3.4
        assert(ae.getErrorClass == "UNRESOLVED_COLUMN.WITH_SUGGESTION"
          || ae.getErrorClass == "MISSING_COLUMN" )
      }
    }
  }

  test("Negative case - UPDATE the child directory") {
    append(Seq((2, 2), (3, 2)).toDF("key", "value"), partitionBy = "key" :: Nil)
    val e = intercept[AnalysisException] {
      executeUpdate(
        target = s"delta.`$tempPath/key=2`",
        set = "key = 1, value = 2",
        where = "value = 2")
    }.getMessage
    assert(e.contains("Expect a full scan of Delta sources, but found a partial scan"))
  }

  test("Negative case - do not support subquery test") {
    append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"))
    Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("c", "d").createOrReplaceTempView("source")

    // basic subquery
    val e0 = intercept[AnalysisException] {
      executeUpdate(target = s"delta.`$tempPath`",
        set = "key = 1",
        where = "key < (SELECT max(c) FROM source)")
    }.getMessage
    assert(e0.contains("Subqueries are not supported"))

    // subquery with EXISTS
    val e1 = intercept[AnalysisException] {
      executeUpdate(target = s"delta.`$tempPath`",
        set = "key = 1",
        where = "EXISTS (SELECT max(c) FROM source)")
    }.getMessage
    assert(e1.contains("Subqueries are not supported"))

    // subquery with NOT EXISTS
    val e2 = intercept[AnalysisException] {
      executeUpdate(target = s"delta.`$tempPath`",
        set = "key = 1",
        where = "NOT EXISTS (SELECT max(c) FROM source)")
    }.getMessage
    assert(e2.contains("Subqueries are not supported"))

    // subquery with IN
    val e3 = intercept[AnalysisException] {
      executeUpdate(target = s"delta.`$tempPath`",
        set = "key = 1",
        where = "key IN (SELECT max(c) FROM source)")
    }.getMessage
    assert(e3.contains("Subqueries are not supported"))

    // subquery with NOT IN
    val e4 = intercept[AnalysisException] {
      executeUpdate(target = s"delta.`$tempPath`",
        set = "key = 1",
        where = "key NOT IN (SELECT max(c) FROM source)")
    }.getMessage
    assert(e4.contains("Subqueries are not supported"))
  }

  test("nested data support") {
    // set a nested field
    checkUpdateJson(target = """
        {"a": {"c": {"d": 'random', "e": 'str'}, "g": 1}, "z": 10}
        {"a": {"c": {"d": 'random2', "e": 'str2'}, "g": 2}, "z": 20}""",
      updateWhere = "z = 10",
      set = "a.c.d = 'RANDOM'" :: Nil,
      expected = """
        {"a": {"c": {"d": 'RANDOM', "e": 'str'}, "g": 1}, "z": 10}
        {"a": {"c": {"d": 'random2', "e": 'str2'}, "g": 2}, "z": 20}""")

    // do nothing as condition has no match
    val unchanged = """
        {"a": {"c": {"d": 'RANDOM', "e": 'str'}, "g": 1}, "z": 10}
        {"a": {"c": {"d": 'random2', "e": 'str2'}, "g": 2}, "z": 20}"""
    checkUpdateJson(target = unchanged,
      updateWhere = "z = 30",
      set = "a.c.d = 'RANDOMMMMM'" :: Nil,
      expected = unchanged)

    // set multiple nested fields at different levels
    checkUpdateJson(
      target = """
          {"a": {"c": {"d": 'RANDOM', "e": 'str'}, "g": 1}, "z": 10}
          {"a": {"c": {"d": 'random2', "e": 'str2'}, "g": 2}, "z": 20}""",
      updateWhere = "z = 20",
      set = "a.c.d = 'RANDOM2'" :: "a.c.e = 'STR2'" :: "a.g = -2" :: "z = -20" :: Nil,
      expected = """
          {"a": {"c": {"d": 'RANDOM', "e": 'str'}, "g": 1}, "z": 10}
          {"a": {"c": {"d": 'RANDOM2', "e": 'STR2'}, "g": -2}, "z": -20}""")

    // set nested fields to null
    checkUpdateJson(
      target = """
          {"a": {"c": {"d": 'RANDOM', "e": 'str'}, "g": 1}, "z": 10}
          {"a": {"c": {"d": 'random2', "e": 'str2'}, "g": 2}, "z": 20}""",
      updateWhere = "a.c.d = 'random2'",
      set = "a.c = null" :: "a.g = null" :: Nil,
      expected = """
          {"a": {"c": {"d": 'RANDOM', "e": 'str'}, "g": 1}, "z": 10}
          {"a": {"c": null, "g": null}, "z": 20}""")

    // set a top struct type column to null
    checkUpdateJson(
      target = """
          {"a": {"c": {"d": 'RANDOM', "e": 'str'}, "g": 1}, "z": 10}
          {"a": {"c": {"d": 'random2', "e": 'str2'}, "g": 2}, "z": 20}""",
      updateWhere = "a.c.d = 'random2'",
      set = "a = null" :: Nil,
      expected = """
          {"a": {"c": {"d": 'RANDOM', "e": 'str'}, "g": 1}, "z": 10}
          {"a": null, "z": 20}""")

    // set a nested field using named_struct
    checkUpdateJson(
      target = """
          {"a": {"c": {"d": 'RANDOM', "e": 'str'}, "g": 1}, "z": 10}
          {"a": {"c": {"d": 'random2', "e": 'str2'}, "g": 2}, "z": 20}""",
      updateWhere = "a.g = 2",
      set = "a.c = named_struct('d', 'RANDOM2', 'e', 'STR2')" :: Nil,
      expected = """
          {"a": {"c": {"d": 'RANDOM', "e": 'str'}, "g": 1}, "z": 10}
          {"a": {"c": {"d": 'RANDOM2', "e": 'STR2'}, "g": 2}, "z": 20}""")

    // set an integer nested field with a string that can be casted into an integer
    checkUpdateJson(
      target = """
        {"a": {"c": {"d": 'random', "e": 'str'}, "g": 1}, "z": 10}
        {"a": {"c": {"d": 'random2', "e": 'str2'}, "g": 2}, "z": 20}""",
      updateWhere = "z = 10",
      set = "a.g = '-1'" :: "z = '30'" :: Nil,
      expected = """
        {"a": {"c": {"d": 'random', "e": 'str'}, "g": -1}, "z": 30}
        {"a": {"c": {"d": 'random2', "e": 'str2'}, "g": 2}, "z": 20}""")

    // set the nested data that has an Array field
    checkUpdateJson(
      target = """
          {"a": {"c": {"d": 'random', "e": [1, 11]}, "g": 1}, "z": 10}
          {"a": {"c": {"d": 'RANDOM2', "e": [2, 22]}, "g": 2}, "z": 20}""",
      updateWhere = "z = 20",
      set = "a.c.d = 'RANDOM22'" :: "a.g = -2" :: Nil,
      expected = """
          {"a": {"c": {"d": 'random', "e": [1, 11]}, "g": 1}, "z": 10}
          {"a": {"c": {"d": 'RANDOM22', "e": [2, 22]}, "g": -2}, "z": 20}""")

    // set an array field
    checkUpdateJson(
      target = """
          {"a": {"c": {"d": 'random', "e": [1, 11]}, "g": 1}, "z": 10}
          {"a": {"c": {"d": 'RANDOM22', "e": [2, 22]}, "g": -2}, "z": 20}""",
      updateWhere = "z = 10",
      set = "a.c.e = array(-1, -11)" ::  "a.g = -1" :: Nil,
      expected = """
          {"a": {"c": {"d": 'random', "e": [-1, -11]}, "g": -1}, "z": 10}
          {"a": {"c": {"d": 'RANDOM22', "e": [2, 22]}, "g": -2}, "z": 20}""")

    // set an array field as a top-level attribute
    checkUpdateJson(
      target = """
          {"a": [1, 11], "b": 'Z'}
          {"a": [2, 22], "b": 'Y'}""",
      updateWhere = "b = 'Z'",
      set = "a = array(-1, -11, -111)" :: Nil,
      expected = """
          {"a": [-1, -11, -111], "b": 'Z'}
          {"a": [2, 22], "b": 'Y'}""")
  }

  test("nested data resolution order") {
    // By default, resolve by name.
    checkUpdateJson(
      target = """
          {"a": {"c": {"d": 'RANDOM', "e": 'str'}, "g": 1}, "z": 10}
          {"a": {"c": {"d": 'random2', "e": 'str2'}, "g": 2}, "z": 20}""",
      updateWhere = "a.g = 2",
      set = "a = named_struct('g', 20, 'c', named_struct('e', 'str0', 'd', 'randomNew'))" :: Nil,
      expected = """
          {"a": {"c": {"d": 'RANDOM', "e": 'str'}, "g": 1}, "z": 10}
          {"a": {"c": {"d": 'randomNew', "e": 'str0'}, "g": 20}, "z": 20}""")
    checkUpdateJson(
      target = """
          {"a": {"c": {"d": 'RANDOM', "e": 'str'}, "g": 1}, "z": 10}
          {"a": {"c": {"d": 'random2', "e": 'str2'}, "g": 2}, "z": 20}""",
      updateWhere = "a.g = 2",
      set = "a.c = named_struct('e', 'str0', 'd', 'randomNew')" :: Nil,
      expected = """
          {"a": {"c": {"d": 'RANDOM', "e": 'str'}, "g": 1}, "z": 10}
          {"a": {"c": {"d": 'randomNew', "e": 'str0'}, "g": 2}, "z": 20}""")

    // With the legacy conf, resolve by position.
    withSQLConf((DeltaSQLConf.DELTA_RESOLVE_MERGE_UPDATE_STRUCTS_BY_NAME.key, "false")) {
      checkUpdateJson(
        target = """
          {"a": {"c": {"d": 'RANDOM', "e": 'str'}, "g": 1}, "z": 10}
          {"a": {"c": {"d": 'random2', "e": 'str2'}, "g": 2}, "z": 20}""",
        updateWhere = "a.g = 2",
        set = "a.c = named_struct('e', 'str0', 'd', 'randomNew')" :: Nil,
        expected = """
          {"a": {"c": {"d": 'RANDOM', "e": 'str'}, "g": 1}, "z": 10}
          {"a": {"c": {"d": 'str0', "e": 'randomNew'}, "g": 2}, "z": 20}""")

      val e = intercept[AnalysisException] {
        checkUpdateJson(
          target =
            """
          {"a": {"c": {"d": 'RANDOM', "e": 'str'}, "g": 1}, "z": 10}
          {"a": {"c": {"d": 'random2', "e": 'str2'}, "g": 2}, "z": 20}""",
          updateWhere = "a.g = 2",
          set =
            "a = named_struct('g', 20, 'c', named_struct('e', 'str0', 'd', 'randomNew'))" :: Nil,
          expected =
            """
          {"a": {"c": {"d": 'RANDOM', "e": 'str'}, "g": 1}, "z": 10}
          {"a": {"c": {"d": 'randomNew', "e": 'str0'}, "g": 20}, "z": 20}""")
      }

      assert(e.getMessage.contains("cannot cast"))
    }
  }

  testQuietly("nested data - negative case") {
    val targetDF = spark.read.json("""
        {"a": {"c": {"d": 'random', "e": 'str'}, "g": 1}, "z": 10}
        {"a": {"c": {"d": 'random2', "e": 'str2'}, "g": 2}, "z": 20}"""
      .split("\n").toSeq.toDS())

    testAnalysisException(
      targetDF,
      set = "a.c = 'RANDOM2'" :: Nil,
      where = "z = 10",
      errMsgs = "data type mismatch" :: Nil)

    testAnalysisException(
      targetDF,
      set = "a.c.z = 'RANDOM2'" :: Nil,
      errMsgs = "No such struct field" :: Nil)

    testAnalysisException(
      targetDF,
      set = "a.c = named_struct('d', 'rand', 'e', 'str')" :: "a.c.d = 'RANDOM2'" :: Nil,
      errMsgs = "There is a conflict from these SET columns" :: Nil)

    testAnalysisException(
      targetDF,
      set =
        Seq("a = named_struct('c', named_struct('d', 'rand', 'e', 'str'))", "a.c.d = 'RANDOM2'"),
      errMsgs = "There is a conflict from these SET columns" :: Nil)

    val schema = new StructType().add("a", MapType(StringType, IntegerType))
    val mapData = spark.read.schema(schema).json(Seq("""{"a": {"b": 1}}""").toDS())
    testAnalysisException(
      mapData,
      set = "a.b = -1" :: Nil,
      errMsgs = "Updating nested fields is only supported for StructType" :: Nil)

    // Updating an ArrayStruct is not supported
    val arrayStructData = spark.read.json(Seq("""{"a": [{"b": 1}, {"b": 2}]}""").toDS())
    testAnalysisException(
      arrayStructData,
      set = "a.b = array(-1)" :: Nil,
      errMsgs = "Updating nested fields is only supported for StructType" :: Nil)
  }

  test("schema pruning on finding files to update") {
    append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"))

    val executedPlans = DeltaTestUtils.withPhysicalPlansCaptured(spark) {
      checkUpdate(condition = Some("key = 2"), setClauses = "key = 1, value = 3",
        expectedResults = Row(1, 3) :: Row(1, 4) :: Row(1, 1) :: Row(0, 3) :: Nil)
    }

    val scans = executedPlans.flatMap(_.collect {
      case f: FileSourceScanExec => f
    })
    // The first scan is for finding files to update. We only are matching against the key
    // so that should be the only field in the schema.
    assert(scans.head.schema == StructType(
      Seq(
        StructField("key", IntegerType)
      )
    ))
  }

  test("nested schema pruning on finding files to update") {
    append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value")
      .select(struct("key", "value").alias("nested")))

    val executedPlans = DeltaTestUtils.withPhysicalPlansCaptured(spark) {
      checkUpdate(condition = Some("nested.key = 2"),
        setClauses = "nested.key = 1, nested.value = 3",
        expectedResults = Row(1, 3) :: Row(1, 4) :: Row(1, 1) :: Row(0, 3) :: Nil,
        prefix = "nested.")
    }

    val scans = executedPlans.flatMap(_.collect {
      case f: FileSourceScanExec => f
    })

    assert(scans.head.schema == StructType.fromDDL("nested STRUCT<key: int>"))
  }

  /**
   * @param function the unsupported function.
   * @param functionType The type of the unsupported expression to be tested.
   * @param data the data in the table.
   * @param set the set action containing the unsupported expression.
   * @param where the where clause containing the unsupported expression.
   * @param expectException whether an exception is expected to be thrown
   * @param customErrorRegex customized error regex.
   */
  def testUnsupportedExpression(
      function: String,
      functionType: String,
      data: => DataFrame,
      set: String,
      where: String,
      expectException: Boolean,
      customErrorRegex: Option[String] = None) {
    test(s"$functionType functions in update - expect exception: $expectException") {
      withTable("deltaTable") {
        data.write.format("delta").saveAsTable("deltaTable")

        val expectedErrorRegex = "(?s).*(?i)unsupported.*(?i).*Invalid expressions.*"

        def checkExpression(
            setOption: Option[String] = None,
            whereOption: Option[String] = None) {
          var catchException = if (functionType.equals("Generate") && setOption.nonEmpty) {
            expectException
          } else true

          var errorRegex = if (functionType.equals("Generate") && whereOption.nonEmpty) {
            ".*Subqueries are not supported in the UPDATE.*"
          } else customErrorRegex.getOrElse(expectedErrorRegex)


          if (catchException) {
            val dataBeforeException = spark.read.format("delta").table("deltaTable").collect()
            val e = intercept[Exception] {
              executeUpdate(
                "deltaTable",
                setOption.getOrElse("b = 4"),
                whereOption.getOrElse("a = 1"))
            }
            val message = if (e.getCause != null) {
              e.getCause.getMessage
            } else e.getMessage
            assert(message.matches(errorRegex))
            checkAnswer(spark.read.format("delta").table("deltaTable"), dataBeforeException)
          } else {
            executeUpdate(
              "deltaTable",
              setOption.getOrElse("b = 4"),
              whereOption.getOrElse("a = 1"))
          }
        }

        // on set
        checkExpression(setOption = Option(set))

        // on condition
        checkExpression(whereOption = Option(where))
      }
    }
  }

  testUnsupportedExpression(
    function = "row_number",
    functionType = "Window",
    data = Seq((1, 2, 3)).toDF("a", "b", "c"),
    set = "b = row_number() over (order by c)",
    where = "row_number() over (order by c) > 1",
    expectException = true
  )

  testUnsupportedExpression(
    function = "max",
    functionType = "Aggregate",
    data = Seq((1, 2, 3)).toDF("a", "b", "c"),
    set = "b = max(c)",
    where = "b > max(c)",
    expectException = true
  )

  // Explode functions are supported in set and where if there's only one row generated.
  testUnsupportedExpression(
    function = "explode",
    functionType = "Generate",
    data = Seq((1, 2, List(3))).toDF("a", "b", "c"),
    set = "b = (select explode(c) from deltaTable)",
    where = "b = (select explode(c) from deltaTable)",
    expectException = false // only one row generated, no exception.
  )

  // Explode functions are supported in set and where but if there's more than one row generated,
  // it will throw an exception.
  testUnsupportedExpression(
    function = "explode",
    functionType = "Generate",
    data = Seq((1, 2, List(3, 4))).toDF("a", "b", "c"),
    set = "b = (select explode(c) from deltaTable)",
    where = "b = (select explode(c) from deltaTable)",
    expectException = true, // more than one generated, expect exception.
    customErrorRegex =
      Some(".*ore than one row returned by a subquery used as an expression(?s).*")
  )

  protected def checkUpdateJson(
      target: Seq[String],
      source: Seq[String] = Nil,
      updateWhere: String,
      set: Seq[String],
      expected: Seq[String]): Unit = {
    withTempDir { dir =>
      withTempView("source") {
        def toDF(jsonStrs: Seq[String]) = spark.read.json(jsonStrs.toDS)
        toDF(target).write.format("delta").mode("overwrite").save(dir.toString)
        if (source.nonEmpty) {
          toDF(source).createOrReplaceTempView("source")
        }
        executeUpdate(s"delta.`$dir`", set, updateWhere)
        checkAnswer(readDeltaTableByPath(dir.toString), toDF(expected))
      }
    }
  }

  protected def testAnalysisException(
      targetDF: DataFrame,
      set: Seq[String],
      where: String = null,
      errMsgs: Seq[String] = Nil) = {
    withTempDir { dir =>
      targetDF.write.format("delta").save(dir.toString)
      val e = intercept[AnalysisException] {
        executeUpdate(target = s"delta.`$dir`", set, where)
      }
      errMsgs.foreach { msg =>
        assert(e.getMessage.toLowerCase(Locale.ROOT).contains(msg.toLowerCase(Locale.ROOT)))
      }
    }
  }

  Seq(true, false).foreach { isPartitioned =>
    val testName = s"test update on temp view - basic - Partition=$isPartitioned"
    testWithTempView(testName) { isSQLTempView =>
      val partitions = if (isPartitioned) "key" :: Nil else Nil
      append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"), partitions)
      createTempViewFromTable(s"delta.`$tempPath`", isSQLTempView)
        checkUpdate(
          condition = Some("key >= 1"),
          setClauses = "value = key + value, key = key + 1",
          expectedResults = Row(0, 3) :: Row(2, 5) :: Row(2, 2) :: Row(3, 4) :: Nil,
          tableName = Some("v"))
    }
  }

  protected def testInvalidTempViews(name: String)(
      text: String,
      expectedErrorMsgForSQLTempView: String = null,
      expectedErrorMsgForDataSetTempView: String = null,
      expectedErrorClassForSQLTempView: String = null,
      expectedErrorClassForDataSetTempView: String = null): Unit = {
    testWithTempView(s"test update on temp view - $name") { isSQLTempView =>
      withTable("tab") {
        Seq((0, 3), (1, 2)).toDF("key", "value").write.format("delta").saveAsTable("tab")
        createTempViewFromSelect(text, isSQLTempView)
        val ex = intercept[AnalysisException] {
          executeUpdate(
            "v",
            where = "key >= 1 and value < 3",
            set = "value = key + value, key = key + 1"
          )
        }
        testErrorMessageAndClass(
          isSQLTempView,
          ex,
          expectedErrorMsgForSQLTempView,
          expectedErrorMsgForDataSetTempView,
          expectedErrorClassForSQLTempView,
          expectedErrorClassForDataSetTempView)
      }
    }
  }

  testInvalidTempViews("subset cols")(
    text = "SELECT key FROM tab",
    expectedErrorClassForSQLTempView = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
    expectedErrorClassForDataSetTempView = "UNRESOLVED_COLUMN.WITH_SUGGESTION"
  )

  testInvalidTempViews("superset cols")(
    text = "SELECT key, value, 1 FROM tab",
    // The analyzer can't tell whether the table originally had the extra column or not.
    expectedErrorMsgForSQLTempView = "Can't resolve column 1 in root",
    expectedErrorMsgForDataSetTempView = "Can't resolve column 1 in root"
  )

  protected def testComplexTempViews(name: String)(text: String, expectedResult: Seq[Row]) = {
    testWithTempView(s"test update on temp view - $name") { isSQLTempView =>
        withTable("tab") {
          Seq((0, 3), (1, 2)).toDF("key", "value").write.format("delta").saveAsTable("tab")
          createTempViewFromSelect(text, isSQLTempView)
          executeUpdate(
            "v",
            where = "key >= 1 and value < 3",
            set = "value = key + value, key = key + 1"
          )
          checkAnswer(spark.read.format("delta").table("v"), expectedResult)
        }
      }
  }

  testComplexTempViews("nontrivial projection")(
    text = "SELECT value as key, key as value FROM tab",
    expectedResult = Seq(Row(3, 0), Row(3, 3))
  )

  testComplexTempViews("view with too many internal aliases")(
    text = "SELECT * FROM (SELECT * FROM tab AS t1) AS t2",
    expectedResult = Seq(Row(0, 3), Row(2, 3))
  )

}
