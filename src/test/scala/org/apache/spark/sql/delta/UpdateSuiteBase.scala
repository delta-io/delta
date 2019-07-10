/*
 * Copyright 2019 Databricks, Inc.
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

import java.io.File

import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.hadoop.fs.Path
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.Utils

abstract class UpdateSuiteBase extends QueryTest
  with SharedSQLContext
  with BeforeAndAfterEach {
  import testImplicits._

  var tempDir: File = _

  var deltaLog: DeltaLog = _

  protected def tempPath = tempDir.getCanonicalPath

  protected def readDeltaTable(path: String): DataFrame = {
    spark.read.format("delta").load(path)
  }

  override def beforeEach() {
    super.beforeEach()
    tempDir = Utils.createTempDir()
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
      tableName: Option[String] = None): Unit = {
    executeUpdate(tableName.getOrElse(s"delta.`$tempPath`"), setClauses, where = condition.orNull)
    checkAnswer(readDeltaTable(tempPath), expectedResults)
  }

  test("basic case") {
    append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"))
    checkUpdate(condition = None, setClauses = "key = 1, value = 2",
      expectedResults = Row(1, 2) :: Row(1, 2) :: Row(1, 2) :: Row(1, 2) :: Nil)
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"basic update - delta table - Partition=$isPartitioned") {
      withTable("deltaTable") {
        val partitions = if (isPartitioned) "key" :: Nil else Nil
        append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"), partitions)

        checkUpdate(
          condition = Some("key >= 1"),
          setClauses = "value = key + value, key = key + 1",
          expectedResults = Row(0, 3) :: Row(2, 5) :: Row(2, 2) :: Row(3, 4) :: Nil)
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
        setClauses = "value = key + cast(value as String), key = key + '1'",
        expectedResults = Row(0, 3) :: Row(2, 5) :: Row(3, 4) :: Row(2, 2) :: Nil)
    }
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"set to null - Partition=$isPartitioned") {
      val partitions = if (isPartitioned) "key" :: Nil else Nil
      append(Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value"), partitions)

      checkUpdate(condition = Some("key >= 1"),
        setClauses = "value = key, key = null + '1'",
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

  test("Negative case - non-delta target") {
    Seq((1, 1), (0, 3), (1, 5)).toDF("key1", "value")
      .write.mode("overwrite").format("parquet").save(tempPath)
    val e = intercept[AnalysisException] {
      executeUpdate(target = s"delta.`$tempPath`", set = "key1 = 3")
    }.getMessage
    assert(e.contains("UPDATE destination only supports Delta sources") ||
      e.contains("is not a Delta table") || e.contains("Incompatible format"))
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

  test("do not support subquery test") {
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
}
