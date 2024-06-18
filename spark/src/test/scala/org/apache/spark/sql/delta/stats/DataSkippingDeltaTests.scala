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

package org.apache.spark.sql.delta.stats

import java.io.File

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.metering.ScanReport
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.test.ScanReportHelper
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.scalatest.GivenWhenThen

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, PredicateHelper}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

trait DataSkippingDeltaTestsBase extends DeltaExcludedBySparkVersionTestMixinShims
    with SharedSparkSession
    with DeltaSQLCommandTest
    with PredicateHelper
    with GivenWhenThen
    with ScanReportHelper {

  val defaultNumIndexedCols = DeltaConfigs.DATA_SKIPPING_NUM_INDEXED_COLS.fromString(
    DeltaConfigs.DATA_SKIPPING_NUM_INDEXED_COLS.defaultValue)

  import testImplicits._

  protected def checkpointAndCreateNewLogIfNecessary(log: DeltaLog): DeltaLog = log

  protected val tableSchemaOnlyTag = org.scalatest.Tag("StatsCollectionWithTableSchemaOnly")

  /**
   * Test stats collection using both the table schema and DataFrame schema (if applicable)
   * TODO(lin): remove this after we remove the DELTA_COLLECT_STATS_USING_TABLE_SCHEMA flag
   */
  protected override def test(testName: String, testTags: org.scalatest.Tag*)
                             (testFun: => Any)
                             (implicit pos: org.scalactic.source.Position): Unit = {
    super.test(testName, testTags : _*)(testFun)(pos)
    if (!testTags.contains(tableSchemaOnlyTag)) {
      super.test(testName + " - old behavior with DataFrame schema", testTags: _*) {
        withSQLConf(DeltaSQLConf.DELTA_COLLECT_STATS_USING_TABLE_SCHEMA.key -> "false") {
          testFun
        }
      }
    }
  }

  testSkipping(
    "top level, single 1",
    """{"a": 1}""",
    hits = Seq(
      "True", // trivial base case
      "a = 1",
      "a <=> 1",
      "a >= 1",
      "a <= 1",
      "a <= 2",
      "a >= 0",
      "1 = a",
      "1 <=> a",
      "1 <= a",
      "1 >= a",
      "2 >= a",
      "0 <= a",
      "NOT a <=> 2"
    ),
    misses = Seq(
      "NOT a = 1",
      "NOT a <=> 1",
      "a = 2",
      "a <=> 2",
      "a != 1",
      "2 = a",
      "2 <=> a",
      "1 != a",
      "a > 1",
      "a < 1",
      "a >= 2",
      "a <= 0",
      "1 < a",
      "1 > a",
      "2 <= a",
      "0 >= a"
    )
  )

  testSkipping(
    "nested, single 1",
    """{"a": {"b": 1}}""",
    hits = Seq(
      "a.b = 1",
      "a.b >= 1",
      "a.b <= 1",
      "a.b <= 2",
      "a.b >= 0"
    ),
    misses = Seq(
      "a.b = 2",
      "a.b > 1",
      "a.b < 1"
    )
  )

  testSkipping(
    "double nested, single 1",
    """{"a": {"b": {"c": 1}}}""",
    hits = Seq(
      "a.b.c = 1",
      "a.b.c >= 1",
      "a.b.c <= 1",
      "a.b.c <= 2",
      "a.b.c >= 0"
    ),
    misses = Seq(
      "a.b.c = 2",
      "a.b.c > 1",
      "a.b.c < 1"
    )
  )

  private def longString(str: String) = str * 1000

  testSkipping(
    "long strings - long min",
    s"""
       {"a": '${longString("A")}'}
       {"a": 'B'}
       {"a": 'C'}
     """,
    hits = Seq(
      "a like 'A%'",
      s"a = '${longString("A")}'",
      "a > 'BA'",
      "a < 'AB'"
    ),
    misses = Seq(
      "a < 'AA'",
      "a > 'CD'"
    )
  )

  testSkipping(
    "long strings - long max",
    s"""
       {"a": 'A'}
       {"a": 'B'}
       {"a": '${longString("C")}'}
     """,
    hits = Seq(
      "a like 'A%'",
      "a like 'C%'",
      s"a = '${longString("C")}'",
      "a > 'BA'",
      "a < 'AB'",
      "a > 'CC'"
    ),
    misses = Seq(
      "a >= 'D'",
      "a > 'CD'"
    )
  )

  testSkipping(
    "starts with",
    """
      {"a": 'apple'}
      {"a": 'microsoft'}
    """,
    hits = Seq(
      "a like 'a%'",
      "a like 'ap%'",
      "a like 'm%'",
      "a like 'mic%'",
      "a like '%'"
    ),
    misses = Seq(
      "a like 'xyz%'"
    )
  )

  testSkipping(
    "starts with, nested",
    """
      {"a":{"b": 'apple'}}
      {"a":{"b": 'microsoft'}}
    """,
    hits = Seq(
      "a.b like 'a%'",
      "a.b like 'ap%'",
      "a.b like 'm%'",
      "a.b like 'mic%'",
      "a.b like '%'"
    ),
    misses = Seq(
      "a.b like 'xyz%'"
    )
  )

  testSkipping(
    "and statements - simple",
    """
      {"a": 1}
      {"a": 2}
    """,
    hits = Seq(
      "a > 0 AND a < 3",
      "a <= 1 AND a > -1"
    ),
    misses = Seq(
      "a < 0 AND a > -2"
    )
  )

  testSkipping(
    "and statements - two fields",
    """
      {"a": 1, "b": "2017-09-01"}
      {"a": 2, "b": "2017-08-31"}
    """,
    hits = Seq(
      "a > 0 AND b = '2017-09-01'",
      "a = 2 AND b >= '2017-08-30'",
      "a >= 2 AND b like '2017-08-%'"
    ),
    misses = Seq(
      "a > 0 AND b like '2016-%'"
    )
  )

  // One side of AND by itself still has pruning power.
  testSkipping(
    "and statements - one side unsupported",
    """
      {"a": 10, "b": 10}
      {"a": 20: "b": 20}
    """,
    hits = Seq(
      "a % 100 < 10 AND b % 100 > 20"
    ),
    misses = Seq(
      "a < 10 AND b % 100 > 20",
      "a % 100 < 10 AND b > 20"
    )
  )

  testSkipping(
    "or statements - simple",
    """
      {"a": 1}
      {"a": 2}
    """,
    hits = Seq(
      "a > 0 or a < -3",
      "a >= 2 or a < -1"
    ),
    misses = Seq(
      "a > 5 or a < -2"
    )
  )

  testSkipping(
    "or statements - two fields",
    """
      {"a": 1, "b": "2017-09-01"}
      {"a": 2, "b": "2017-08-31"}
    """,
    hits = Seq(
      "a < 0 or b = '2017-09-01'",
      "a = 2 or b < '2017-08-30'",
      "a < 2 or b like '2017-08-%'",
      "a >= 2 or b like '2016-08-%'"
    ),
    misses = Seq(
      "a < 0 or b like '2016-%'"
    )
  )

  // One side of OR by itself isn't powerful enough to prune any files.
  testSkipping(
    "or statements - one side unsupported",
    """
      {"a": 10, "b": 10}
      {"a": 20: "b": 20}
    """,
    hits = Seq(
      "a % 100 < 10 OR b > 20",
      "a < 10 OR b % 100 > 20"
    ),
    misses = Seq(
      "a < 10 OR b > 20"
    )
  )

  testSkipping(
    "not statements - simple",
    """
      {"a": 1}
      {"a": 2}
    """,
    hits = Seq(
      "not a < 0"
    ),
    misses = Seq(
      "not a > 0"
    )
  )

  // NOT(AND(a, b)) === OR(NOT(a), NOT(b)) ==> One side by itself cannot prune.
  testSkipping(
    "not statements - and",
    """
      {"a": 10, "b": 10}
      {"a": 20: "b": 20}
    """,
    hits = Seq(
      "NOT(a % 100 >= 10 AND b % 100 <= 20)",
      "NOT(a >= 10 AND b % 100 <= 20)",
      "NOT(a % 100 >= 10 AND b <= 20)"
    ),
    misses = Seq(
      "NOT(a >= 10 AND b <= 20)"
    )
  )

  // NOT(OR(a, b)) === AND(NOT(a), NOT(b)) => One side by itself is enough to prune.
  testSkipping(
    "not statements - or",
    """
      {"a": 1, "b": 10}
      {"a": 2, "b": 20}
    """,
    hits = Seq(
      "NOT(a < 1 OR b > 20)",
      "NOT(a % 100 >= 1 OR b % 100 <= 20)"
    ),
    misses = Seq(
      "NOT(a >= 1 OR b <= 20)",
      "NOT(a % 100 >= 1 OR b <= 20)",
      "NOT(a >= 1 OR b % 100 <= 20)"
    )
  )

  // If a column does not have stats, it does not participate in data skipping, which disqualifies
  // that leg of whatever conjunct it was part of.
  testSkipping(
    "missing stats columns",
    """
      {"a": 1, "b": 10}
      {"a": 2, "b": 20}
    """,
    hits = Seq(
      "b < 10",  // disqualified
      "a < 1 OR b < 10",  // a disqualified by b (same conjunct)
      "a < 1 OR (a >= 1 AND b < 10)"  // ==> a < 1 OR a >=1 ==> TRUE
    ),
    misses = Seq(
      "a < 1 AND b < 10",  // ==> a < 1 ==> FALSE
      "a < 1 OR (a > 10 AND b < 10)"  // ==> a < 1 OR a > 10 ==> FALSE
    ),
    indexedCols = 1
  )

  private def generateJsonData(numCols: Int): String = {
    val fields = (0 until numCols).map(i => s""""col${"%02d".format(i)}":$i""".stripMargin)

    "{" + fields.mkString(",") + "}"
  }

  testSkipping(
    "more columns than indexed",
    generateJsonData(defaultNumIndexedCols + 1),
    hits = Seq(
      "col00 = 0",
      s"col$defaultNumIndexedCols = $defaultNumIndexedCols",
      s"col$defaultNumIndexedCols = -1"
    ),
    misses = Seq(
      "col00 = 1"
    )
  )

  testSkipping(
    "nested schema - # indexed column = 3",
    """{
      "a": 1,
      "b": {
        "c": {
          "d": 2,
          "e": 3,
          "f": {
            "g": 4,
            "h": 5,
            "i": 6
          },
          "j": 7,
          "k": 8
        },
        "l": 9
      },
      "m": 10
    }""".replace("\n", ""),
    hits = Seq(
      "a = 1",
      "b.c.d = 2",
      "b.c.e = 3",
      // below matches due to missing stats
      "b.c.f.g < 0",
      "b.c.f.i < 0",
      "b.l < 0"),
    misses = Seq(
      "a < 0",
      "b.c.d < 0",
      "b.c.e < 0"),
    indexedCols = 3
  )

  testSkipping(
    "nested schema - # indexed column = 6",
    """{
      "a": 1,
      "b": {
        "c": {
          "d": 2,
          "e": 3,
          "f": {
            "g": 4,
            "h": 5,
            "i": 6
          },
          "j": 7,
          "k": 8
        },
        "l": 9
      },
      "m": 10
    }""".replace("\n", ""),
    hits = Seq(
      "b.c.f.i = 6",
      // below matches are due to missing stats
      "b.c.j < 0",
      "b.c.k < 0",
      "b.l < 0"),
    misses = Seq(
      "a < 0",
      "b.c.f.i < 0"
    ),
    indexedCols = 6
  )

  testSkipping(
    "nested schema - # indexed column = 9",
    """{
      "a": 1,
      "b": {
        "c": {
          "d": 2,
          "e": 3,
          "f": {
            "g": 4,
            "h": 5,
            "i": 6
          },
          "j": 7,
          "k": 8
        },
        "l": 9
      },
      "m": 10
    }""".replace("\n", ""),
    hits = Seq(
      "b.c.d = 2",
      "b.c.f.i = 6",
      "b.l = 9",
      // below matches are due to missing stats
      "m < 0"),
    misses = Seq(
      "b.l < 0",
      "b.c.f.i < 0"
    ),
    indexedCols = 9
  )

  testSkipping(
    "nested schema - # indexed column = 0",
    """{
      "a": 1,
      "b": {
        "c": {
          "d": 2,
          "e": 3,
          "f": {
            "g": 4,
            "h": 5,
            "i": 6
          },
          "j": 7,
          "k": 8
        },
        "l": 9
      },
      "m": 10
    }""".replace("\n", ""),
    hits = Seq(
      // all included due to missing stats
      "a < 0",
      "b.c.d < 0",
      "b.c.f.i < 0",
      "b.l < 0",
      "m < 0"),
    misses = Seq(),
    indexedCols = 0
  )

  testSkipping(
    "indexed column names - empty list disables stats collection",
    """{
      "a": 1,
      "b": 2,
      "c": 3,
      "d": 4
    }""".replace("\n", ""),
    hits = Seq(
      "a < 0",
      "b < 0",
      "c < 0",
      "d < 0"
    ),
    misses = Seq(),
    indexedCols = 3,
    deltaStatsColNamesOpt = Some(" ")
  )

  testSkipping(
    "indexed column names - naming a nested column indexes all leaf fields of that column",
    """{
      "a": 1,
      "b": {
        "c": {
          "d": 2,
          "e": 3,
          "f": {
            "g": 4,
            "h": 5,
            "i": 6
          },
          "j": 7,
          "k": 8
        },
        "l": 9
      },
      "m": 10
    }""".replace("\n", ""),
    hits = Seq(
      // these all have missing stats
      "a < 0",
      "b.l < 0",
      "m < 0"
    ),
    misses = Seq(
      "b.c.d < 0",
      "b.c.e < 0",
      "b.c.f.g < 0",
      "b.c.f.h < 0",
      "b.c.f.i < 0",
      "b.c.j < 0",
      "b.c.k < 0"
    ),
    indexedCols = 3,
    deltaStatsColNamesOpt = Some("b.c")
  )

  testSkipping(
    "indexed column names - index only a subset of leaf columns",
    """{
      "a": 1,
      "b": {
        "c": {
          "d": 2,
          "e": 3,
          "f": {
            "g": 4,
            "h": 5,
            "i": 6
          },
          "j": 7,
          "k": 8
        },
        "l": 9
      },
      "m": 10
    }""".replace("\n", ""),
    hits = Seq(
      // these all have missing stats
      "a < 0",
      "b.c.d < 0",
      "b.c.f.g < 0",
      "b.c.f.i < 0",
      "b.c.j < 0",
      "m < 0"
    ),
    misses = Seq(
      "b.c.e < 0",
      "b.c.f.h < 0",
      "b.c.k < 0",
      "b.l < 0"
    ),
    indexedCols = 3,
    deltaStatsColNamesOpt = Some("b.c.e, b.c.f.h, b.c.k, b.l")
  )

  testSkipping(
    "indexed column names - backtick escapes work as expected",
    """{
      "a": 1,
      "b.c": 2,
      "b": {
        "c": 3,
        "d": 4
      }
    }""".replace("\n", ""),
    hits = Seq(
      "b.c < 0"
    ),
    misses = Seq(
      "a < 0",
      "`b.c` < 0",
      "b.d < 0"
    ),
    indexedCols = 3,
    deltaStatsColNamesOpt = Some("`a`, `b.c`, `b`.`d`")
  )

  testSkipping(
    "boolean comparisons",
    """{"a": false}""",
    hits = Seq(
      "!a",
      "NOT a",
      "a", // there is no skipping for BooleanValues
      "a = false",
      "NOT a = false",
      "a > true",
      "a <= false",
      "true = a",
      "true < a",
      "false = a or a"
    ),
    misses = Seq()
  )

  // Data skipping by stats should still work even when the only data in file is null, in spite of
  // the NULL min/max stats that result -- this is different to having no stats at all.
  testSkipping(
    "nulls - only null in file",
    """
      {"a": null }
    """,
    schema = new StructType().add(new StructField("a", IntegerType)),
    hits = Seq(
      "a IS NULL",
      "a = NULL",  // Ideally this should not hit as it is always FALSE, but its correct to not skip
      "NOT a = NULL", // Same as previous case
      "a <=> NULL", // This is optimized to `IsNull(a)` by NullPropagation
      "TRUE",
      "FALSE",     // Ideally this should not hit, but its correct to not skip
      "NULL AND a = 1", // This is optimized to FALSE by ReplaceNullWithFalse, so it's same as above
      "NOT a <=> 1"
    ),
    misses = Seq(
      // stats tell us a is always NULL, so any predicate that requires non-NULL a should skip
      "a IS NOT NULL",
      "NOT a <=> NULL", // This is optimized to `IsNotNull(a)`
      "a = 1",
      "NOT a = 1",
      "a > 1",
      "a < 1",
      "a <> 1",
      "a <=> 1"
    )
  )

  testSkipping(
    "nulls - null + not-null in same file",
    """
      {"a": null }
      {"a": 1 }
    """,
    schema = new StructType().add(new StructField("a", IntegerType)),
    hits = Seq(
      "a IS NULL",
      "a IS NOT NULL",
      "a = NULL", // Ideally this should not hit as it is always FALSE, but its correct to not skip
      "NOT a = NULL", // Same as previous case
      "a <=> NULL", // This is optimized to `IsNull(a)` by NullPropagation
      "NOT a <=> NULL", // This is optimized to `IsNotNull(a)`
      "a = 1",
      "a <=> 1",
      "TRUE",
      "FALSE",    // Ideally this should not hit, but its correct to not skip
      "NULL AND a = 1", // This is optimized to FALSE by ReplaceNullWithFalse, so it's same as above
      "NOT a <=> 1"
    ),
    misses = Seq(
      "a <> 1",
      "a > 1",
      "a < 1",
      "NOT a = 1"
    )
  )

  test("data skipping with missing stats") {
    val tempDir = Utils.createTempDir()
    Seq(1, 2, 3).toDF().write.format("delta").save(tempDir.toString)
    val log = DeltaLog.forTable(spark, new Path(tempDir.toString))
    val txn = log.startTransaction()
    val noStats = txn.filterFiles(Nil).map(_.copy(stats = null))
    txn.commit(noStats, DeltaOperations.ComputeStats(Nil))

    val df = spark.read.format("delta").load(tempDir.toString)
    checkAnswer(df.where("value > 0"), Seq(Row(1), Row(2), Row(3)))
  }

  test("data skipping stats before and after optimize") {
      val tempDir = Utils.createTempDir()
      var r = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))

      val (numTuples, numFiles) = (10, 2)
      val data = spark.range(0, numTuples, 1, 2).repartition(numFiles)
      data.write.format("delta").save(r.dataPath.toString)
      r = checkpointAndCreateNewLogIfNecessary(r)
      def rStats: DataFrame =
        getStatsDf(r, $"numRecords", $"minValues.id".as("id_min"), $"maxValues.id".as("id_max"))

      checkAnswer(rStats, Seq(Row(4, 0, 8), Row(6, 1, 9)))
      val optimizeDf = sql(s"OPTIMIZE '$tempDir'")
      checkAnswer(rStats, Seq(Row(10, 0, 9)))
  }

  test("number of indexed columns") {
    val numTotalCols = defaultNumIndexedCols + 5
    val path = Utils.createTempDir().getCanonicalPath
    var r = DeltaLog.forTable(spark, new Path(path))
    val data = spark.range(10).select(Seq.tabulate(numTotalCols)(i => lit(i) as s"col$i"): _*)
    data.coalesce(1).write.format("delta").save(r.dataPath.toString)

    def checkNumIndexedCol(numIndexedCols: Int): Unit = {
      if (defaultNumIndexedCols != numTotalCols) {
        setNumIndexedColumns(r.dataPath.toString, numIndexedCols)
      }
      data.coalesce(1).write.format("delta").mode("overwrite").save(r.dataPath.toString)
      r = checkpointAndCreateNewLogIfNecessary(r)

      if (numIndexedCols == 0) {
        intercept[AnalysisException] {
          getStatsDf(r, $"numRecords", $"minValues.col0").first()
        }
      } else if (numIndexedCols < numTotalCols) {
        checkAnswer(
          getStatsDf(r, $"numRecords", $"minValues.col${numIndexedCols - 1}"),
          Seq(Row(10, numIndexedCols - 1)))
        intercept[AnalysisException] {
          getStatsDf(r, $"minValues.col$numIndexedCols").first()
        }
      } else {
        checkAnswer(
          getStatsDf(r, $"numRecords", $"minValues.col${numTotalCols - 1}"),
          Seq(Row(10, numTotalCols - 1)))
        intercept[AnalysisException] {
          getStatsDf(r, $"minValues.col$numTotalCols").first()
        }
      }
    }

    checkNumIndexedCol(defaultNumIndexedCols)
    checkNumIndexedCol(numTotalCols - 1)
    checkNumIndexedCol(numTotalCols)
    checkNumIndexedCol(numTotalCols + 1)
    checkNumIndexedCol(0)
  }

  test("remove redundant stats column references in data skipping expression") {
    withTable("table") {
      val colNames = (0 to 100).map(i => s"col_$i")
      sql(s"""CREATE TABLE `table` (${colNames.map(x => x + " INT").mkString(", ")}) using delta""")
      val conditions = colNames.map(i => s"$i != 1")
      val whereClause = conditions.mkString("WHERE ", " AND ", "")

      // This query reproduces the issue raised by running TPC-DS q41. Basically the breaking
      // condition is when the query involves a big boolean expression. As data skipping
      // generates many redundant null checks on the non-leaf stats columns, e.g., stats
      // and stats.minValues, the query complexity is amplified in the data skipping expression.
      // This fix was to simply apply a distinct() on stats column references before generating
      // the data skipping expression.
      sql(s"select col_0 from table $whereClause").collect
    }
  }

  test("data skipping shouldn't use expressions involving a subquery ") {
    withTable("t1", "t2") {
      sql(s"CREATE TABLE t1(i int, p string) USING delta partitioned by (i)")
      sql("INSERT INTO t1 SELECT 1, 'a1'")
      sql("INSERT INTO t1 SELECT 2, 'a2'")
      sql("INSERT INTO t1 SELECT 3, 'a3'")
      sql("INSERT INTO t1 SELECT 4, 'a4'")

      sql("CREATE TABLE t2(j int, q string) USING delta")
      sql("INSERT INTO t2 SELECT 1, 'b1'")
      sql("INSERT INTO t2 SELECT 2, 'b2'")

      // This query would fail before the fix, i.e., when skipping considers subquery filters.
      checkAnswer(sql("SELECT i FROM t1 join t2 on i + 2 = j + 1 where q = 'b2'"), Row(1))

      // Partition filter with subquery should be ignored for skipping
      val r1 = getScanReport { checkAnswer(
        sql("SELECT p from t1 where i in (select j from t2 where q = 'b1')"),
        Seq(Row("a1")))
      }
      assert(isFullScan(r1(0)))


      // Partition filter with subquery should be ignored for skipping
      val r3 = getScanReport { checkAnswer(
        sql("SELECT p from t1 where i in (select j from t2 where q = 'b1') and p = 'a2'"), Nil)
      }
      assert(r3(0).size("scanned").rows === Some(1))
    }
  }

  test("support case insensitivity for partitioning filters") {
    withTable("table") {
      sql(s"CREATE TABLE table(Year int, P string, Y int) USING delta partitioned by (Year)")
      sql("INSERT INTO table SELECT 1999, 'a1', 1990")
      sql("INSERT INTO table SELECT 1989, 'a2', 1990")

      val Seq(r1) = getScanReport {
        checkAnswer(sql("SELECT * from table where year > 1990"), Row(1999, "a1", 1990))
      }
      assert(!isFullScan(r1))

      val Seq(r2) = getScanReport {
        checkAnswer(
          sql("SELECT * from table where year > 1990 and p = 'a1'"), Row(1999, "a1", 1990))
      }
      assert(!isFullScan(r2))

      val Seq(r3) = getScanReport {
        checkAnswer(sql("SELECT * from table where p = 'a1'"), Row(1999, "a1", 1990))
      }
      assert(!isFullScan(r3))


      checkAnswer(sql("SELECT * from table where year < y"), Row(1989, "a2", 1990))

      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        intercept[AnalysisException] {
          sql("SELECT * from table where year > 1990")
        }
      }
    }
  }

  test("Test file pruning metrics with data skipping") {
    withTempDir { tempDir =>
      withTempView("t1", "t2") {
        val data = spark.range(10).toDF("col1")
          .withColumn("col2", 'col1./(3).cast(DataTypes.IntegerType))
        data.write.format("delta").partitionBy("col1")
          .save(tempDir.getCanonicalPath)
        spark.read.format("delta").load(tempDir.getAbsolutePath).createTempView("t1")
        val deltaLog = DeltaLog.forTable(spark, tempDir.toString())

        val query = "SELECT * from t1 where col1 > 5"
        val Seq(r1) = getScanReport {
          assert(sql(query).collect().length == 4)
        }
        val inputFiles = spark.sql(query).inputFiles
        assert(deltaLog.snapshot.numOfFiles - inputFiles.length == 6)

        val allQuery = "SELECT * from t1"
        val Seq(r2) = getScanReport {
          assert(sql(allQuery).collect().length == 10)
        }
      }
    }
  }

  test("loading data from Delta to parquet should skip data") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(5).write.format("delta").save(path)
      spark.range(5, 10).write.format("delta").mode("append").save(path)

      withTempDir { dir2 =>
        val path2 = dir2.getCanonicalPath
        val scans = getScanReport {
          spark.read.format("delta").load(path).where("id < 2")
            .write.format("parquet").mode("overwrite").save(path2)
        }
        assert(scans.size == 1)
        assert(
          scans.head.size("scanned").bytesCompressed != scans.head.size("total").bytesCompressed)
      }
    }
  }

  test("data skipping with a different DataFrame schema order", tableSchemaOnlyTag) {
    withTable("table") {
      sql("CREATE TABLE table (col1 Int, col2 Int, col3 Int) USING delta")
      val r = DeltaLog.forTable(spark, new TableIdentifier("table"))
      // Only index the first two columns
      setNumIndexedColumns(r.dataPath.toString, 2)
      val dataSeq = Seq((1, 2, 3))
      // We should use the table schema to create stats and the DataFrame schema should be ignored
      dataSeq.toDF("col1", "col2", "col3")
        .select("col2", "col3", "col1") // DataFrame schema order
        .write.mode("append").format("delta")
        .save(r.dataPath.toString)

      var hits = Seq(
        "col3 = 10",
        "col1 = 1",
        "col2 = 2",
        "col3 = 3"
      )
      var misses = Seq(
        "col1 = 5",
        "col1 = 5 AND col2 = 10",
        "col1 = 5 and col3 = 10",
        "col2 = 10",
        "col2 = 5 and col3 = 10",
        "col1 = 5 and col2 = 10 and col3 = 10"
      )

      checkSkipping(r, hits, misses, dataSeq.toString(), false)

      // Change the statsSchema to 3 columns. But there are only two columns in the stats from
      // the file
      setNumIndexedColumns(r.dataPath.toString, 3)
      hits = Seq(
        "col3 = 3",  // 3 is in col3, but no stats
        "col3 = 10",  // No stats on col3
        // The data skipping filters will be generated but verifyStatsForFilter will invalidate
        // the entire predicate
        "col1 = 5 and col3 = 10"
      )
      misses = Seq(
        "col1 = 5",
        "col1 = 5 AND col2 = 10"
      )
      checkSkipping(r, hits, misses, dataSeq.toString(), false)
    }
  }

  test("data skipping with a different DataFrame schema and column name case", tableSchemaOnlyTag) {
    withTable("table") {
      sql("CREATE TABLE table (col1 Int, col2 Int, col3 Int) USING delta")
      val r = DeltaLog.forTable(spark, new TableIdentifier("table"))
      // Only index the first two columns
      setNumIndexedColumns(r.dataPath.toString, 2)
      val dataSeq = Seq((1, 2, 3))
      // We should use the table schema to create stats and the DataFrame schema should be ignored
      dataSeq.toDF("col1", "col2", "col3")
        .select("COL2", "Col3", "coL1") // DataFrame schema order
        .write.mode("append").format("delta")
        .save(r.dataPath.toString)

      val hits = Seq(
        "col3 = 10",  // No stats for col3
        // These values should be in the columns
        "col1 = 1",
        "col2 = 2",
        "col3 = 3"
      )
      val misses = Seq(
        "col1 = 5",
        "col1 = 5 AND col2 = 10",
        "col1 = 5 and col3 = 10",
        "col2 = 10",
        "col2 = 5 and col3 = 10",
        "col1 = 5 and col2 = 10 and col3 = 10"
      )

      checkSkipping(r, hits, misses, dataSeq.toString(), false)
    }
  }

  test("data skipping with a different DataFrame schema order and nested columns",
    tableSchemaOnlyTag) {
    withTempDir { dir =>
      val structureData = Seq(
        Row(Row("James ", "", "Smith"), "36636", "M", 3100)
      )

      val structureDataSchema = new StructType()
        .add("name", new StructType()
          .add("firstname", StringType)
          .add("middlename", StringType)
          .add("lastname", StringType))
        .add("id", StringType)
        .add("gender", StringType)
        .add("salary", IntegerType)

      val data = spark.createDataFrame(
        spark.sparkContext.parallelize(structureData), structureDataSchema)

      data.write.partitionBy("id").format("delta").save(dir.getAbsolutePath)
      // Only index the first three columns (unnested), excluding partition column id
      val deltaLog = DeltaLog.forTable(spark, new Path(dir.getCanonicalPath))
      setNumIndexedColumns(deltaLog.dataPath.toString, 3)

      val structureDfData = Seq(
        // The same content as previous row but different DataFrame schema order
        Row(3100, "M", Row("James ", "", "Smith"), "36636")
      )
      val structureDfSchema = new StructType()
        .add("salary", IntegerType)
        .add("gender", StringType)
        .add("name", new StructType()
          .add("firstname", StringType)
          .add("middlename", StringType)
          .add("lastname", StringType))
        .add("id", StringType)

      // middlename is missing, but we collect NULL_COUNT for it
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(structureDfData), structureDfSchema)
      df.write.mode("append").format("delta").save(dir.getAbsolutePath)

      val hits = Seq(
        // Can't skip them since stats schema only has three columns now
        "gender = 'M'",
        "salary = 3100"
      )
      val misses = Seq(
        "name.firstname = 'Michael'",
        "name.middlename = 'L'",
        "name.lastname = 'Miller'",
        "id = '10000'",
        "name.firstname = 'Robert' and name.middlename = ''",
        "name.firstname = 'Robert' and salary = 3100"
      )
      checkSkipping(deltaLog, hits, misses, structureDfData.toString(), false)
    }
  }

  test("compatibility with the old behavior that collect stats based on DataFrame schema",
    tableSchemaOnlyTag) {
    withTable("table") {
      sql("CREATE TABLE table (col2 Int, col3 Int, col1 Int) USING delta")
      val r = DeltaLog.forTable(spark, new TableIdentifier("table"))
      // Only index the first two columns
      setNumIndexedColumns(r.dataPath.toString, 2)
      val dataSeq = Seq((1, 2, 3))
      // Only collect stats for col2 and col3
      dataSeq.toDF("col1", "col2", "col3")
        .select("col2", "col3", "col1") // DataFrame schema order
        .write.mode("append").format("delta")
        .save(r.dataPath.toString)

      // Change the schema to (col1, col2, col3). The final result would be the same as using the
      // old approach to collect stats based on the DataFrame schema
      sql("ALTER TABLE table ALTER COLUMN col1 FIRST")

      // Since the stats schema is (col1, col2), and we only have stats on col2 and col3, only
      // the predicate on col2 can be used for filters
      val hits = Seq(
        "col1 = 1",
        "col2 = 2",
        "col3 = 3",
        "col1 = 5",
        "col3 = 10",
        "col1 = 5 AND col2 = 10",
        "col1 = 5 and col3 = 10",
        "col1 = 5 and col2 = 10 and col3 = 10"
      )
      val misses = Seq(
        "col2 = 10",
        "col2 = 5 and col3 = 10"  // This can pass because stats also exists on col3
      )

      checkSkipping(r, hits, misses, dataSeq.toString(), false)
    }
  }

  // TODO(lin): remove this after we remove the DELTA_COLLECT_STATS_USING_TABLE_SCHEMA flag
  test("old behavior with DELTA_COLLECT_STATS_USING_TABLE_SCHEMA set to false") {
    // This force the system restore the old stats collection behavior based on the DataFrame schema
    withSQLConf(DeltaSQLConf.DELTA_COLLECT_STATS_USING_TABLE_SCHEMA.key -> "false") {
      withTable("table") {
        sql("CREATE TABLE table (col1 Int, col2 Int, col3 Int) USING delta")
        val r = DeltaLog.forTable(spark, new TableIdentifier("table"))
        // Only index the first two columns
        setNumIndexedColumns(r.dataPath.toString, 2)
        val dataSeq = Seq((1, 2, 3))
        // Only collect stats for col2 and col3
        dataSeq.toDF("col1", "col2", "col3")
          .select("col2", "col3", "col1") // DataFrame schema order
          .write.mode("append").format("delta")
          .save(r.dataPath.toString)

        // Since the stats schema is (col1, col2), and we only have stats on col2 and col3, only
        // the predicate on col2 can be used for filters
        val hits = Seq(
          "col1 = 1",
          "col2 = 2",
          "col3 = 3",
          "col1 = 5",
          "col3 = 10",
          "col1 = 5 AND col2 = 10",
          "col1 = 5 and col3 = 10",
          "col1 = 5 and col2 = 10 and col3 = 10"
        )
        val misses = Seq(
          "col2 = 10",
          "col2 = 5 and col3 = 10" // This can pass because stats also exists on col3
        )

        checkSkipping(r, hits, misses, dataSeq.toString(), false)
      }
    }
  }

  test("data skipping with missing columns in DataFrame", tableSchemaOnlyTag) {
    // case-1: dataframe schema has less columns than the dataSkippingNumIndexedCols
    withTable("table") {
      sql("CREATE TABLE table (a Int, b Int, c Int, d Int, e Int) USING delta PARTITIONED BY(b)")
      val r = DeltaLog.forTable(spark, new TableIdentifier("table"))
      // Only index the first three columns, excluding partition column b
      setNumIndexedColumns(r.dataPath.toString, 3)
      val dataSeq = Seq((1, 2, 3, 4, 5))

      dataSeq.toDF("a", "b", "c", "d", "e")
        .select("a", "b") // DataFrame schema order
        .write.mode("append").format("delta")
        .save(r.dataPath.toString)

      val hits = Seq(
        // These values are in the table
        "a = 1",
        "b = 2",
        "c <=> null",
        "d is null",
        // No stats for e
        "e = 10"
      )
      val misses = Seq(
        "a = 10",
        "b = 10",
        "c = 10",
        "c is not null",
        "d = 10",
        "isnotnull(d)"
      )
      checkSkipping(r, hits, misses, dataSeq.toString(), false)
    }

    // case-2: dataframe schema lacks columns that are supposed to be part of the stats schema,
    // but has an additional column that should not collect stats on
    withTable("table") {
      sql("CREATE TABLE table (a Int, b Int, c Int, d Int, e Int) USING delta PARTITIONED BY(b)")
      val r = DeltaLog.forTable(spark, new TableIdentifier("table"))
      // Only index the first three columns, excluding partition column b
      setNumIndexedColumns(r.dataPath.toString, 3)
      val dataSeq = Seq((1, 2, 3, 4, 5))

      dataSeq.toDF("a", "b", "c", "d", "e")
        .select("a", "b", "d", "e") // DataFrame schema order
        .write.mode("append").format("delta")
        .save(r.dataPath.toString)

      val hits = Seq(
        "a = 1", // In table
        "isnull(c)", // In table
        "e = 20" // No stats
      )
      val misses = Seq(
        "a = 20",
        "b = 20",
        "c = 20",
        "d = 20",
        "a = 20 and c = 20",
        "a = 20 and e = 20"
      )
      checkSkipping(r, hits, misses, dataSeq.toString(), false)
    }

    // case-3: Structured data with some columns missing and some additional columns
    withTempDir { dir =>
      val structureData = Seq(
        Row(Row("James ", "", "Smith"), "36636", "M", 3100)
      )

      val structureDataSchema = new StructType()
        .add("name", new StructType()
          .add("firstname", StringType)
          .add("middlename", StringType)
          .add("lastname", StringType))
        .add("id", StringType)
        .add("gender", StringType)
        .add("salary", IntegerType)

      val data = spark.createDataFrame(
        spark.sparkContext.parallelize(structureData), structureDataSchema)

      data.write.partitionBy("id").format("delta").save(dir.getAbsolutePath)
      // Only index the first three columns (unnested), excluding partition column id
      val deltaLog = DeltaLog.forTable(spark, new Path(dir.getCanonicalPath))
      setNumIndexedColumns(deltaLog.dataPath.toString, 3)

      val structureDfData = Seq(
        Row(2000, Row("Robert ", "Johnson"), "40000")
      )
      val structureDfSchema = new StructType()
        .add("salary", IntegerType)
        .add("name", new StructType()
          .add("firstname", StringType)
          .add("lastname", StringType))
        .add("id", StringType)

      // middlename is missing, but we collect NULL_COUNT for it
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(structureDfData), structureDfSchema)
      df.write.mode("append").format("delta").save(dir.getAbsolutePath)

      val hits = Seq(
        "gender = 'M'", // No stats
        "salary = 1000" // No stats
      )
      val misses = Seq(
        "name.firstname = 'Michael'",
        "name.middlename = 'L'",
        "name.lastname = 'Miller'",
        "id = '10000'",
        "name.firstname = 'Robert' and name.middlename = 'L'"
      )
      checkSkipping(deltaLog, hits, misses, structureDfData.toString(), false)
    }

    // case-4: dataframe schema does not have any columns within the first
    // dataSkippingNumIndexedCols columns of the table schema
    withTable("table") {
      sql("CREATE TABLE table (a Int, b Int, c Int, d Int, e Int) USING delta")
      val r = DeltaLog.forTable(spark, new TableIdentifier("table"))
      // Only index the first three columns
      setNumIndexedColumns(r.dataPath.toString, 3)
      val dataSeq = Seq((1, 2, 3, 4, 5))

      dataSeq.toDF("a", "b", "c", "d", "e")
        .select("d", "e") // DataFrame schema order
        .write.mode("append").format("delta")
        .save(r.dataPath.toString)

      val hits = Seq(
        "d = 40", // No stats
        "e = 40" // No stats
      )
      // We can still collect NULL_COUNT for a, b, and c
      val misses = Seq(
        "a = 40",
        "b = 40",
        "c = 40"
      )
      checkSkipping(r, hits, misses, dataSeq.toString(), false)
    }

    // case-5: The first dataSkippingNumIndexedCols columns of the table schema has map or array
    // types, which we only collect NULL_COUNT
    withTable("table") {
      sql("CREATE TABLE table (a Int, b Map<String, Int>, c Array<Int>, d Int, e Int)" +
        " USING delta")
      val r = DeltaLog.forTable(spark, new TableIdentifier("table"))
      // Only index the first three columns
      setNumIndexedColumns(r.dataPath.toString, 3)
      val dataSeq = Seq((1, Map("key" -> 2), Seq(3, 3, 3), 4, 5))

      dataSeq.toDF("a", "b", "c", "d", "e")
        .select("b", "c", "d") // DataFrame schema order
        .write.mode("append").format("delta")
        .save(r.dataPath.toString)

      val hits = Seq(
        "d = 50", // No stats
        "e = 50", // No stats
        // No min/max stats for c. We couldn't check = for b since EqualTo does not support
        // ordering on type maP
        "c = array(50, 50)",
        // b and c should have NULL_COUNT stats, but currently they're not SkippingEligibleColumn
        // (since they're not AtomicType), we couldn't skip for them
        "isnull(b)",
        "c is null"
      )
      val misses = Seq(
        // a has NULL_COUNT stats since it's missing from DataFrame schema
        "a = 50"
      )
      checkSkipping(r, hits, misses, dataSeq.toString(), false)
    }
  }


  test("data skipping with generated column") {
    withTable("table") {
      // OSS does not support the generated column syntax in SQL so we have to use table builder
      val tableBuilder = io.delta.tables.DeltaTable.create(spark).tableName("table")
      // add regular columns
      val col1 = io.delta.tables.DeltaTable.columnBuilder(spark, "col1")
        .dataType("int")
        .build()
      val col2 = io.delta.tables.DeltaTable.columnBuilder(spark, "col2")
        .dataType("string")
        .build()
      // add generated column
      val genCol3 = io.delta.tables.DeltaTable.columnBuilder(spark, "genCol3")
        .dataType("string")
        .generatedAlwaysAs("substring(col2, 3, 2)")
        .build()

      tableBuilder
        .addColumn(col1)
        .addColumn(col2)
        .addColumn(genCol3)
        .execute()
      // Only pass in two columns, and col3 will be generated as "st"
      val tableData = Seq((1, "test string"))
      tableData.toDF("col1", "col2")
        .write.format("delta").mode("append")
        .saveAsTable("table")

      val hits = Seq(
        "genCol3 = 'st'"
      )
      val misses = Seq(
        "col1 = 10",
        "col2 = 'test'",
        "genCol3 = 'test'"
      )

      val r = DeltaLog.forTable(spark, new TableIdentifier("table"))
      checkSkipping(r, hits, misses, tableData.toString(), false)
    }
  }

  test("data skipping by partitions and data values - nulls") {
    val tableDir = Utils.createTempDir().getAbsolutePath
    val dataSeqs = Seq( // each sequence produce a single file
      Seq((null, null)),
      Seq((null, "a")),
      Seq((null, "b")),
      Seq(("a", "a"), ("a", null)),
      Seq(("b", null))
    )
    dataSeqs.foreach { seq =>
      seq.toDF("key", "value").coalesce(1)
        .write.format("delta").partitionBy("key").mode("append").save(tableDir)
    }
    val allData = dataSeqs.flatten

    def checkResults(
                      predicate: String,
                      expResults: Seq[(String, String)],
                      expNumPartitions: Int,
                      expNumFiles: Long): Unit =
      checkResultsWithPartitions(tableDir, predicate, expResults, expNumPartitions, expNumFiles)

    // Trivial base case
    checkResults(
      predicate = "True",
      expResults = allData,
      expNumPartitions = 3,
      expNumFiles = 5)

    // Conditions on partition key
    checkResults(
      predicate = "key IS NULL",
      expResults = allData.filter(_._1 == null),
      expNumPartitions = 1,
      expNumFiles = 3) // 3 files with key = null

    checkResults(
      predicate = "key IS NOT NULL",
      expResults = allData.filter(_._1 != null),
      expNumPartitions = 2,
      expNumFiles = 2) // 2 files with key = 'a', and 1 file with key = 'b'

    checkResults(
      predicate = "key <=> NULL",
      expResults = allData.filter(_._1 == null),
      expNumPartitions = 1,
      expNumFiles = 3) // 3 files with key = null

    checkResults(
      predicate = "key = 'a'",
      expResults = allData.filter(_._1 == "a"),
      expNumPartitions = 1,
      expNumFiles = 1) // 1 files with key = 'a'

    checkResults(
      predicate = "key <=> 'a'",
      expResults = allData.filter(_._1 == "a"),
      expNumPartitions = 1,
      expNumFiles = 1) // 1 files with key <=> 'a'

    checkResults(
      predicate = "key = 'b'",
      expResults = allData.filter(_._1 == "b"),
      expNumPartitions = 1,
      expNumFiles = 1) // 1 files with key = 'b'

    checkResults(
      predicate = "key <=> 'b'",
      expResults = allData.filter(_._1 == "b"),
      expNumPartitions = 1,
      expNumFiles = 1) // 1 files with key <=> 'b'

    // Conditions on partitions keys and values
    checkResults(
      predicate = "value IS NULL",
      expResults = allData.filter(_._2 == null),
      expNumPartitions = 3,
      expNumFiles = 3) // files with all non-NULL values get skipped

    checkResults(
      predicate = "value IS NOT NULL",
      expResults = allData.filter(_._2 != null),
      expNumPartitions = 2, // one of the partitions has no files left after data skipping
      expNumFiles = 3) // files with all NULL values get skipped

    checkResults(
      predicate = "value <=> NULL",
      expResults = allData.filter(_._2 == null),
      expNumPartitions = 3,
      expNumFiles = 3) // same as IS NULL case above

    checkResults(
      predicate = "value = 'a'",
      expResults = allData.filter(_._2 == "a"),
      expNumPartitions = 2, // one partition has no files left after data skipping
      expNumFiles = 2) // only two files contain "a"

    checkResults(
      predicate = "value <=> 'a'",
      expResults = allData.filter(_._2 == "a"),
      expNumPartitions = 2, // one partition has no files left after data skipping
      expNumFiles = 2) // only two files contain "a"

    checkResults(
      predicate = "value <> 'a'",
      expResults = allData.filter(x => x._2 != "a" && x._2 != null), // i.e., only (null, b)
      expNumPartitions = 1,
      expNumFiles = 1) // only one file contains 'b'

    checkResults(
      predicate = "value = 'b'",
      expResults = allData.filter(_._2 == "b"),
      expNumPartitions = 1,
      expNumFiles = 1) // same as previous case

    checkResults(
      predicate = "value <=> 'b'",
      expResults = allData.filter(_._2 == "b"),
      expNumPartitions = 1,
      expNumFiles = 1) // same as previous case

    // Conditions on both, partition keys and values
    checkResults(
      predicate = "key IS NULL AND value = 'a'",
      expResults = Seq((null, "a")),
      expNumPartitions = 1,
      expNumFiles = 1) // only one file in the partition has (*, "a")

    checkResults(
      predicate = "key IS NOT NULL AND value IS NOT NULL",
      expResults = Seq(("a", "a")),
      expNumPartitions = 1,
      expNumFiles = 1) // 1 file with (*, a)

    checkResults(
      predicate = "key <=> NULL AND value <=> NULL",
      expResults = Seq((null, null)),
      expNumPartitions = 1,
      expNumFiles = 1) // 3 files with key = null, but only 1 with val = null.

    checkResults(
      predicate = "key <=> NULL OR value <=> NULL",
      expResults = allData.filter(_ != (("a", "a"))),
      expNumPartitions = 3,
      expNumFiles = 5) // all 5 files
  }

  // Note that we cannot use testSkipping here, because the JSON parsing bug we're working around
  // prevents specifying a microsecond timestamp as input data.
  for (timestampType <- Seq("TIMESTAMP", "TIMESTAMP_NTZ")) {
    test(s"data skipping on $timestampType") {
      val data = "2019-09-09 01:02:03.456789"
      val df = Seq(data).toDF("strTs")
        .selectExpr(
          s"CAST(strTs AS $timestampType) AS ts",
          s"STRUCT(CAST(strTs AS $timestampType) AS ts) AS nested")

      val tempDir = Utils.createTempDir()
      val r = DeltaLog.forTable(spark, tempDir)
      df.coalesce(1).write.format("delta").save(r.dataPath.toString)

      // Check to ensure that the value actually in the file is always in range queries.
      val hits = Seq(
        s"""ts >= cast("2019-09-09 01:02:03.456789" AS $timestampType)""",
        s"""ts <= cast("2019-09-09 01:02:03.456789" AS $timestampType)""",
        s"""nested.ts >= cast("2019-09-09 01:02:03.456789" AS $timestampType)""",
        s"""nested.ts <= cast("2019-09-09 01:02:03.456789" AS $timestampType)""",
        s"""TS >= cast("2019-09-09 01:02:03.456789" AS $timestampType)""",
        s"""nEstED.tS >= cast("2019-09-09 01:02:03.456789" AS $timestampType)""")

      // Check the range of values that are far enough away to be data skipped. Note that the values
      // are aligned with millisecond boundaries because of the JSON serialization truncation.
      val misses = Seq(
        s"""ts >= cast("2019-09-09 01:02:03.457001" AS $timestampType)""",
        s"""ts <= cast("2019-09-04 01:02:03.455999" AS $timestampType)""",
        s"""nested.ts >= cast("2019-09-09 01:02:03.457001" AS $timestampType)""",
        s"""nested.ts <= cast("2019-09-09 01:02:03.455999" AS $timestampType)""",
        s"""TS >= cast("2019-09-09 01:02:03.457001" AS $timestampType)""",
        s"""nEstED.tS >= cast("2019-09-09 01:02:03.457001" AS $timestampType)""")

      hits.foreach { predicate =>
        Given(predicate)
        if (filesRead(r, predicate) != 1) {
          failPretty(s"Expected hit but got miss for $predicate", predicate, data)
        }
      }

      misses.foreach { predicate =>
        Given(predicate)
        if (filesRead(r, predicate) != 0) {
          failPretty(s"Expected miss but got hit for $predicate", predicate, data)
        }
      }
    }
  }

  test("Ensure that we don't reuse scans when tables are different") {
    withTempDir { dir =>
      val table1 = new File(dir, "tbl1")
      val table1Dir = table1.getCanonicalPath
      val table2 = new File(dir, "tbl2")
      val table2Dir = table2.getCanonicalPath
      spark.range(100).withColumn("part", 'id % 5).withColumn("id2", 'id)
        .write.format("delta").partitionBy("part").save(table1Dir)

      FileUtils.copyDirectory(table1, table2)

      sql(s"DELETE FROM delta.`$table2Dir` WHERE part = 0 and id < 65")

      val query = sql(s"SELECT * FROM delta.`$table1Dir` WHERE part = 0 AND id2 < 85 AND " +
        s"id NOT IN (SELECT id FROM delta.`$table2Dir` WHERE part = 0 AND id2 < 85)")

      checkAnswer(
        query,
        sql(s"SELECT * FROM delta.`$table1Dir` WHERE part = 0 and id < 65"))
    }
  }

  test("Data skipping should always return files from latest commit version") {
    withTempDir { dir =>
      // If this test is flacky it is broken
      Seq("aaa").toDF().write.format("delta").save(dir.getCanonicalPath)
      val (log, snapshot) = DeltaLog.forTableWithSnapshot(spark, dir.getPath)
      val addFile = snapshot.allFiles.collect().head
      val fileWithStat = snapshot.getSpecificFilesWithStats(Seq(addFile.path)).head
      // Ensure the stats has actual stats, not {}
      assert(fileWithStat.stats.size > 2)
      log.startTransaction().commitManually(addFile.copy(stats = "{}"))

      // Delta dedup should always keep AddFile from newer version so
      // getSpecificFilesWithStats should return the AddFile with empty stats
      log.update()
      val newfileWithStat =
        log.unsafeVolatileSnapshot.getSpecificFilesWithStats(Seq(addFile.path)).head
      assert(newfileWithStat.stats === "{}")
    }
  }

  Seq("create", "alter").foreach { label =>
    test(s"Basic: Data skipping with delta statistic column $label") {
      withTable("table") {
        val tableProperty = if (label == "create") {
          "TBLPROPERTIES('delta.dataSkippingStatsColumns' = 'c1,c2,c3,c4,c5,c6,c7,c10')"
        } else {
          ""
        }
        sql(
          s"""CREATE TABLE table(
             |c1 long, c2 STRING, c3 FLOAT, c4 DOUBLE, c5 TIMESTAMP, c6 TIMESTAMP_NTZ, c7 DATE,
             |c8 BINARY, c9 BOOLEAN, c10 DECIMAL(3, 2)
             |) USING delta $tableProperty""".stripMargin)
        if (label == "alter") {
          sql(
            s"""ALTER TABLE table
               |SET TBLPROPERTIES (
               |  'delta.dataSkippingStatsColumns' = 'c1,c2,c3,c4,c5,c6,c7,c10'
               |)""".stripMargin)
        }
        sql(
          """insert into table values
            |(1, '1', 1.0, 1.0, TIMESTAMP'2001-01-01 01:00', TIMESTAMP_NTZ'2001-01-01 01:00',
            |DATE'2001-01-01', '1111', true, 1.0),
            |(2, '2', 2.0, 2.0, TIMESTAMP'2002-02-02 02:00', TIMESTAMP_NTZ'2002-02-02 02:00',
            |DATE'2002-02-02', '2222', false, 2.0)
            |""".stripMargin).count()
        val hits = Seq(
          "c1 = 1",
          "c2 = \'2\'",
          "c3 < 1.5",
          "c4 > 1.0",
          "c5 >= \"2001-01-01 01:00:00\"",
          "c6 >= \"2001-01-01 01:00:00\"",
          "c7 = \"2002-02-02\"",
          "c8 = HEX(\"1111\")", // Binary Column doesn't support delta statistics.
          "c8 = HEX(\"3333\")", // Binary Column doesn't support delta statistics.
          "c9 = true",
          "c9 = false",
          "c10 > 1.5"
        )
        val misses = Seq(
          "c1 = 10",
          "c2 = \'4\'",
          "c3 < 0.5",
          "c4 > 5.0",
          "c5 >= \"2003-01-01 01:00:00\"",
          "c6 >= \"2003-01-01 01:00:00\"",
          "c7 = \"2003-02-02\"",
          "c10 > 2.5"
        )
        val dataSeq = Seq(
          (1L, "1", 1.0f, 1.0d, "2002-01-01 01:00", "2002-01-01 01:00", "2001-01-01", "1111",
            true, 1.0f),
          (2L, "2", 2.0f, 2.0d, "2002-02-02 02:00", "2002-02-02 02:00", "2002-02-02", "2222",
            false, 2.0f)
        )
        val r = DeltaLog.forTable(spark, new TableIdentifier("table"))
        checkSkipping(r, hits, misses, dataSeq.toString(), false)
      }
    }
  }

  testSparkMasterOnly("data skipping by stats - variant type") {
    withTable("tbl") {
      sql("""CREATE TABLE tbl(v VARIANT,
              v_struct STRUCT<v: VARIANT>,
              null_v VARIANT,
              null_v_struct STRUCT<v: VARIANT>) USING DELTA""")
      sql("""INSERT INTO tbl (SELECT
          parse_json(cast(id as string)),
          named_struct('v', parse_json(cast(id as string))),
          cast(null as variant),
          named_struct('v', cast(null as variant))
          FROM range(100))""")

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("tbl", None, None))
      val hits = Seq(
        "v IS NOT NULL",
        "v_struct.v IS NOT NULL",
        "null_v IS NULL",
        "null_v_struct.v IS NULL"
      )
      val misses = Seq(
        "v IS NULL",
        "v_struct.v IS NULL",
        "null_v IS NOT NULL",
        "null_v_struct.v IS NOT NULL"
      )
      val data = spark.sql("select * from tbl").collect().toSeq.toString
      checkSkipping(deltaLog, hits, misses, data, false)
    }
  }

  test(s"Data skipping with delta statistic column rename column") {
    withTable("table") {
      sql(
        s"""CREATE TABLE table(
           |c1 long, c2 STRING, c3 FLOAT, c4 DOUBLE, c5 TIMESTAMP, c6 TIMESTAMP_NTZ,
           |c7 DATE, c8 BINARY, c9 BOOLEAN, c10 DECIMAL(3, 2)
           |) USING delta
           |TBLPROPERTIES(
           |'delta.dataSkippingStatsColumns' = 'c1,c2,c3,c4,c5,c6,c7,c10',
           |'delta.columnMapping.mode' = 'name',
           |'delta.minReaderVersion' = '2',
           |'delta.minWriterVersion' = '5'
           |)
           |""".stripMargin)
      (1 to 10).foreach { i =>
        sql(s"alter table table RENAME COLUMN c$i to cc$i")
      }
      val newConfiguration = sql("SHOW TBLPROPERTIES table ")
        .collect()
        .map { row =>
          row.getString(0) -> row.getString(1)
        }
        .filter(_._1 == "delta.dataSkippingStatsColumns")
        .toSeq
      assert(
        newConfiguration == Seq(
          ("delta.dataSkippingStatsColumns", "cc1,cc2,cc3,cc4,cc5,cc6,cc7,cc10"))
      )
      sql(
        """insert into table values
          |(1, '1', 1.0, 1.0, TIMESTAMP'2001-01-01 01:00', TIMESTAMP_NTZ'2001-01-01 01:00',
          |DATE'2001-01-01', '1111', true, 1.0),
          |(2, '2', 2.0, 2.0, TIMESTAMP'2002-02-02 02:00', TIMESTAMP_NTZ'2002-02-02 02:00',
          |DATE'2002-02-02', '2222', false, 2.0)
          |""".stripMargin).count()
      val hits = Seq(
        "cc1 = 1",
        "cc2 = \'2\'",
        "cc3 < 1.5",
        "cc4 > 1.0",
        "cc5 >= \"2001-01-01 01:00:00\"",
        "cc6 >= \"2001-01-01 01:00:00\"",
        "cc7 = \"2002-02-02\"",
        "cc8 = HEX(\"1111\")", // Binary Column doesn't support delta statistics.
        "cc8 = HEX(\"3333\")", // Binary Column doesn't support delta statistics.
        "cc9 = true",
        "cc9 = false",
        "cc10 > 1.5"
      )
      val misses = Seq(
        "cc1 = 10",
        "cc2 = \'4\'",
        "cc3 < 0.5",
        "cc4 > 5.0",
        "cc5 >= \"2003-01-01 01:00:00\"",
        "cc6 >= \"2003-01-01 01:00:00\"",
        "cc7 = \"2003-02-02\"",
        "cc10 > 2.5"
      )
      val dataSeq = Seq(
        (1L, "1", 1.0f, 1.0d, "2002-01-01 01:00", "2002-01-01 01:00", "2001-01-01", "1111", true,
          1.0f),
        (2L, "2", 2.0f, 2.0d, "2002-02-02 02:00", "2002-02-02 02:00", "2002-02-02", "2222", false,
          2.0f)
      )
      val r = DeltaLog.forTable(spark, new TableIdentifier("table"))
      checkSkipping(r, hits, misses, dataSeq.toString(), false)
    }
  }

  test(s"Data skipping with delta statistic column drop column") {
    withTable("table") {
      sql(
        s"""CREATE TABLE table(
           |c1 long, c2 STRING, c3 FLOAT, c4 DOUBLE, c5 TIMESTAMP, c6 TIMESTAMP_NTZ,
           |c7 DATE, c8 BINARY, c9 BOOLEAN, c10 DECIMAL(3, 2))
           |USING delta
           |TBLPROPERTIES(
           |'delta.dataSkippingStatsColumns' = 'c1,c2,c3,c4,c5,c6,c7,c10',
           |'delta.columnMapping.mode' = 'name',
           |'delta.minReaderVersion' = '2',
           |'delta.minWriterVersion' = '5'
           |)
           |""".stripMargin)
      sql(s"alter table table drop COLUMN c2")
      sql(s"alter table table drop COLUMN c8")
      sql(s"alter table table drop COLUMN c9")
      val newConfiguration = sql("SHOW TBLPROPERTIES table ")
        .collect()
        .map { row =>
          row.getString(0) -> row.getString(1)
        }
        .filter(_._1 == "delta.dataSkippingStatsColumns")
        .toSeq
      assert(newConfiguration == Seq(("delta.dataSkippingStatsColumns", "c1,c3,c4,c5,c6,c7,c10")))
      sql(
        """insert into table values
          |(1, 1.0, 1.0, TIMESTAMP'2001-01-01 01:00', TIMESTAMP_NTZ'2001-01-01 01:00',
          |DATE'2001-01-01', 1.0),
          |(2, 2.0, 2.0, TIMESTAMP'2002-02-02 02:00', TIMESTAMP_NTZ'2002-02-02 02:00',
          |DATE'2002-02-02', 2.0)
          |""".stripMargin).count()
      val hits = Seq(
        "c1 = 1",
        "c3 < 1.5",
        "c4 > 1.0",
        "c5 >= \"2001-01-01 01:00:00\"",
        "c6 >= \"2001-01-01 01:00:00\"",
        "c7 = \"2002-02-02\"",
        "c10 > 1.5"
      )
      val misses = Seq(
        "c1 = 10",
        "c3 < 0.5",
        "c4 > 5.0",
        "c5 >= \"2003-01-01 01:00:00\"",
        "c6 >= \"2003-01-01 01:00:00\"",
        "c7 = \"2003-02-02\"",
        "c10 > 2.5"
      )
      val dataSeq = Seq(
        (1L, 1.0f, 1.0d, "2002-01-01 01:00", "2002-01-01 01:00", "2001-01-01", 1.0f),
        (2L, 2.0f, 2.0d, "2002-02-02 02:00", "2002-02-02 02:00", "2002-02-02", 2.0f)
      )
      val r = DeltaLog.forTable(spark, new TableIdentifier("table"))
      checkSkipping(r, hits, misses, dataSeq.toString(), false)
    }
  }

  protected def expectedStatsForFile(index: Int, colName: String, deltaLog: DeltaLog): String = {
      s"""{"numRecords":1,"minValues":{"$colName":$index},"maxValues":{"$colName":$index},""" +
        s""""nullCount":{"$colName":0}}""".stripMargin
  }

  test("data skipping get specific files with Stats API") {
    withTempDir { tempDir =>
      val tableDirPath = tempDir.getCanonicalPath

      val fileCount = 5
      // Create 5 files each having 1 row - x=1/x=2/x=3/x=4/x=5
      val data = spark.range(1, fileCount).toDF("x").repartition(fileCount, col("x"))
      data.write.format("delta").save(tableDirPath)

      var deltaLog = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))

      // Get name of file corresponding to row x=1
      val file1 = getFilesRead(deltaLog, "x = 1").head.path
      // Get name of file corresponding to row x=2
      val file2 = getFilesRead(deltaLog, "x = 2").head.path
      // Get name of file corresponding to row x=3
      val file3 = getFilesRead(deltaLog, "x = 3").head.path

      deltaLog = checkpointAndCreateNewLogIfNecessary(deltaLog)
      // Delete rows/files for x >= 3 from snapshot
      sql(s"DELETE FROM delta.`$tableDirPath` WHERE x >= 3")
      // Add another file with just one row x=6 in snapshot
      sql(s"INSERT INTO delta.`$tableDirPath` VALUES (6)")

      // We want the file from the INSERT VALUES (6) stmt. However, this `getFilesRead` call might
      // also return the AddFile (due to data file re-writes) from the DELETE stmt above. Since
      // they were committed in different commits, we can select the addFile with the higher
      // version
      val addPathToCommitVersion = deltaLog.getChanges(0).flatMap {
        case (version, actions) => actions
          .collect { case a: AddFile => a }
          .map(a => (a.path, version))
      }.toMap

      val file6 = getFilesRead(deltaLog, "x = 6")
        .map(_.path)
        .maxBy(path => addPathToCommitVersion(path))

      // At this point, our latest snapshot has only 3 rows: x=1, x=2, x=6 - all in
      // different files

      // Case-1: all passes files to the API exists in the snapshot
      val result1 = deltaLog.snapshot.getSpecificFilesWithStats(Seq(file1, file2))
        .map(addFile => (addFile.path, addFile)).toMap
      assert(result1.size == 2)
      assert(result1.keySet == Set(file1, file2))
      assert(result1(file1).stats === expectedStatsForFile(1, "x", deltaLog))
      assert(result1(file2).stats === expectedStatsForFile(2, "x", deltaLog))

      // Case-2: few passes files exists in the snapshot and few don't exists
      val result2 = deltaLog.snapshot.getSpecificFilesWithStats(Seq(file1, file2, file3))
        .map(addFile => (addFile.path, addFile)).toMap
      assert(result1 == result2)

      // Case-3: all passed files don't exists in the snapshot
      val result3 = deltaLog.snapshot.getSpecificFilesWithStats(Seq(file3, "xyz"))
      assert(result3.isEmpty)

      // Case-4: file3 doesn't exist and file6 exists in the latest commit
      val result4 = deltaLog.snapshot.getSpecificFilesWithStats(Seq(file3, file6))
        .map(addFile => (addFile.path, addFile)).toMap
      assert(result4.size == 1)
      assert(result4(file6).stats == expectedStatsForFile(6, "x", deltaLog))
    }
  }

  protected def parse(deltaLog: DeltaLog, predicate: String): Seq[Expression] = {

    // We produce a wrong filter in this case otherwise
    if (predicate == "True") return Seq(Literal.TrueLiteral)

    val filtered = spark.read.format("delta").load(deltaLog.dataPath.toString).where(predicate)
    filtered
      .queryExecution
      .optimizedPlan
      .expressions
      .flatMap(splitConjunctivePredicates)
  }

  /**
   * Returns the number of files that should be included in a scan after applying the given
   * predicate on a snapshot of the Delta log.
   *
   * @param deltaLog Delta log for a table.
   * @param predicate Predicate to run on the Delta table.
   * @param checkEmptyUnusedFilters If true, check if there were no unused filters, meaning
   *                                the given predicate was used as data or partition filters.
   * @return The number of files that should be included in a scan after applying the predicate.
   */
  protected def filesRead(
      deltaLog: DeltaLog,
      predicate: String,
      checkEmptyUnusedFilters: Boolean = false): Int =
    getFilesRead(deltaLog, predicate, checkEmptyUnusedFilters).size

  /**
   * Returns the files that should be included in a scan after applying the given predicate on
   * a snapshot of the Delta log.
   * @param deltaLog Delta log for a table.
   * @param predicate Predicate to run on the Delta table.
   * @param checkEmptyUnusedFilters If true, check if there were no unused filters, meaning
   *                                the given predicate was used as data or partition filters.
   * @return The files that should be included in a scan after applying the predicate.
   */
  protected def getFilesRead(
      deltaLog: DeltaLog,
      predicate: String,
      checkEmptyUnusedFilters: Boolean = false): Seq[AddFile] = {
    val parsed = parse(deltaLog, predicate)
    val res = deltaLog.snapshot.filesForScan(parsed)
    assert(res.total.files.get == deltaLog.snapshot.numOfFiles)
    assert(res.total.bytesCompressed.get == deltaLog.snapshot.sizeInBytes)
    assert(res.scanned.files.get == res.files.size)
    assert(res.scanned.bytesCompressed.get == res.files.map(_.size).sum)
    assert(!checkEmptyUnusedFilters || res.unusedFilters.isEmpty)
    res.files
  }

  protected def checkResultsWithPartitions(
    tableDir: String,
    predicate: String,
    expResults: Seq[(String, String)],
    expNumPartitions: Int,
    expNumFiles: Long): Unit = {
    Given(predicate)
    val df = spark.read.format("delta").load(tableDir).where(predicate)
    checkAnswer(df, expResults.toDF())

    val files = getFilesRead(DeltaLog.forTable(spark, tableDir), predicate)
    assert(files.size == expNumFiles, "# files incorrect:\n\t" + files.mkString("\n\t"))

    val partitionValues = files.map(_.partitionValues).distinct
    assert(partitionValues.size == expNumPartitions,
      "# partitions incorrect:\n\t" + partitionValues.mkString("\n\t"))
  }

  protected def getStatsDf(deltaLog: DeltaLog, columns: Column*): DataFrame = {
    deltaLog.snapshot.withStats.select("stats.*").select(columns: _*)
  }

  protected def failPretty(error: String, predicate: String, data: String) = {
    fail(
      s"""$error
         |
         |== Data ==
         |$data
       """.stripMargin)
  }

  protected def setNumIndexedColumns(path: String, numIndexedCols: Int): Unit = {
    sql(s"""
          |ALTER TABLE delta.`$path`
          |SET TBLPROPERTIES (
          |  'delta.dataSkippingNumIndexedCols' = '$numIndexedCols'
          |)""".stripMargin)
  }

  protected def setDeltaStatsColumns(path: String, deltaStatsColumns: String): Unit = {
    sql(s"""
           |ALTER TABLE delta.`$path`
           |SET TBLPROPERTIES (
           |  'delta.dataSkippingStatsColumns' = '$deltaStatsColumns'
           |)""".stripMargin)
  }

  private def isFullScan(report: ScanReport): Boolean = {
    report.size("scanned").bytesCompressed === report.size("total").bytesCompressed
  }

  protected def checkSkipping(
      log: DeltaLog,
      hits: Seq[String],
      misses: Seq[String],
      data: String,
      checkEmptyUnusedFiltersForHits: Boolean): Unit = {
    hits.foreach { predicate =>
      Given(predicate)
      if (filesRead(log, predicate, checkEmptyUnusedFiltersForHits) == 0) {
        failPretty(s"Expected hit but got miss for $predicate", predicate, data)
      }
    }

    misses.foreach { predicate =>
      Given(predicate)
      if (filesRead(log, predicate) != 0) {
        failPretty(s"Expected miss but got hit for $predicate", predicate, data)
      }
    }
    val schemaDiff = SchemaUtils.reportDifferences(
      log.snapshot.statsSchema.asNullable,
      log.snapshot.statsSchema)
    if (schemaDiff.nonEmpty) {
      fail(s"The stats schema should be nullable. Differences:\n${schemaDiff.mkString("\n")}")
    }
  }
  protected def getDataSkippingConfs(
      indexedCols: Int,
      deltaStatsColNamesOpt: Option[String]): TraversableOnce[(String, String)] = {
    val numIndexedColsConfOpt = Option(indexedCols)
      .filter(_ != defaultNumIndexedCols)
      .map(DeltaConfigs.DATA_SKIPPING_NUM_INDEXED_COLS.defaultTablePropertyKey -> _.toString)
    val indexedColNamesConfOpt = deltaStatsColNamesOpt
      .map(DeltaConfigs.DATA_SKIPPING_STATS_COLUMNS.defaultTablePropertyKey -> _)
    numIndexedColsConfOpt ++ indexedColNamesConfOpt
  }

  protected def testSkipping(
      name: String,
      data: String,
      schema: StructType = null,
      hits: Seq[String],
      misses: Seq[String],
      sqlConfs: Seq[(String, String)] = Nil,
      indexedCols: Int = defaultNumIndexedCols,
      deltaStatsColNamesOpt: Option[String] = None,
      checkEmptyUnusedFiltersForHits: Boolean = false,
      exceptionOpt: Option[Throwable] = None): Unit = {
    test(s"data skipping by stats - $name") {
      val allSQLConfs = sqlConfs ++ getDataSkippingConfs(indexedCols, deltaStatsColNamesOpt)
      withSQLConf(allSQLConfs: _*) {
        val jsonRecords = data.split("\n").toSeq
        val reader = spark.read
        if (schema != null) { reader.schema(schema) }
        val df = reader.json(jsonRecords.toDS())

        val tempDir = Utils.createTempDir()
        val r = DeltaLog.forTable(spark, tempDir)
        df.coalesce(1).write.format("delta").save(r.dataPath.toString)

        exceptionOpt.map { exception =>
          val except = intercept[Throwable] {
            deltaStatsColNamesOpt.foreach { deltaStatsColNames =>
              setDeltaStatsColumns(r.dataPath.toString, deltaStatsColNames)
              df.coalesce(1).write.format("delta").mode("overwrite").save(r.dataPath.toString)
              if (indexedCols != defaultNumIndexedCols) {
                setNumIndexedColumns(r.dataPath.toString, indexedCols)
                df.coalesce(1).write.format("delta").mode("overwrite").save(r.dataPath.toString)
              }
              checkSkipping(r, hits, misses, data, checkEmptyUnusedFiltersForHits)
            }
          }
          assert(except.getClass == exception.getClass &&
            except.getMessage.contains(exception.getMessage))
        }.getOrElse {
          if (indexedCols != defaultNumIndexedCols) {
            setNumIndexedColumns(r.dataPath.toString, indexedCols)
            df.coalesce(1).write.format("delta").mode("overwrite").save(r.dataPath.toString)
          }
          deltaStatsColNamesOpt.foreach { deltaStatsColNames =>
            setDeltaStatsColumns(r.dataPath.toString, deltaStatsColNames)
            df.coalesce(1).write.format("delta").mode("overwrite").save(r.dataPath.toString)
          }
          checkSkipping(r, hits, misses, data, checkEmptyUnusedFiltersForHits)
        }
      }
    }
  }
}

trait DataSkippingDeltaTests extends DataSkippingDeltaTestsBase
/** Tests code paths within DataSkippingReader.scala */
class DataSkippingDeltaV1Suite extends DataSkippingDeltaTests
{
  import testImplicits._

  test("data skipping flags") {
    val tempDir = Utils.createTempDir()
    val r = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
    def rStats: DataFrame =
      getStatsDf(r, $"numRecords", $"minValues.id".as("id_min"), $"maxValues.id".as("id_max"))

    val data = spark.range(10).repartition(2)

    Given("appending data without collecting stats")
    withSQLConf(DeltaSQLConf.DELTA_COLLECT_STATS.key -> "false") {
      data.write.format("delta").save(r.dataPath.toString)
      checkAnswer(rStats, Seq(Row(null, null, null), Row(null, null, null)))
    }

    Given("appending data and collecting stats")
    withSQLConf(DeltaSQLConf.DELTA_COLLECT_STATS.key -> "true") {
      data.write.format("delta").mode("append").save(r.dataPath.toString)
      checkAnswer(rStats,
        Seq(Row(null, null, null), Row(null, null, null), Row(4, 0, 8), Row(6, 1, 9)))
    }

    Given("querying reservoir without using stats")
    withSQLConf(DeltaSQLConf.DELTA_STATS_SKIPPING.key -> "false") {
      assert(filesRead(r, "id = 0") == 4)
    }

    Given("querying reservoir using stats")
    withSQLConf(DeltaSQLConf.DELTA_STATS_SKIPPING.key -> "true") {
      assert(filesRead(r, "id = 0") == 3)
    }
  }
}

/**
 * Used to disable the tests with the old stats collection behavior on long-running suites to
 * avoid time-out
 * TODO(lin): remove this after we remove the DELTA_COLLECT_STATS_USING_TABLE_SCHEMA flag
 */
trait DataSkippingDisableOldStatsSchemaTests extends DataSkippingDeltaTests {

  protected override def test(testName: String, testTags: org.scalatest.Tag*)
                             (testFun: => Any)
                             (implicit pos: org.scalactic.source.Position): Unit = {
    // Adding the null check in case tableSchemaOnlyTag has not been initialized in base traits
    val newTestTags = if (tableSchemaOnlyTag == null) testTags else tableSchemaOnlyTag +: testTags
    super.test(testName, newTestTags: _*)(testFun)(pos)
  }
}

/** DataSkipping tests under id column mapping */
trait DataSkippingDeltaIdColumnMappingTests extends DataSkippingDeltaTests
  with DeltaColumnMappingTestUtils {

  override def expectedStatsForFile(index: Int, colName: String, deltaLog: DeltaLog): String = {
    val x = colName.phy(deltaLog)
      s"""{"numRecords":1,"minValues":{"$x":$index},"maxValues":{"$x":$index},""" +
        s""""nullCount":{"$x":0}}""".stripMargin
  }
}

trait DataSkippingDeltaTestV1ColumnMappingMode extends DataSkippingDeltaIdColumnMappingTests {
  override protected def getStatsDf(deltaLog: DeltaLog, columns: Column*): DataFrame = {
    deltaLog.snapshot.withStats.select("stats.*")
      .select(convertToPhysicalColumns(columns, deltaLog): _*)
  }
}

class DataSkippingDeltaV1NameColumnMappingSuite
  extends DataSkippingDeltaV1Suite
    with DeltaColumnMappingEnableNameMode
    with DataSkippingDeltaTestV1ColumnMappingMode {
  override protected def runAllTests: Boolean = true
}

class DataSkippingDeltaV1JsonCheckpointV2Suite extends DataSkippingDeltaV1Suite {
  override def sparkConf: SparkConf = {
    super.sparkConf.setAll(
      Seq(
        DeltaConfigs.CHECKPOINT_POLICY.defaultTablePropertyKey -> CheckpointPolicy.V2.name,
        DeltaSQLConf.CHECKPOINT_V2_TOP_LEVEL_FILE_FORMAT.key -> V2Checkpoint.Format.JSON.name
      )
    )
  }
}

class DataSkippingDeltaV1ParquetCheckpointV2Suite extends DataSkippingDeltaV1Suite {
  override def sparkConf: SparkConf = {
    super.sparkConf.setAll(
      Seq(
        DeltaConfigs.CHECKPOINT_POLICY.defaultTablePropertyKey -> CheckpointPolicy.V2.name,
        DeltaSQLConf.CHECKPOINT_V2_TOP_LEVEL_FILE_FORMAT.key -> V2Checkpoint.Format.PARQUET.name
      )
    )
  }
}
