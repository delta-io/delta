/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults

import io.delta.golden.GoldenTableUtils.goldenTablePath
import io.delta.kernel.data.{ColumnVector, ColumnarBatch, FilteredColumnarBatch, Row}
import io.delta.kernel.defaults.engine.{DefaultEngine, DefaultJsonHandler, DefaultParquetHandler}
import io.delta.kernel.defaults.utils.{ExpressionTestUtils, TestUtils}
import io.delta.kernel.engine.{Engine, JsonHandler, ParquetHandler}
import io.delta.kernel.expressions.Literal._
import io.delta.kernel.expressions._
import io.delta.kernel.internal.util.InternalUtils
import io.delta.kernel.internal.{InternalScanFileUtils, ScanImpl}
import io.delta.kernel.types._
import io.delta.kernel.types.IntegerType.INTEGER
import io.delta.kernel.types.StringType.STRING
import io.delta.kernel.utils.{CloseableIterator, FileStatus}
import io.delta.kernel.{Scan, Snapshot, Table}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.delta.{DeltaConfigs, DeltaLog}
import org.apache.spark.sql.types.{IntegerType => SparkIntegerType, StructField => SparkStructField, StructType => SparkStructType}
import org.apache.spark.sql.{Row => SparkRow}
import org.scalatest.funsuite.AnyFunSuite

import java.math.{BigDecimal => JBigDecimal}
import java.sql.Date
import java.time.temporal.ChronoUnit
import java.time.{Instant, OffsetDateTime}
import java.util.Optional
import scala.collection.JavaConverters._

class ScanSuite extends AnyFunSuite with TestUtils with ExpressionTestUtils with SQLHelper {

  import io.delta.kernel.defaults.ScanSuite._

  // scalastyle:off sparkimplicits
  import spark.implicits._
  // scalastyle:on sparkimplicits

  private def getDataSkippingConfs(
    indexedCols: Option[Int], deltaStatsColNamesOpt: Option[String]): Seq[(String, String)] = {
    val numIndexedColsConfOpt = indexedCols
      .map(DeltaConfigs.DATA_SKIPPING_NUM_INDEXED_COLS.defaultTablePropertyKey -> _.toString)
    val indexedColNamesConfOpt = deltaStatsColNamesOpt
      .map(DeltaConfigs.DATA_SKIPPING_STATS_COLUMNS.defaultTablePropertyKey -> _)
    (numIndexedColsConfOpt ++ indexedColNamesConfOpt).toSeq
  }

  def writeDataSkippingTable(
    tablePath: String,
    data: String,
    schema: SparkStructType,
    indexedCols: Option[Int],
    deltaStatsColNamesOpt: Option[String]): Unit = {
    withSQLConf(getDataSkippingConfs(indexedCols, deltaStatsColNamesOpt): _*) {
      val jsonRecords = data.split("\n").toSeq
      val reader = spark.read
      if (schema != null) { reader.schema(schema) }
      val df = reader.json(jsonRecords.toDS())

      val r = DeltaLog.forTable(spark, tablePath)
      df.coalesce(1).write.format("delta").save(r.dataPath.toString)
    }
  }

  private def getScanFileStats(scanFiles: Seq[Row]): Seq[String] = {
    scanFiles.map { scanFile =>
      val addFile = scanFile.getStruct(scanFile.getSchema.indexOf("add"))
      if (scanFile.getSchema.indexOf("stats") >= 0) {
        addFile.getString(scanFile.getSchema.indexOf("stats"))
      } else {
        "[No stats read]"
      }
    }
  }

  /**
   * @param tablePath the table to scan
   * @param hits query filters that should yield at least one scan file
   * @param misses query filters that should yield no scan files
   */
  def checkSkipping(tablePath: String, hits: Seq[Predicate], misses: Seq[Predicate]): Unit = {
    val snapshot = latestSnapshot(tablePath)
    hits.foreach { predicate =>
      val scanFiles = collectScanFileRows(
        snapshot.getScanBuilder(defaultEngine)
          .withFilter(defaultEngine, predicate)
          .build())
      assert(scanFiles.nonEmpty, s"Expected hit but got miss for $predicate")
    }
    misses.foreach { predicate =>
      val scanFiles = collectScanFileRows(
        snapshot.getScanBuilder(defaultEngine)
          .withFilter(defaultEngine, predicate)
          .build())
      assert(scanFiles.isEmpty, s"Expected miss but got hit for $predicate\n" +
        s"Returned scan files have stats: ${getScanFileStats(scanFiles)}"
      )
    }
  }

  test("check basic collated predicate") {
    // CHECK SKIPPING
    checkSkipping(
      goldenTablePath("data_skipping_basic_stats_collated_predicate"),
      Map(new CollatedPredicate("=", new Column("c1"), Literal.ofString("a"),
        CollationIdentifier.DEFAULT_COLLATION_IDENTIFIER) -> 2))
  }

  test("check basic collated predicate3") {
    // CHECK SKIPPING
    checkSkipping(
      goldenTablePath("data_skipping_basic_stats_collated_predicate"),
      Map(new And(new Predicate("=", new Column("c1"), Literal.ofString("a")),
        new CollatedPredicate("=", new Column("c1"), Literal.ofString("a"),
          CollationIdentifier.DEFAULT_COLLATION_IDENTIFIER)) -> 1))
  }

  /**
   * @param tablePath the table to scan
   * @param filterToNumExpFiles map of {predicate -> number of expected scan files}
   */
  def checkSkipping(tablePath: String, filterToNumExpFiles: Map[Predicate, Int]): Unit = {
    val snapshot = latestSnapshot(tablePath)
    filterToNumExpFiles.foreach { case (filter, numExpFiles) =>
      val scanFiles = collectScanFileRows(
        snapshot.getScanBuilder(defaultEngine)
          .withFilter(defaultEngine, filter)
          .build())
      assert(scanFiles.length == numExpFiles,
        s"Expected $numExpFiles but found ${scanFiles.length} for $filter")
    }
  }

  def testSkipping(
    testName: String,
    data: String,
    schema: SparkStructType = null,
    hits: Seq[Predicate],
    misses: Seq[Predicate],
    indexedCols: Option[Int] = None,
    deltaStatsColNamesOpt: Option[String] = None): Unit = {
    test(testName) {
      withTempDir { tempDir =>
        writeDataSkippingTable(
          tempDir.getCanonicalPath,
          data,
          schema,
          indexedCols,
          deltaStatsColNamesOpt
        )
        checkSkipping(
          tempDir.getCanonicalPath,
          hits,
          misses
        )
      }
    }
  }

  /* Where timestampStr is in the format of "yyyy-MM-dd'T'HH:mm:ss.SSSXXX" */
  def getTimestampPredicate(expr: String, col: Column,
                            timestampStr: String, timeStampType: String): Predicate = {
    val time = OffsetDateTime.parse(timestampStr)
    new Predicate(expr, col,
      if (timeStampType.equalsIgnoreCase("timestamp")) {
        ofTimestamp(ChronoUnit.MICROS.between(Instant.EPOCH, time))
      } else {
        ofTimestampNtz(ChronoUnit.MICROS.between(Instant.EPOCH, time))
      }
    )
  }

  //////////////////////////////////////////////////////////////////////////////////
  // Skipping tests from Spark's DataSkippingDeltaTests
  //////////////////////////////////////////////////////////////////////////////////

  testSkipping(
    "data skipping - top level, single 1",
    """{"a": 1}""",
    hits = Seq(
      AlwaysTrue.ALWAYS_TRUE, // trivial base case
      equals(col("a"), ofInt(1)), // a = 1
      equals(ofInt(1), col("a")), // 1 = a
      greaterThanOrEqual(col("a"), ofInt(1)), // a >= 1
      lessThanOrEqual(col("a"), ofInt(1)), // a <= 1
      lessThanOrEqual(col("a"), ofInt(2)), // a <= 2
      greaterThanOrEqual(col("a"), ofInt(0)), // a >= 0
      lessThanOrEqual(ofInt(1), col("a")), // 1 <= a
      greaterThanOrEqual(ofInt(1), col("a")), // 1 >= a
      greaterThanOrEqual(ofInt(2), col("a")), // 2 >= a
      lessThanOrEqual(ofInt(0), col("a")), // 0 <= a
      // note <=> is not supported yet but these should still be hits once supported
      nullSafeEquals(col("a"), ofInt(1)), // a <=> 1
      nullSafeEquals(ofInt(1), col("a")), // 1 <=> a
      not(nullSafeEquals(col("a"), ofInt(2))), // NOT a <=> 2
      // MOVE BELOW EXPRESSIONS TO MISSES ONCE SUPPORTED BY DATA SKIPPING
      not(nullSafeEquals(col("a"), ofInt(1))), // NOT a <=> 1
      nullSafeEquals(col("a"), ofInt(2)), // a <=> 2
      notEquals(col("a"), ofInt(1)), // a != 1
      nullSafeEquals(col("a"), ofInt(2)), // a <=> 2
      notEquals(ofInt(1), col("a")) // 1 != a
    ),
    misses = Seq(
      equals(col("a"), ofInt(2)), // a = 2
      equals(ofInt(2), col("a")), // 2 = a
      greaterThan(col("a"), ofInt(1)), // a > 1
      lessThan(col("a"), ofInt(1)), // a  < 1
      greaterThanOrEqual(col("a"), ofInt(2)), // a >= 2
      lessThanOrEqual(col("a"), ofInt(0)), // a <= 0
      lessThan(ofInt(1), col("a")), // 1 < a
      greaterThan(ofInt(1), col("a")), // 1 > a
      lessThanOrEqual(ofInt(2), col("a")), // 2 <= a
      greaterThanOrEqual(ofInt(0), col("a")), // 0 >= a
      not(equals(col("a"), ofInt(1))), // NOT a = 1
      not(equals(ofInt(1), col("a"))) // NOT 1 = a
    )
  )

  testSkipping(
    "data skipping - nested, single 1",
    """{"a": {"b": 1}}""",
    hits = Seq(
      equals(nestedCol("a.b"), ofInt(1)), // a.b = 1
      greaterThanOrEqual(nestedCol("a.b"), ofInt(1)), // a.b >= 1
      lessThanOrEqual(nestedCol("a.b"), ofInt(1)), // a.b <= 1
      lessThanOrEqual(nestedCol("a.b"), ofInt(2)), // a.b <= 2
      greaterThanOrEqual(nestedCol("a.b"), ofInt(0)) // a.b >= 0
    ),
    misses = Seq(
      equals(nestedCol("a.b"), ofInt(2)), // a.b = 2
      greaterThan(nestedCol("a.b"), ofInt(1)), // a.b > 1
      lessThan(nestedCol("a.b"), ofInt(1)) // a.b < 1
    )
  )

  testSkipping(
    "data skipping - double nested, single 1",
    """{"a": {"b": {"c": 1}}}""",
    hits = Seq(
      equals(nestedCol("a.b.c"), ofInt(1)), // a.b.c = 1
      greaterThanOrEqual(nestedCol("a.b.c"), ofInt(1)), // a.b.c >= 1
      lessThanOrEqual(nestedCol("a.b.c"), ofInt(1)), // a.b.c <= 1
      lessThanOrEqual(nestedCol("a.b.c"), ofInt(2)), // a.b.c <= 2
      greaterThanOrEqual(nestedCol("a.b.c"), ofInt(0)) // a.b.c >= 0
    ),
    misses = Seq(
      equals(nestedCol("a.b.c"), ofInt(2)), // a.b.c = 2
      greaterThan(nestedCol("a.b.c"), ofInt(1)), // a.b.c > 1
      lessThan(nestedCol("a.b.c"), ofInt(1)) // a.b.c < 1
    )
  )

  private def longString(str: String) = str * 1000

  testSkipping(
    "data skipping - long strings - long min",
    s"""
       {"a": '${longString("A")}'}
       {"a": 'B'}
       {"a": 'C'}
     """,
    hits = Seq(
      equals(col("a"), ofString(longString("A"))),
      greaterThan(col("a"), ofString("BA")),
      lessThan(col("a"), ofString("AB")),
      // note startsWith is not supported yet but these should still be hits once supported
      startsWith(col("a"), ofString("A")) // a like 'A%'
    ),
    misses = Seq(
      lessThan(col("a"), ofString("AA")),
      greaterThan(col("a"), ofString("CD"))
    )
  )

  testSkipping(
    "data skipping - long strings - long max",
    s"""
       {"a": 'A'}
       {"a": 'B'}
       {"a": '${longString("C")}'}
     """,
    hits = Seq(
      equals(col("a"), ofString(longString("C"))),
      greaterThan(col("a"), ofString("BA")),
      lessThan(col("a"), ofString("AB")),
      greaterThan(col("a"), ofString("CC")),
      // note startsWith is not supported yet but these should still be hits once supported
      startsWith(col("a"), ofString("A")), // a like 'A%'
      startsWith(col("a"), ofString("C")) // a like 'C%'
    ),
    misses = Seq(
      greaterThanOrEqual(col("a"), ofString("D")),
      greaterThan(col("a"), ofString("CD"))
    )
  )

  // Test:'starts with'  Expression: like
  // Test:'starts with, nested'  Expression: like

  testSkipping(
    "data skipping - and statements - simple",
    """
      {"a": 1}
      {"a": 2}
    """,
    hits = Seq(
      new And(
        greaterThan(col("a"), ofInt(0)),
        lessThan(col("a"), ofInt(3))
      ),
      new And(
        lessThanOrEqual(col("a"), ofInt(1)),
        greaterThan(col("a"), ofInt(-1))
      )
    ),
    misses = Seq(
      new And(
        lessThan(col("a"), ofInt(0)),
        greaterThan(col("a"), ofInt(-2))
      )
    )
  )

  testSkipping(
    "data skipping - and statements - two fields",
    """
      {"a": 1, "b": "2017-09-01"}
      {"a": 2, "b": "2017-08-31"}
    """,
    hits = Seq(
      new And(
        greaterThan(col("a"), ofInt(0)),
        equals(col("b"), ofString("2017-09-01"))
      ),
      new And(
        equals(col("a"), ofInt(2)),
        greaterThanOrEqual(col("b"), ofString("2017-08-30"))
      ),
      // note startsWith is not supported yet but these should still be hits once supported
      new And( //  a >= 2 AND b like '2017-08-%'
        greaterThanOrEqual(col("a"), ofInt(2)),
        startsWith(col("b"), ofString("2017-08-"))
      ),
      // MOVE BELOW EXPRESSION TO MISSES ONCE SUPPORTED BY DATA SKIPPING
      new And( // a > 0 AND b like '2016-%'
        greaterThan(col("a"), ofInt(0)),
        startsWith(col("b"), ofString("2016-"))
      )
    ),
    misses = Seq()
  )

  private val aRem100 = new ScalarExpression("%", Seq(col("a"), ofInt(100)).asJava)
  private val bRem100 = new ScalarExpression("%", Seq(col("b"), ofInt(100)).asJava)

  testSkipping(
    "data skipping - and statements - one side unsupported",
    """
      {"a": 10, "b": 10}
      {"a": 20: "b": 20}
    """,
    hits = Seq(
      // a % 100 < 10 AND b % 100 > 20
      new And(lessThan(aRem100, ofInt(10)), greaterThan(bRem100, ofInt(20)))
    ),
    misses = Seq(
      // a < 10 AND b % 100 > 20
      new And(lessThan(col("a"), ofInt(10)), greaterThan(bRem100, ofInt(20))),
      // a % 100 < 10 AND b > 20
      new And(lessThan(aRem100, ofInt(10)), greaterThan(col("b"), ofInt(20)))
    )
  )

  testSkipping(
    "data skipping - or statements - simple",
    """
      {"a": 1}
      {"a": 2}
    """,
    hits = Seq(
      // a > 0 or a < -3
      new Or(greaterThan(col("a"), ofInt(0)), lessThan(col("a"), ofInt(-3))),
      // a >= 2 or a < -1
      new Or(greaterThanOrEqual(col("a"), ofInt(2)), lessThan(col("a"), ofInt(-1)))
    ),
    misses = Seq(
      // a > 5 or a < -2
      new Or(greaterThan(col("a"), ofInt(5)), lessThan(col("a"), ofInt(-2)))
    )
  )

  testSkipping(
    "data skipping - or statements - two fields",
    """
      {"a": 1, "b": "2017-09-01"}
      {"a": 2, "b": "2017-08-31"}
    """,
    hits = Seq(
      new Or(
        lessThan(col("a"), ofInt(0)),
        equals(col("b"), ofString("2017-09-01"))
      ),
      new Or(
        equals(col("a"), ofInt(2)),
        lessThan(col("b"), ofString("2017-08-30"))
      ),
      // note startsWith is not supported yet but these should still be hits once supported
      new Or( //  a < 2 or b like '2017-08-%'
        lessThan(col("a"), ofInt(2)),
        startsWith(col("b"), ofString("2017-08-"))
      ),
      new Or( //  a >= 2 or b like '2016-08-%'
        greaterThanOrEqual(col("a"), ofInt(2)),
        startsWith(col("b"), ofString("2016-08-"))
      ),
      // MOVE BELOW EXPRESSION TO MISSES ONCE SUPPORTED BY DATA SKIPPING
      new Or( // a < 0 or b like '2016-%'
        lessThan(col("a"), ofInt(0)),
        startsWith(col("b"), ofString("2016-"))
      )
    ),
    misses = Seq()
  )

  // One side of OR by itself isn't powerful enough to prune any files.
  testSkipping(
    "data skipping - or statements - one side unsupported",
    """
      {"a": 10, "b": 10}
      {"a": 20: "b": 20}
    """,
    hits = Seq(
      // a % 100 < 10 OR b > 20
      new Or(lessThan(aRem100, ofInt(10)), greaterThan(col("b"), ofInt(20))),
      // a < 10 OR b % 100 > 20
      new Or(lessThan(col("a"), ofInt(10)), greaterThan(bRem100, ofInt(20)))
    ),
    misses = Seq(
      // a < 10 OR b > 20
      new Or(lessThan(col("a"), ofInt(10)), greaterThan(col("b"), ofInt(20)))
    )
  )

  testSkipping(
    "data skipping - not statements - simple",
    """
      {"a": 1}
      {"a": 2}
    """,
    hits = Seq(
      not(lessThan(col("a"), ofInt(0)))
    ),
    misses = Seq(
      not(greaterThan(col("a"), ofInt(0))),
      not(lessThan(col("a"), ofInt(3))),
      not(greaterThanOrEqual(col("a"), ofInt(1))),
      not(lessThanOrEqual(col("a"), ofInt(2))),
      not(not(lessThan(col("a"), ofInt(0)))),
      not(not(equals(col("a"), ofInt(3))))
    )
  )

  // NOT(AND(a, b)) === OR(NOT(a), NOT(b)) ==> One side by itself cannot prune.
  testSkipping(
    "data skipping - not statements - and",
    """
      {"a": 10, "b": 10}
      {"a": 20: "b": 20}
    """,
    hits = Seq(
      not(
        new And(
          greaterThanOrEqual(aRem100, ofInt(10)),
          lessThanOrEqual(bRem100, ofInt(20))
        )
      ),
      not(
        new And(
          greaterThanOrEqual(col("a"), ofInt(10)),
          lessThanOrEqual(bRem100, ofInt(20))
        )
      ),
      not(
        new And(
          greaterThanOrEqual(aRem100, ofInt(10)),
          lessThanOrEqual(col("b"), ofInt(20))
        )
      )
    ),
    misses = Seq(
      not(
        new And(
          greaterThanOrEqual(col("a"), ofInt(10)),
          lessThanOrEqual(col("b"), ofInt(20))
        )
      )
    )
  )

  // NOT(OR(a, b)) === AND(NOT(a), NOT(b)) => One side by itself is enough to prune.
  testSkipping(
    "data skipping - not statements - or",
    """
      {"a": 1, "b": 10}
      {"a": 2, "b": 20}
    """,
    hits = Seq(
      // NOT(a < 1 OR b > 20),
      not(new Or(lessThan(col("a"), ofInt(1)), greaterThan(col("b"), ofInt(20)))),
      // NOT(a % 100 >= 1 OR b % 100 <= 20)
      not(new Or(greaterThanOrEqual(aRem100, ofInt(1)), lessThanOrEqual(bRem100, ofInt(20))))
    ),
    misses = Seq(
      // NOT(a >= 1 OR b <= 20)
      not(
        new Or(greaterThanOrEqual(col("a"), ofInt(1)), lessThanOrEqual(col("b"), ofInt(20)))
      ),
      // NOT(a % 100 >= 1 OR b <= 20),
      not(
        new Or(greaterThanOrEqual(aRem100, ofInt(1)), lessThanOrEqual(col("b"), ofInt(20)))
      ),
      // NOT(a >= 1 OR b % 100 <= 20)
      not(
        new Or(greaterThanOrEqual(col("a"), ofInt(1)), lessThanOrEqual(bRem100, ofInt(20)))
      )
    )
  )

  // If a column does not have stats, it does not participate in data skipping, which disqualifies
  // that leg of whatever conjunct it was part of.
  testSkipping(
    "data skipping - missing stats columns",
    """
      {"a": 1, "b": 10}
      {"a": 2, "b": 20}
    """,
    indexedCols = Some(1),
    hits = Seq(
      lessThan(col("b"), ofInt(10)), // b < 10: disqualified
      // note OR is not supported yet but these should still be hits once supported
      new Or( // a < 1 OR b < 10: a disqualified by b (same conjunct)
        lessThan(col("a"), ofInt(1)), lessThan(col("b"), ofInt(10))),
      new Or( // a < 1 OR (a >= 1 AND b < 10): ==> a < 1 OR a >=1 ==> TRUE
        lessThan(col("a"), ofInt(1)),
        new And(greaterThanOrEqual(col("a"), ofInt(1)), lessThan(col("b"), ofInt(10)))
      )
    ),
    misses = Seq(
      new And( // a < 1 AND b < 10: ==> a < 1 ==> FALSE
        lessThan(col("a"), ofInt(1)), lessThan(col("b"), ofInt(10))),
      new Or( // a < 1 OR (a > 10 AND b < 10): ==> a < 1 OR a > 10 ==> FALSE
        lessThan(col("a"), ofInt(1)),
        new And(greaterThan(col("a"), ofInt(10)), lessThan(col("b"), ofInt(10)))
      )
    )
  )

  private def generateJsonData(numCols: Int): String = {
    val fields = (0 until numCols).map(i => s""""col${"%02d".format(i)}":$i""".stripMargin)

    "{" + fields.mkString(",") + "}"
  }

  testSkipping(
    "data-skipping - more columns than indexed",
    generateJsonData(33), // defaultNumIndexedCols + 1
    hits = Seq(
      equals(col("col00"), ofInt(0)),
      equals(col("col32"), ofInt(32)),
      equals(col("col32"), ofInt(-1))
    ),
    misses = Seq(
      equals(col("col00"), ofInt(1))
    )
  )

  testSkipping(
    "data skipping - nested schema - # indexed column = 3",
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
    indexedCols = Some(3),
    hits = Seq(
      equals(col("a"), ofInt(1)), // a = 1
      equals(nestedCol("b.c.d"), ofInt(2)), // b.c.d = 2
      equals(nestedCol("b.c.e"), ofInt(3)), // b.c.e = 3
      // below matches due to missing stats
      lessThan(nestedCol("b.c.f.g"), ofInt(0)), // b.c.f.g < 0
      lessThan(nestedCol("b.c.f.i"), ofInt(0)), // b.c.f.i < 0
      lessThan(nestedCol("b.l"), ofInt(0)) // b.l < 0
    ),
    misses = Seq(
      lessThan(col("a"), ofInt(0)), // a < 0
      lessThan(nestedCol("b.c.d"), ofInt(0)), // b.c.d < 0
      lessThan(nestedCol("b.c.e"), ofInt(0)) // b.c.e < 0
    )
  )

  testSkipping(
    "data skipping - nested schema - # indexed column = 0",
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
    indexedCols = Some(0),
    hits = Seq(
      // all included due to missing stats
      lessThan(col("a"), ofInt(0)),
      lessThan(nestedCol("b.c.d"), ofInt(0)),
      lessThan(nestedCol("b.c.f.i"), ofInt(0)),
      lessThan(nestedCol("b.l"), ofInt(0)),
      lessThan(col("m"), ofInt(0))
    ),
    misses = Seq()
  )

  testSkipping(
    "data skipping - indexed column names - " +
      "naming a nested column indexes all leaf fields of that column",
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
    indexedCols = Some(3),
    deltaStatsColNamesOpt = Some("b.c"),
    hits = Seq(
      // these all have missing stats
      lessThan(col("a"), ofInt(0)),
      lessThan(nestedCol("b.l"), ofInt(0)),
      lessThan(col("m"), ofInt(0))
    ),
    misses = Seq(
      lessThan(nestedCol("b.c.d"), ofInt(0)),
      lessThan(nestedCol("b.c.e"), ofInt(0)),
      lessThan(nestedCol("b.c.f.g"), ofInt(0)),
      lessThan(nestedCol("b.c.f.h"), ofInt(0)),
      lessThan(nestedCol("b.c.f.i"), ofInt(0)),
      lessThan(nestedCol("b.c.j"), ofInt(0)),
      lessThan(nestedCol("b.c.k"), ofInt(0))
    )
  )

  testSkipping(
    "data skipping - indexed column names - index only a subset of leaf columns",
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
    indexedCols = Some(3),
    deltaStatsColNamesOpt = Some("b.c.e, b.c.f.h, b.c.k, b.l"),
    hits = Seq(
      // these all have missing stats
      lessThan(col("a"), ofInt(0)),
      lessThan(nestedCol("b.c.d"), ofInt(0)),
      lessThan(nestedCol("b.c.f.g"), ofInt(0)),
      lessThan(nestedCol("b.c.f.i"), ofInt(0)),
      lessThan(nestedCol("b.c.j"), ofInt(0)),
      lessThan(col("m"), ofInt(0))
    ),
    misses = Seq(
      lessThan(nestedCol("b.c.e"), ofInt(0)),
      lessThan(nestedCol("b.c.f.h"), ofInt(0)),
      lessThan(nestedCol("b.c.k"), ofInt(0)),
      lessThan(nestedCol("b.l"), ofInt(0))
    )
  )

  testSkipping(
    "data skipping - boolean comparisons",
    """{"a": false}""",
    hits = Seq(
      equals(col("a"), ofBoolean(false)),
      greaterThan(col("a"), ofBoolean(true)),
      lessThanOrEqual(col("a"), ofBoolean(false)),
      equals(ofBoolean(true), col("a")),
      lessThan(ofBoolean(true), col("a")),
      not(equals(col("a"), ofBoolean(false)))
    ),
    misses = Seq()
  )

  // Data skipping by stats should still work even when the only data in file is null, in spite of
  // the NULL min/max stats that result -- this is different to having no stats at all.
  testSkipping(
    "data skipping - nulls - only null in file",
    """
      {"a": null }
    """,
    schema = new SparkStructType().add(new SparkStructField("a", SparkIntegerType)),
    hits = Seq(
      AlwaysTrue.ALWAYS_TRUE,
      // Ideally this should not hit as it is always FALSE, but its correct to not skip
      equals(col("a"), ofNull(INTEGER)),
      not(equals(col("a"), ofNull(INTEGER))), // Same as previous case
      isNull(col("a")),
      // This is optimized to `IsNull(a)` by NullPropagation in Spark
      nullSafeEquals(col("a"), ofNull(INTEGER)),
      not(nullSafeEquals(col("a"), ofInt(1))),
      // In delta-spark we use verifyStatsForFilter to deal with missing stats instead of
      // converting all nulls ==> true (keep). For comparisons with null statistics we end up with
      // filter: dataFilter || !(verifyStatsForFilter) = null || false = null
      // When filtering on a DF nulls are counted as false and eliminated. Thus these are misses
      // in Delta-Spark.
      // Including them is not incorrect. To skip these filters for Kernel we could use
      // verifyStatsForFilter or some other solution like inserting a && isNotNull(a) expression.
      equals(col("a"), ofInt(1)),
      lessThan(col("a"), ofInt(1)),
      greaterThan(col("a"), ofInt(1)),
      not(equals(col("a"), ofInt(1))),
      notEquals(col("a"), ofInt(1)),
      nullSafeEquals(col("a"), ofInt(1)),

      // MOVE BELOW EXPRESSIONS TO MISSES ONCE SUPPORTED BY DATA SKIPPING
      // This can be optimized to `IsNotNull(a)` (done by NullPropagation in Spark)
      not(nullSafeEquals(col("a"), ofNull(INTEGER)))
    ),
    misses = Seq(
      AlwaysFalse.ALWAYS_FALSE,
      isNotNull(col("a"))
    )
  )

  testSkipping(
    "data skipping - nulls - null + not-null in same file",
    """
      {"a": null }
      {"a": 1 }
    """,
    schema = new SparkStructType().add(new SparkStructField("a", SparkIntegerType)),
    hits = Seq(
      // Ideally this should not hit as it is always FALSE, but its correct to not skip
      equals(col("a"), ofNull(INTEGER)),
      equals(col("a"), ofInt(1)),
      AlwaysTrue.ALWAYS_TRUE,
      isNotNull(col("a")),

      // Note these expressions either aren't supported or aren't added to skipping yet
      // but should still be hits once supported
      isNull(col("a")),
      not(equals(col("a"), ofNull(INTEGER))),
      // This is optimized to `IsNull(a)` by NullPropagation in Spark
      nullSafeEquals(col("a"), ofNull(INTEGER)),
      // This is optimized to `IsNotNull(a)` by NullPropagation in Spark
      not(nullSafeEquals(col("a"), ofNull(INTEGER))),
      nullSafeEquals(col("a"), ofInt(1)),
      not(nullSafeEquals(col("a"), ofInt(1))),

      // MOVE BELOW EXPRESSIONS TO MISSES ONCE SUPPORTED BY DATA SKIPPING
      notEquals(col("a"), ofInt(1))
    ),
    misses = Seq(
      AlwaysFalse.ALWAYS_FALSE,
      lessThan(col("a"), ofInt(1)),
      greaterThan(col("a"), ofInt(1)),
      not(equals(col("a"), ofInt(1)))
    )
  )

  Seq("TIMESTAMP", "TIMESTAMP_NTZ").foreach { dataType =>
    test(s"data skipping - on $dataType type") {
      withTempDir { tempDir =>
        withSparkTimeZone("UTC") {
          val data = "2019-09-09 01:02:03.456789"
          val df = Seq(data).toDF("strTs")
            .selectExpr(
              s"CAST(strTs AS $dataType) AS ts",
              s"STRUCT(CAST(strTs AS $dataType) AS ts) AS nested")

          val r = DeltaLog.forTable(spark, tempDir.getCanonicalPath)
          df.coalesce(1).write.format("delta").save(r.dataPath.toString)
        }

        checkSkipping(
          tempDir.getCanonicalPath,
          hits = Seq(
            getTimestampPredicate("=", col("ts"), "2019-09-09T01:02:03.456789Z", dataType),
            getTimestampPredicate(">=", col("ts"), "2019-09-09T01:02:03.456789Z", dataType),
            getTimestampPredicate("<=", col("ts"), "2019-09-09T01:02:03.456789Z", dataType),
            getTimestampPredicate(
              ">=", nestedCol("nested.ts"), "2019-09-09T01:02:03.456789Z", dataType),
            getTimestampPredicate(
              "<=", nestedCol("nested.ts"), "2019-09-09T01:02:03.456789Z", dataType)
          ),
          misses = Seq(
            getTimestampPredicate("=", col("ts"), "2019-09-09T01:02:03.457001Z", dataType),
            getTimestampPredicate(">=", col("ts"), "2019-09-09T01:02:03.457001Z", dataType),
            getTimestampPredicate("<=", col("ts"), "2019-09-09T01:02:03.455999Z", dataType),
            getTimestampPredicate(
              ">=", nestedCol("nested.ts"), "2019-09-09T01:02:03.457001Z", dataType),
            getTimestampPredicate(
              "<=", nestedCol("nested.ts"), "2019-09-09T01:02:03.455999Z", dataType)
          )
        )
      }
    }
  }

  test("data skipping - Basic: Data skipping with delta statistic column") {
    withTempDir { tempDir =>
      val tablePath = tempDir.getCanonicalPath
      val tableProperty = "TBLPROPERTIES('delta.dataSkippingStatsColumns' = 'c1,c2,c3,c4,c5,c6,c9')"
      spark.sql(
        s"""CREATE TABLE delta.`$tablePath`(
           |c1 long, c2 STRING, c3 FLOAT, c4 DOUBLE, c5 TIMESTAMP, c6 DATE,
           |c7 BINARY, c8 BOOLEAN, c9 DECIMAL(3, 2)
           |) USING delta $tableProperty""".stripMargin)
      spark.sql(
        s"""insert into delta.`$tablePath` values
           |(1, '1', 1.0, 1.0, TIMESTAMP'2001-01-01 01:00', DATE'2001-01-01', '1111', true, 1.0),
           |(2, '2', 2.0, 2.0, TIMESTAMP'2002-02-02 02:00', DATE'2002-02-02', '2222', false, 2.0)
           |""".stripMargin)
      checkSkipping(
        tablePath,
        hits = Seq(
          equals(col("c1"), ofInt(1)),
          equals(col("c2"), ofString("2")),
          lessThan(col("c3"), ofFloat(1.5f)),
          greaterThan(col("c4"), ofFloat(1.0F)),
          equals(col("c6"), ofDate(InternalUtils.daysSinceEpoch(Date.valueOf("2002-02-02")))),
          // Binary Column doesn't support delta statistics.
          equals(col("c7"), ofBinary("1111".getBytes)),
          equals(col("c7"), ofBinary("3333".getBytes)),
          equals(col("c8"), ofBoolean(true)),
          equals(col("c8"), ofBoolean(false)),
          greaterThan(col("c9"), ofDecimal(JBigDecimal.valueOf(1.5), 3, 2)),
          getTimestampPredicate(">=", col("c5"), "2001-01-01T01:00:00-07:00", "TIMESTAMP")
        ),
        misses = Seq(
          equals(col("c1"), ofInt(10)),
          equals(col("c2"), ofString("4")),
          lessThan(col("c3"), ofFloat(0.5f)),
          greaterThan(col("c4"), ofFloat(5.0f)),
          equals(col("c6"), ofDate(InternalUtils.daysSinceEpoch(Date.valueOf("2003-02-02")))),
          greaterThan(col("c9"), ofDecimal(JBigDecimal.valueOf(2.5), 3, 2)),
          getTimestampPredicate(">=", col("c5"), "2003-01-01T01:00:00-07:00", "TIMESTAMP")
        )
      )
    }
  }

  test("data skipping - Data skipping with delta statistic column rename column") {
    withTempDir { tempDir =>
      val tablePath = tempDir.getCanonicalPath
      spark.sql(
        s"""CREATE TABLE delta.`$tablePath`(
           |c1 long, c2 STRING, c3 FLOAT, c4 DOUBLE, c5 TIMESTAMP, c6 DATE,
           |c7 BINARY, c8 BOOLEAN, c9 DECIMAL(3, 2)
           |) USING delta
           |TBLPROPERTIES(
           |'delta.dataSkippingStatsColumns' = 'c1,c2,c3,c4,c5,c6,c9',
           |'delta.columnMapping.mode' = 'name',
           |'delta.minReaderVersion' = '2',
           |'delta.minWriterVersion' = '5'
           |)
           |""".stripMargin)
      (1 to 9).foreach { i =>
        spark.sql(s"alter table delta.`$tablePath` RENAME COLUMN c$i to cc$i")
      }
      spark.sql(
        s"""insert into delta.`$tablePath` values
           |(1, '1', 1.0, 1.0, TIMESTAMP'2001-01-01 01:00', DATE'2001-01-01', '1111', true, 1.0),
           |(2, '2', 2.0, 2.0, TIMESTAMP'2002-02-02 02:00', DATE'2002-02-02', '2222', false, 2.0)
           |""".stripMargin)

      checkSkipping(
        tablePath,
        hits = Seq(
          equals(col("cc1"), ofInt(1)),
          equals(col("cc2"), ofString("2")),
          lessThan(col("cc3"), ofFloat(1.5f)),
          greaterThan(col("cc4"), ofFloat(1.0f)),
          equals(col("cc6"), ofDate(InternalUtils.daysSinceEpoch(Date.valueOf("2002-02-02")))),
          // Binary Column doesn't support delta statistics.
          equals(col("cc7"), ofBinary("1111".getBytes)),
          equals(col("cc7"), ofBinary("3333".getBytes)),
          equals(col("cc8"), ofBoolean(true)),
          equals(col("cc8"), ofBoolean(false)),
          greaterThan(col("cc9"), ofDecimal(JBigDecimal.valueOf(1.5), 3, 2)),
          getTimestampPredicate(">=", col("cc5"), "2001-01-01T01:00:00-07:00", "TIMESTAMP")
        ),
        misses = Seq(
          equals(col("cc1"), ofInt(10)),
          equals(col("cc2"), ofString("4")),
          lessThan(col("cc3"), ofFloat(0.5f)),
          greaterThan(col("cc4"), ofFloat(5.0f)),
          equals(col("cc6"), ofDate(InternalUtils.daysSinceEpoch(Date.valueOf("2003-02-02")))),
          getTimestampPredicate(">=", col("cc5"), "2003-01-01T01:00:00-07:00", "TIMESTAMP"),
          greaterThan(col("cc9"), ofDecimal(JBigDecimal.valueOf(2.5), 3, 2))
        )
      )
    }
  }

  test("data skipping - Data skipping with delta statistic column drop column") {
    withTempDir { tempDir =>
      val tablePath = tempDir.getCanonicalPath
      spark.sql(
        s"""CREATE TABLE delta.`$tablePath`(
           |c1 long, c2 STRING, c3 FLOAT, c4 DOUBLE, c5 TIMESTAMP, c6 DATE,
           |c7 BINARY, c8 BOOLEAN, c9 DECIMAL(3, 2), c10 TIMESTAMP_NTZ
           |) USING delta
           |TBLPROPERTIES(
           |'delta.dataSkippingStatsColumns' = 'c1,c2,c3,c4,c5,c6,c9,c10',
           |'delta.columnMapping.mode' = 'name',
           |'delta.minReaderVersion' = '2',
           |'delta.minWriterVersion' = '5'
           |)
           |""".stripMargin)
      spark.sql(s"alter table delta.`$tablePath` drop COLUMN c2")
      spark.sql(s"alter table delta.`$tablePath` drop COLUMN c7")
      spark.sql(s"alter table delta.`$tablePath` drop COLUMN c8")
      spark.sql(
        s"""insert into delta.`$tablePath` values
           |(1, 1.0, 1.0, TIMESTAMP'2001-01-01 01:00', DATE'2001-01-01',
           |1.0, TIMESTAMP_NTZ'2001-01-01 01:00'),
           |(2, 2.0, 2.0, TIMESTAMP'2002-02-02 02:00', DATE'2002-02-02',
           |2.0, TIMESTAMP_NTZ'2002-02-02 02:00')
           |""".stripMargin)
      checkSkipping(
        tablePath,
        hits = Seq(
          equals(col("c1"), ofInt(1)),
          lessThan(col("c3"), ofFloat(1.5f)),
          greaterThan(col("c4"), ofFloat(1.0f)),
          equals(col("c6"), ofDate(InternalUtils.daysSinceEpoch(Date.valueOf("2002-02-02")))),
          greaterThan(col("c9"), ofDecimal(JBigDecimal.valueOf(1.5), 3, 2)),
          getTimestampPredicate(">=", col("c5"), "2001-01-01T01:00:00-07:00", "TIMESTAMP"),
          getTimestampPredicate(">=", col("c10"), "2001-01-01T01:00:00-07:00", "TIMESTAMP_NTZ")
        ),
        misses = Seq(
          equals(col("c1"), ofInt(10)),
          lessThan(col("c3"), ofFloat(0.5f)),
          greaterThan(col("c4"), ofFloat(5.0f)),
          equals(col("c6"), ofDate(InternalUtils.daysSinceEpoch(Date.valueOf("2003-02-02")))),
          greaterThan(col("c9"), ofDecimal(JBigDecimal.valueOf(2.5), 3, 2)),
          getTimestampPredicate(">=", col("c5"), "2003-01-01T01:00:00-07:00", "TIMESTAMP"),
          getTimestampPredicate(">=", col("c10"), "2003-01-01T01:00:00-07:00", "TIMESTAMP_NTZ")
        )
      )
    }
  }

  test("data skipping by partition and data values - nulls") {
    withTempDir { tableDir =>
      val dataSeqs = Seq( // each sequence produce a single file
        Seq((null, null)),
        Seq((null, "a")),
        Seq((null, "b")),
        Seq(("a", "a"), ("a", null)),
        Seq(("b", null))
      )
      dataSeqs.foreach { seq =>
        seq.toDF("key", "value").coalesce(1)
          .write.format("delta").partitionBy("key").mode("append").save(tableDir.getCanonicalPath)
      }
      def checkResults(
          predicate: Predicate, expNumPartitions: Int, expNumFiles: Long): Unit = {
        val snapshot = latestSnapshot(tableDir.getCanonicalPath)
        val scanFiles = collectScanFileRows(
          snapshot.getScanBuilder(defaultEngine)
            .withFilter(defaultEngine, predicate)
            .build())
        assert(scanFiles.length == expNumFiles,
          s"Expected $expNumFiles but found ${scanFiles.length} for $predicate")

        val partitionValues = scanFiles.map { row =>
          InternalScanFileUtils.getPartitionValues(row)
        }.distinct
        assert(partitionValues.length == expNumPartitions,
          s"Expected $expNumPartitions partitions but found ${partitionValues.length}")
      }

      // Trivial base case
      checkResults(
        predicate = AlwaysTrue.ALWAYS_TRUE,
        expNumPartitions = 3,
        expNumFiles = 5)

      // Conditions on partition key
      checkResults(
        predicate = isNotNull(col("key")),
        expNumPartitions = 2,
        expNumFiles = 2) // 2 files with key = 'a', and 1 file with key = 'b'

      checkResults(
        predicate = equals(col("key"), ofString("a")),
        expNumPartitions = 1,
        expNumFiles = 1) // 1 files with key = 'a'


      checkResults(
        predicate = equals(col("key"), ofString("b")),
        expNumPartitions = 1,
        expNumFiles = 1) // 1 files with key = 'b'

      // TODO shouldn't partition filters on unsupported expressions just not prune instead of fail?
      checkResults(
        predicate = isNull(col("key")),
        expNumPartitions = 1,
        expNumFiles = 3) // 3 files with key = null

      /*
      NOT YET SUPPORTED EXPRESSIONS
      checkResults(
        predicate = nullSafeEquals(col("key"), ofNull(string)),
        expNumPartitions = 1,
        expNumFiles = 3) // 3 files with key = null

      checkResults(
        predicate = nullSafeEquals(col("key"), ofString("a")),
        expNumPartitions = 1,
        expNumFiles = 1) // 1 files with key <=> 'a'

      checkResults(
        predicate = nullSafeEquals(col("key"), ofString("b")),
        expNumPartitions = 1,
        expNumFiles = 1) // 1 files with key <=> 'b'
      */

      // Conditions on partitions keys and values
      checkResults(
        predicate = isNull(col("value")),
        expNumPartitions = 3,
        expNumFiles = 3)

      checkResults(
        predicate = isNotNull(col("value")),
        expNumPartitions = 2, // one of the partitions has no files left after data skipping
        expNumFiles = 3) // files with all NULL values get skipped

      checkResults(
        predicate = nullSafeEquals(col("value"), ofNull(STRING)),
        expNumPartitions = 3,
        expNumFiles = 5) // should be 3 once <=> is supported

      checkResults(
        predicate = equals(col("value"), ofString("a")),
        expNumPartitions = 3, // should be 2 if we can correctly skip "value = 'a'" for nulls
        expNumFiles = 4) // should be 2 if we can correctly skip "value = 'a'" for nulls

      checkResults(
        predicate = nullSafeEquals(col("value"), ofString("a")),
        expNumPartitions = 3, // should be 2 once <=> is supported
        expNumFiles = 5) // should be 2 once <=> is supported

      checkResults(
        predicate = notEquals(col("value"), ofString("a")),
        expNumPartitions = 3, // should be 1 once <> is supported
        expNumFiles = 5) // should be 1 once <> is supported

      checkResults(
        predicate = equals(col("value"), ofString("b")),
        expNumPartitions = 2, // should be 1 if we can correctly skip "value = 'b'" for nulls
        expNumFiles = 3) // should be 1 if we can correctly skip "value = 'a'" for nulls

      checkResults(
        predicate = nullSafeEquals(col("value"), ofString("b")),
        expNumPartitions = 3, // should be 1 once <=> is supported
        expNumFiles = 5) // should be 1 once <=> is supported

      // Conditions on both, partition keys and values
      /*
      NOT YET SUPPORTED EXPRESSIONS
      checkResults(
        predicate = new And(isNull(col("key")), equals(col("value"), ofString("a"))),
        expNumPartitions = 2,
        expNumFiles = 1) // only one file in the partition has (*, "a")

      checkResults(
        predicate = new And(nullSafeEquals(col("key"), ofNull(STRING)), nullSafeEquals(col("value"),
        ofNull(STRING))),
        expNumPartitions = 1,
        expNumFiles = 1) // 3 files with key = null, but only 1 with val = null.
      */

      checkResults(
        predicate = new And(isNotNull(col("key")), isNotNull(col("value"))),
        expNumPartitions = 1,
        expNumFiles = 1) // 1 file with (*, a)

      checkResults(
        predicate = new Or(
          nullSafeEquals(col("key"), ofNull(STRING)), nullSafeEquals(col("value"), ofNull(STRING))),
        expNumPartitions = 3,
        expNumFiles = 5) // all 5 files
    }
  }

  //////////////////////////////////////////////////////////////////////////////////
  // Kernel data skipping tests
  //////////////////////////////////////////////////////////////////////////////////

  test("basic data skipping for all types - all CM modes + checkpoint") {
    // Map of column name to (value_in_table, smaller_value, bigger_value)
    val colToLits = Map(
      "as_int" -> (ofInt(0), ofInt(-1), ofInt(1)),
      "as_long" -> (ofLong(0), ofLong(-1), ofLong(1)),
      "as_byte" -> (ofByte(0), ofByte(-1), ofByte(1)),
      "as_short" -> (ofShort(0), ofShort(-1), ofShort(1)),
      "as_float" -> (ofFloat(0), ofFloat(-1), ofFloat(1)),
      "as_double" -> (ofDouble(0), ofDouble(-1), ofDouble(1)),
      "as_string" -> (ofString("0"), ofString("!"), ofString("1")),
      "as_date" -> (ofDate(InternalUtils.daysSinceEpoch(Date.valueOf("2000-01-01"))),
        ofDate(InternalUtils.daysSinceEpoch(Date.valueOf("1999-01-01"))),
        ofDate(InternalUtils.daysSinceEpoch(Date.valueOf("2000-01-02")))),
      // TODO (delta-io/delta#2462) add Timestamp once we support skipping for TimestampType
      "as_big_decimal" -> (ofDecimal(JBigDecimal.valueOf(0), 1, 0),
        ofDecimal(JBigDecimal.valueOf(-1), 1, 0),
        ofDecimal(JBigDecimal.valueOf(1), 1, 0))
    )
    val misses = colToLits.flatMap { case (colName, (value, small, big)) =>
      Seq(
        equals(col(colName), small),
        greaterThan(col(colName), value),
        greaterThanOrEqual(col(colName), big),
        lessThan(col(colName), value),
        lessThanOrEqual(col(colName), small)
      )
    }.toSeq
    val hits = colToLits.flatMap { case (colName, (value, small, big)) =>
      Seq(
        equals(col(colName), value),
        greaterThan(col(colName), small),
        greaterThanOrEqual(col(colName), value),
        lessThan(col(colName), big),
        lessThanOrEqual(col(colName), value)
      )
    }.toSeq
    Seq(
      "data-skipping-basic-stats-all-types",
      "data-skipping-basic-stats-all-types-columnmapping-name",
      "data-skipping-basic-stats-all-types-columnmapping-id",
      "data-skipping-basic-stats-all-types-checkpoint"
    ).foreach { goldenTable =>
      checkSkipping(
        goldenTablePath(goldenTable),
        hits,
        misses
      )
    }
  }

  test("data skipping - implicit casting works") {
    checkSkipping(
      goldenTablePath("data-skipping-basic-stats-all-types"),
      hits = Seq(
        equals(col("as_short"), ofFloat(0f)),
        equals(col("as_float"), ofShort(0))
      ),
      misses = Seq(
        equals(col("as_short"), ofFloat(1f)),
        equals(col("as_float"), ofShort(1))
      )
    )
  }

  test("data skipping - incompatible schema change doesn't break") {
    withTempDir { tempDir =>
      val tablePath = tempDir.getPath
      // initially write with integer column value
      Seq(0, 1, 2).toDF.repartition(1).write.format("delta").save(tablePath)
      // overwrite with string column value
      Seq("0", "1", "2").toDF.repartition(1).write
        .format("delta").mode("overwrite").option("overwriteSchema", true).save(tablePath)

      checkSkipping(
        tablePath,
        hits = Seq(
          equals(col("value"), ofString("1"))
        ),
        misses = Seq(
          equals(col("value"), ofString("3"))
        )
      )
    }
  }

  test("data skipping - filter on non-existent column") {
    checkSkipping(
      goldenTablePath("data-skipping-basic-stats-all-types"),
      hits = Seq(equals(col("foo"), ofInt(1))),
      misses = Seq()
    )
  }

  // todo add a test with dvs where tightBounds=false

  test("data skipping - filter on partition AND data column") {
    checkSkipping(
      goldenTablePath("data-skipping-basic-stats-all-types"),
      filterToNumExpFiles = Map(
        new And(
          greaterThan(col("part"), ofInt(0)),
          greaterThan(col("id"), ofInt(0))
        ) -> 1 // should prune 3 files from partition + data filter
      )
    )
  }

  test("data skipping - stats collected changing across versions") {
    checkSkipping(
      goldenTablePath("data-skipping-change-stats-collected-across-versions"),
      filterToNumExpFiles = Map(
        equals(col("col1"), ofInt(1)) -> 1, // should prune 2 files
        equals(col("col2"), ofInt(1)) -> 2, // should prune 1 file
        new And(
          equals(col("col1"), ofInt(1)),
          equals(col("col2"), ofInt(1))
        ) -> 1 // should prune 2 files
      )
    )
  }

  test("data skipping - range of ints") {
    withTempDir { tempDir =>
      spark.range(10).repartition(1).write.format("delta").save(tempDir.getCanonicalPath)
      // to test where MIN != MAX
      checkSkipping(
        tempDir.getCanonicalPath,
        hits = Seq(
          equals(col("id"), ofInt(5)),
          lessThan(col("id"), ofInt(7)),
          lessThan(col("id"), ofInt(15)),
          lessThanOrEqual(col("id"), ofInt(9)),
          greaterThan(col("id"), ofInt(3)),
          greaterThan(col("id"), ofInt(-1)),
          greaterThanOrEqual(col("id"), ofInt(0))
        ),
        misses = Seq(
          equals(col("id"), ofInt(10)),
          lessThan(col("id"), ofInt(0)),
          lessThan(col("id"), ofInt(-1)),
          lessThanOrEqual(col("id"), ofInt(-1)),
          greaterThan(col("id"), ofInt(10)),
          greaterThan(col("id"), ofInt(11)),
          greaterThanOrEqual(col("id"), ofInt(11))
        )
      )
    }
  }

  test("data skipping - non-eligible min/max data skipping types") {
    withTempDir { tempDir =>
      val schema = SparkStructType.fromDDL("`id` INT, `arr_col` ARRAY<INT>, " +
        "`map_col` MAP<STRING, INT>, `struct_col` STRUCT<`field1`: INT>")
      val data = SparkRow(0, Array(1, 2), Map("foo" -> 1), SparkRow(5)) :: Nil
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
        .write.format("delta").save(tempDir.getCanonicalPath)
      checkSkipping(
        tempDir.getCanonicalPath,
        hits = Seq(
          equals(col("id"), ofInt(0)), // filter on the one eligible column
          isNotNull(col("id")),
          isNotNull(col("arr_col")),
          isNotNull(col("map_col")),
          isNotNull(col("struct_col")),
          isNotNull(nestedCol("struct_col.field1")),
          not(isNotNull(col("struct_col"))), // we don't skip on non-leaf columns

          not(isNull(col("id"))),
          not(isNull(col("arr_col"))),
          not(isNull(col("map_col"))),
          not(isNull(col("struct_col"))),
          not(isNull(nestedCol("struct_col.field1"))),
          isNull(col("struct_col"))
        ),
        misses = Seq(
          equals(col("id"), ofInt(1)),
          not(isNotNull(col("id"))),
          not(isNotNull(col("arr_col"))),
          not(isNotNull(col("map_col"))),
          not(isNotNull(nestedCol("struct_col.field1"))),

          isNull(col("id")),
          isNull(col("arr_col")),
          isNull(col("map_col")),
          isNull(nestedCol("struct_col.field1"))
        )
      )
    }
  }

  test("data skipping - non-eligible min/max data skipping types all nulls in file") {
    withTempDir { tempDir =>
      val schema = SparkStructType.fromDDL("`id` INT, `arr_col` ARRAY<INT>, " +
        "`map_col` MAP<STRING, INT>, `struct_col` STRUCT<`field1`: INT>")
      val data = SparkRow(null, null, null, null) :: Nil
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema).coalesce(1)
        .write.format("delta").save(tempDir.getCanonicalPath)
      checkSkipping(
        tempDir.getCanonicalPath,
        hits = Seq(
          // [not(is_not_null) is converted to is_null]
          not(isNotNull(col("id"))),
          not(isNotNull(col("arr_col"))),
          not(isNotNull(col("map_col"))),
          not(isNotNull(col("struct_col"))),
          not(isNotNull(nestedCol("struct_col.field1"))),
          isNotNull(col("struct_col")) // we don't skip on non-leaf columns
        ),
        misses = Seq(
          isNotNull(col("id")),
          isNotNull(col("arr_col")),
          isNotNull(col("map_col")),
          isNotNull(nestedCol("struct_col.field1"))
        )
      )
    }
  }

  test("data skipping - non-eligible min/max data skipping types null +" +
    "non-null in same file") {
    withTempDir { tempDir =>
      val schema = SparkStructType.fromDDL("`id` INT, `arr_col` ARRAY<INT>, " +
        "`map_col` MAP<STRING, INT>, `struct_col` STRUCT<`field1`: INT>")
      val data = SparkRow(0, Array(1, 2), Map("foo" -> 1), SparkRow(5)) ::
        SparkRow(null, null, null, null) :: Nil
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
        .write.format("delta").save(tempDir.getCanonicalPath)
      checkSkipping(
        tempDir.getCanonicalPath,
        hits = Seq(
          // [not(is_not_null) is converted to is_null]
          not(isNotNull(col("id"))),
          not(isNotNull(col("arr_col"))),
          not(isNotNull(col("map_col"))),
          not(isNotNull(col("struct_col"))),
          not(isNotNull(nestedCol("struct_col.field1"))),
          isNotNull(col("id")),
          isNotNull(col("arr_col")),
          isNotNull(col("map_col")),
          isNotNull(col("struct_col")),
          isNotNull(nestedCol("struct_col.field1"))
        ),
        misses = Seq()
      )
    }
  }

  test("data skipping - is not null with DVs in file with non-nulls") {
    withSQLConf(("spark.databricks.delta.properties.defaults.enableDeletionVectors", "true")) {
      withTempDir { tempDir =>
        def overwriteTableAndPerformDelete(deleteCondition: String): Unit = {
          val data = SparkRow(0, 0) :: SparkRow(1, 1) :: SparkRow(2, null) ::
            SparkRow(3, null) :: Nil
          val schema = new SparkStructType()
            .add("col1", SparkIntegerType)
            .add("col2", SparkIntegerType)

          spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
            .repartition(1)
            .write
            .format("delta")
            .mode("overwrite")
            .save(tempDir.getCanonicalPath)
          spark.sql(s"DELETE FROM delta.`${tempDir.getCanonicalPath}` WHERE $deleteCondition")
        }
        def checkNoSkipping(): Unit = {
          checkSkipping(
            tempDir.getCanonicalPath,
            hits = Seq(
              isNotNull(col("col2")),
              isNotNull(col("col1"))
            ),
            misses = Seq()
          )
        }

        // remove no rows
        overwriteTableAndPerformDelete("false")
        checkNoSkipping()
        // remove all null rows
        overwriteTableAndPerformDelete("col2 IS NULL")
        checkNoSkipping()
        // remove all non-null rows
        overwriteTableAndPerformDelete("col2 IS NOT NULL")
        checkNoSkipping()
        // remove one null row
        overwriteTableAndPerformDelete("col1 = 2")
        checkNoSkipping()
      }
    }
  }

  test("data skipping - is not null with DVs in file with all nulls") {
    withSQLConf(("spark.databricks.delta.properties.defaults.enableDeletionVectors", "true")) {
      withTempDir { tempDir =>
        def checkDoesSkipping(): Unit = {
          checkSkipping(
            tempDir.getCanonicalPath,
            hits = Seq(
              isNotNull(col("col1"))
            ),
            misses = Seq(
              isNotNull(col("col2"))
            )
          )
        }
        // write initial table with all nulls for col2
        val data = SparkRow(0, null) :: SparkRow(1, null) :: Nil
        val schema = new SparkStructType()
          .add("col1", SparkIntegerType)
          .add("col2", SparkIntegerType)
        spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
          .repartition(1)
          .write
          .format("delta")
          .save(tempDir.getCanonicalPath)
        checkDoesSkipping()
        // delete one of the nulls
        spark.sql(s"DELETE FROM delta.`${tempDir.getCanonicalPath}` WHERE col1 = 0")
        checkDoesSkipping()
      }
    }
  }

  test("don't read stats column when there is no usable data skipping filter") {
    val path = goldenTablePath("data-skipping-basic-stats-all-types")
    val engine = engineDisallowedStatsReads

    def snapshot(engine: Engine): Snapshot = {
      Table.forPath(engine, path).getLatestSnapshot(engine)
    }

    def verifyNoStatsColumn(scanFiles: CloseableIterator[FilteredColumnarBatch]): Unit = {
      scanFiles.forEach { batch =>
        val addSchema = batch.getData.getSchema.get("add").getDataType.asInstanceOf[StructType]
        assert(addSchema.indexOf("stats") < 0)
      }
    }

    // no filter --> don't read stats
    verifyNoStatsColumn(
      snapshot(engineDisallowedStatsReads)
        .getScanBuilder(engine).build()
        .getScanFiles(engine))

    // partition filter only --> don't read stats
    val partFilter = equals(new Column("part"), ofInt(1))
    verifyNoStatsColumn(
      snapshot(engineDisallowedStatsReads)
        .getScanBuilder(engine).withFilter(engine, partFilter).build()
        .getScanFiles(engine))

    // no eligible data skipping filter --> don't read stats
    val nonEligibleFilter = lessThan(
      new ScalarExpression("%", Seq(col("as_int"), ofInt(10)).asJava),
      ofInt(1))
    verifyNoStatsColumn(
      snapshot(engineDisallowedStatsReads)
        .getScanBuilder(engine).withFilter(engine, nonEligibleFilter).build()
        .getScanFiles(engine))
  }

  test("data skipping - prune schema correctly for various predicates") {
    def structTypeToLeafColumns(
        schema: StructType, parentPath: Seq[String] = Seq()): Set[Column] = {
      schema.fields().asScala.flatMap { field =>
        field.getDataType() match {
          case nestedSchema: StructType =>
            assert(nestedSchema.fields().size() > 0,
              "Schema should not have field of type StructType with no child fields")
            structTypeToLeafColumns(nestedSchema, parentPath ++ Seq(field.getName()))
          case _ =>
            Seq(new Column(parentPath.toArray :+ field.getName()))
        }
      }.toSet
    }
    def verifySchema(expectedReadCols: Set[Column]): StructType => Unit = { readSchema =>
      assert(structTypeToLeafColumns(readSchema) == expectedReadCols)
    }
    val path = goldenTablePath("data-skipping-basic-stats-all-types")
    // Map of expression -> expected read columns
    Map(
      equals(col("as_int"), ofInt(0)) ->
        Set(nestedCol("minValues.as_int"), nestedCol("maxValues.as_int")),
      lessThan(col("as_int"), ofInt(0)) -> Set(nestedCol("minValues.as_int")),
      greaterThan(col("as_int"), ofInt(0)) -> Set(nestedCol("maxValues.as_int")),
      greaterThanOrEqual(col("as_int"), ofInt(0)) -> Set(nestedCol("maxValues.as_int")),
      lessThanOrEqual(col("as_int"), ofInt(0)) -> Set(nestedCol("minValues.as_int")),
      new And(
        lessThan(col("as_int"), ofInt(0)),
        greaterThan(col("as_long"), ofInt(0))
      ) -> Set(nestedCol("minValues.as_int"), nestedCol("maxValues.as_long"))
    ).foreach { case (predicate, expectedCols) =>
      val engine = engineVerifyJsonParseSchema(verifySchema(expectedCols))
      collectScanFileRows(
        Table.forPath(engine, path).getLatestSnapshot(engine)
          .getScanBuilder(engine)
          .withFilter(engine, predicate)
          .build(),
        engine = engine)
    }
  }

  //////////////////////////////////////////////////////////////////////////////////////////
  // Check the includeStats parameter on ScanImpl.getScanFiles(engine, includeStats)
  //////////////////////////////////////////////////////////////////////////////////////////

  test("check ScanImpl.getScanFiles for includeStats=true") {
    // When includeStats=true the JSON statistic should always be returned in the scan files
    withTempDir { tempDir =>
      spark.range(10).write.format("delta").save(tempDir.getCanonicalPath)
      def checkStatsPresent(scan: Scan): Unit = {
        val scanFileBatches = scan.asInstanceOf[ScanImpl].getScanFiles(defaultEngine, true)
        scanFileBatches.forEach { batch =>
          assert(batch.getData().getSchema() == InternalScanFileUtils.SCAN_FILE_SCHEMA_WITH_STATS)
        }
      }
      // No query filter
      checkStatsPresent(
        latestSnapshot(tempDir.getCanonicalPath)
          .getScanBuilder(defaultEngine)
          .build()
      )
      // Query filter but no valid data skipping filter
      checkStatsPresent(
        latestSnapshot(tempDir.getCanonicalPath)
          .getScanBuilder(defaultEngine)
          .withFilter(
            defaultEngine,
            greaterThan(
              new ScalarExpression("+", Seq(col("id"), ofInt(10)).asJava),
              ofInt(100)
            )
          ).build()
      )
      // With valid data skipping filter present
      checkStatsPresent(
        latestSnapshot(tempDir.getCanonicalPath)
          .getScanBuilder(defaultEngine)
          .withFilter(
            defaultEngine,
            greaterThan(
              col("id"),
              ofInt(0)
            )
          ).build()
      )
    }
  }

  Seq(
    ("version 0 no predicate", None, Some(0), 2),
    ("latest version (has checkpoint) no predicate", None, None, 4),
    ("version 0 with predicate", Some(equals(col("id"), ofLong(10))), Some(0), 1)
  ).foreach { case (nameSuffix, predicate, snapshotVersion, expectedNumFiles) =>
    test(s"read scan files with variant - $nameSuffix") {
      val path = getTestResourceFilePath("spark-variant-checkpoint")
      val table = Table.forPath(defaultEngine, path)
      val snapshot = snapshotVersion match {
        case Some(version) => table.getSnapshotAsOfVersion(defaultEngine, version)
        case None => table.getLatestSnapshot(defaultEngine)
      }
      val snapshotSchema = snapshot.getSchema(defaultEngine)

      val expectedSchema = new StructType()
        .add("id", LongType.LONG, true)
        .add("v", VariantType.VARIANT, true)
        .add("array_of_variants", new ArrayType(VariantType.VARIANT, true), true)
        .add("struct_of_variants", new StructType().add("v", VariantType.VARIANT, true))
        .add("map_of_variants", new MapType(StringType.STRING, VariantType.VARIANT, true), true)
        .add(
          "array_of_struct_of_variants",
          new ArrayType(new StructType().add("v", VariantType.VARIANT, true), true),
          true
        )
        .add(
          "struct_of_array_of_variants",
          new StructType().add("v", new ArrayType(VariantType.VARIANT, true), true),
          true
        )

      assert(snapshotSchema == expectedSchema)

      val scanBuilder = snapshot.getScanBuilder(defaultEngine)
      val scan = predicate match {
        case Some(pred) => scanBuilder.withFilter(defaultEngine, pred).build()
        case None => scanBuilder.build()
      }

      val scanFiles = scan.asInstanceOf[ScanImpl].getScanFiles(defaultEngine, true)

      assert(scanFiles.next().getRows().toSeq.length == expectedNumFiles)
    }
  }
}

object ScanSuite {

  private def throwErrorIfAddStatsInSchema(readSchema: StructType): Unit = {
    if (readSchema.indexOf("add") >= 0) {
      val addSchema = readSchema.get("add").getDataType.asInstanceOf[StructType]
      assert(addSchema.indexOf("stats") < 0, "reading column add.stats is not allowed");
    }
  }

  /**
   * Returns a custom engine implementation that doesn't allow "add.stats" in the read schema
   * for parquet or json handlers.
   */
  def engineDisallowedStatsReads: Engine = {
    val hadoopConf = new Configuration()
    new DefaultEngine(hadoopConf) {

      override def getParquetHandler: ParquetHandler = {
        new DefaultParquetHandler(hadoopConf) {
          override def readParquetFiles(
              fileIter: CloseableIterator[FileStatus],
              physicalSchema: StructType,
              predicate: Optional[Predicate]): CloseableIterator[ColumnarBatch] = {
            throwErrorIfAddStatsInSchema(physicalSchema)
            super.readParquetFiles(fileIter, physicalSchema, predicate)
          }
        }
      }

      override def getJsonHandler: JsonHandler = {
        new DefaultJsonHandler(hadoopConf) {
          override def readJsonFiles(
              fileIter: CloseableIterator[FileStatus],
              physicalSchema: StructType,
              predicate: Optional[Predicate]): CloseableIterator[ColumnarBatch] = {
            throwErrorIfAddStatsInSchema(physicalSchema)
            super.readJsonFiles(fileIter, physicalSchema, predicate)
          }
        }
      }
    }
  }

  def engineVerifyJsonParseSchema(verifyFx: StructType => Unit): Engine = {
    val hadoopConf = new Configuration()
    new DefaultEngine(hadoopConf) {
      override def getJsonHandler: JsonHandler = {
        new DefaultJsonHandler(hadoopConf) {
          override def parseJson(stringVector: ColumnVector, schema: StructType,
              selectionVector: Optional[ColumnVector]): ColumnarBatch = {
            verifyFx(schema)
            super.parseJson(stringVector, schema, selectionVector)
          }
        }
      }
    }
  }
}
