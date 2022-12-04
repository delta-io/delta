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
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.{DeltaColumnMappingSelectedTestMixin, DeltaSQLCommandTest}

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.{Analyzer, ResolveSessionCatalog}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.{DeltaMergeInto, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class MergeIntoSQLSuite extends MergeIntoSuiteBase  with DeltaSQLCommandTest
  with DeltaTestUtilsForTempViews {

  import testImplicits._

  private def basicMergeStmt(
      target: String,
      source: String,
      condition: String,
      update: String,
      insert: String): String = {
    s"""
       |MERGE INTO $target
       |USING $source
       |ON $condition
       |WHEN MATCHED THEN UPDATE SET $update
       |WHEN NOT MATCHED THEN INSERT $insert
      """.stripMargin
  }

  override def executeMerge(
      target: String,
      source: String,
      condition: String,
      update: String,
      insert: String): Unit = {
    sql(basicMergeStmt(target, source, condition, update, insert))
  }

  override def executeMerge(
      tgt: String,
      src: String,
      cond: String,
      clauses: MergeClause*): Unit = {

    val merge = s"MERGE INTO $tgt USING $src ON $cond\n" + clauses.map(_.sql).mkString("\n")
    sql(merge)
  }

  test("CTE as a source in MERGE") {
    withTable("source") {
      Seq((1, 1), (0, 3)).toDF("key1", "value").write.saveAsTable("source")
      append(Seq((2, 2), (1, 4)).toDF("key2", "value"))

      val cte = "WITH cte1 AS (SELECT key1 + 2 AS key3, value FROM source) "
      val merge = basicMergeStmt(
        target = s"delta.`$tempPath` as target",
        source = "cte1 src",
        condition = "src.key3 = target.key2",
        update = "key2 = 20 + src.key3, value = 20 + src.value",
        insert = "(key2, value) VALUES (src.key3 - 10, src.value + 10)")

      QueryTest.checkAnswer(sql(cte + merge), Seq(Row(2, 1, 0, 1)))
      checkAnswer(readDeltaTable(tempPath),
        Row(1, 4) :: // No change
        Row(22, 23) :: // Update
        Row(-7, 11) :: // Insert
        Nil)
    }
  }

  test("inline tables with set operations in source query") {
    withTable("source") {
      append(Seq((2, 2), (1, 4)).toDF("key2", "value"))

      executeMerge(
        target = s"delta.`$tempPath` as trg",
        source =
          """
            |( SELECT * FROM VALUES (1, 6, "a") as t1(key1, value, others)
            |  UNION
            |  SELECT * FROM VALUES (0, 3, "b") as t2(key1, value, others)
            |) src
          """.stripMargin,
        condition = "src.key1 = trg.key2",
        update = "trg.key2 = 20 + key1, value = 20 + src.value",
        insert = "(trg.key2, value) VALUES (key1 - 10, src.value + 10)")

      checkAnswer(readDeltaTable(tempPath),
        Row(2, 2) :: // No change
          Row(21, 26) :: // Update
          Row(-10, 13) :: // Insert
          Nil)
    }
  }

  testNestedDataSupport("conflicting assignments between two nested fields")(
    source = """{ "key": "A", "value": { "a": { "x": 0 } } }""",
    target = """{ "key": "A", "value": { "a": { "x": 1 } } }""",
    update = "value.a.x = 2" :: "value.a.x = 3" :: Nil,
    errorStrs = "There is a conflict from these SET columns" :: Nil)

  test("Negative case - basic syntax analysis SQL") {
    withTable("source") {
      Seq((1, 1), (0, 3), (1, 5)).toDF("key1", "value").createOrReplaceTempView("source")
      append(Seq((2, 2), (1, 4)).toDF("key2", "value"))

      // duplicate column names in update clause
      var e = intercept[AnalysisException] {
        executeMerge(
          target = s"delta.`$tempPath` as target",
          source = "source src",
          condition = "src.key1 = target.key2",
          update = "key2 = 1, key2 = 2",
          insert = "(key2, value) VALUES (3, 4)")
      }.getMessage

      errorContains(e, "There is a conflict from these SET columns")

      // duplicate column names in insert clause
      e = intercept[AnalysisException] {
        executeMerge(
          target = s"delta.`$tempPath` as target",
          source = "source src",
          condition = "src.key1 = target.key2",
          update = "key2 = 1, value = 2",
          insert = "(key2, key2) VALUES (3, 4)")
      }.getMessage

      errorContains(e, "Duplicate column names in INSERT clause")
    }
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"no column is used from source table - column pruning, isPartitioned: $isPartitioned") {
      withTable("source") {
        val partitions = if (isPartitioned) "key2" :: Nil else Nil
        append(Seq((2, 2), (1, 4)).toDF("key2", "value"), partitions)
        Seq((1, 1, "a"), (0, 3, "b")).toDF("key1", "value", "col1")
          .createOrReplaceTempView("source")

        // filter pushdown can cause empty join conditions and cross-join being used
        withCrossJoinEnabled {
          val merge = basicMergeStmt(
            target = s"delta.`$tempPath`",
            source = "source src",
            condition = "key2 < 0", // no row match
            update = "key2 = 20, value = 20",
            insert = "(key2, value) VALUES (10, 10)")

          val df = sql(merge)

          val readSchema: Seq[StructType] = df.queryExecution.executedPlan.collect {
            case f: FileSourceScanExec => f.requiredSchema
          }
          assert(readSchema.flatten.isEmpty, "column pruning does not work")
        }

        checkAnswer(readDeltaTable(tempPath),
          Row(2, 2) :: // No change
          Row(1, 4) :: // No change
          Row(10, 10) :: // Insert
          Row(10, 10) :: // Insert
          Nil)
      }
    }
  }

  test("negative case - omit multiple insert conditions") {
    withTable("source") {
      Seq((1, 1), (0, 3)).toDF("srcKey", "srcValue").write.saveAsTable("source")
      append(Seq((2, 2), (1, 4)).toDF("trgKey", "trgValue"))

      // only the last NOT MATCHED clause can omit the condition
      val e = intercept[AnalysisException](
        sql(s"""
          |MERGE INTO delta.`$tempPath`
          |USING source
          |ON srcKey = trgKey
          |WHEN NOT MATCHED THEN
          |  INSERT (trgValue, trgKey) VALUES (srcValue, srcKey + 1)
          |WHEN NOT MATCHED THEN
          |  INSERT (trgValue, trgKey) VALUES (srcValue, srcKey)
        """.stripMargin))
      assert(e.getMessage.contains("only the last NOT MATCHED clause can omit the condition"))
    }
  }

  def testNondeterministicOrder: Unit = {
    withTable("target") {
      // For the spark sql random() function the seed is fixed for both invocations
      val trueRandom = () => Math.random()
      val trueRandomUdf = udf(trueRandom)
      spark.udf.register("trueRandom", trueRandomUdf.asNondeterministic())

      sql("CREATE TABLE target(`trgKey` INT, `trgValue` INT) using delta")
      sql("INSERT INTO target VALUES (1,2), (3,4)")
      // This generates different data sets on every execution
      val sourceSql =
      s"""
         |(SELECT r.id AS srcKey, r.id AS srcValue
         | FROM range(1, 100000) as r
         |  JOIN (SELECT trueRandom() * 100000 AS bound) ON r.id < bound
         |) AS source
         |""".stripMargin

      sql(
        s"""
           |MERGE INTO target
           |USING ${sourceSql}
           |ON srcKey = trgKey
           |WHEN MATCHED THEN
           |  UPDATE SET trgValue = srcValue
           |WHEN NOT MATCHED THEN
           |  INSERT (trgValue, trgKey) VALUES (srcValue, srcKey)
      """.stripMargin)
    }
  }

  test("detect nondeterministic source - flag on") {
    withSQLConf(
      // materializing source would fix determinism
      DeltaSQLConf.MERGE_MATERIALIZE_SOURCE.key -> DeltaSQLConf.MergeMaterializeSource.NONE,
      DeltaSQLConf.MERGE_FAIL_IF_SOURCE_CHANGED.key -> "true"
    ) {
      val e = intercept[UnsupportedOperationException](
        testNondeterministicOrder
      )
      assert(e.getMessage.contains("source dataset is not deterministic"))
    }
  }

  test("detect nondeterministic source - flag off") {
    withSQLConf(
      // materializing source would fix determinism
      DeltaSQLConf.MERGE_MATERIALIZE_SOURCE.key -> DeltaSQLConf.MergeMaterializeSource.NONE,
      DeltaSQLConf.MERGE_FAIL_IF_SOURCE_CHANGED.key -> "false"
    ) {
      testNondeterministicOrder
    }
  }

  test("detect nondeterministic source - flag on, materialized") {
    withSQLConf(
      // materializing source fixes determinism, so the source is no longer nondeterministic
      DeltaSQLConf.MERGE_MATERIALIZE_SOURCE.key -> DeltaSQLConf.MergeMaterializeSource.ALL,
      DeltaSQLConf.MERGE_FAIL_IF_SOURCE_CHANGED.key -> "true"
    ) {
      testNondeterministicOrder
    }
  }

  test("merge into a dataset temp views with star") {
    withTempView("v") {
      def testMergeWithView(testClue: String): Unit = {
        withClue(testClue) {
          withTempView("src") {
            sql("CREATE TEMP VIEW src AS SELECT * FROM VALUES (10, 1), (20, 2) AS t(value, key)")
            sql(
              s"""
                 |MERGE INTO v
                 |USING src
                 |ON src.key = v.key
                 |WHEN MATCHED THEN
                 |  UPDATE SET *
                 |WHEN NOT MATCHED THEN
                 |  INSERT *
                 |""".stripMargin)
            checkAnswer(spark.sql(s"select * from v"), Seq(Row(0, 0), Row(1, 10), Row(2, 20)))
          }
        }
      }

      // View on path-based table
      append(Seq((0, 0), (1, 1)).toDF("key", "value"))
      readDeltaTable(tempPath).createOrReplaceTempView("v")
      testMergeWithView("with path-based table")

      // View on catalog table
      withTable("tab") {
        Seq((0, 0), (1, 1)).toDF("key", "value").write.format("delta").saveAsTable("tab")
        spark.table("tab").as("name").createOrReplaceTempView("v")
        testMergeWithView(s"delta.`$tempPath`")
      }
    }
  }


  testWithTempView("Update specific column does not work in temp views") { isSQLTempView =>
    withJsonData(
      """{ "key": "A", "value": { "a": { "x": 1 } } }""",
      """{ "key": "A", "value": { "a": { "x": 2 } } }"""
    ) { (sourceName, targetName) =>
      createTempViewFromTable(targetName, isSQLTempView)
      val fieldNames = spark.table(targetName).schema.fieldNames
      val fieldNamesStr = fieldNames.mkString("`", "`, `", "`")
      val e = intercept[DeltaAnalysisException] {
        executeMerge(
          target = "v t",
          source = s"$sourceName s",
          condition = "s.key = t.key",
          update = "value.a.x = s.value.a.x",
          insert = s"($fieldNamesStr) VALUES ($fieldNamesStr)")
      }
      assert(e.getMessage.contains("Unexpected assignment key"))
    }
  }

  test("Complex Data Type - Array of Struct") {
    withTable("source") {
      withTable("target") {
        // scalastyle:off line.size.limit
        sql("CREATE TABLE source(`smtUidNr` STRING,`evt` ARRAY<STRUCT<`busLinCd`: STRING, `cmyHdrOidNr`: STRING, `cmyLinNr`: STRING, `coeOidNr`: STRING, `dclOidNr`: STRING, `evtCd`: STRING, `evtDclUidNr`: STRING, `evtDscTe`: STRING, `evtDt`: STRING, `evtLclTmZnNa`: STRING, `evtLclTs`: STRING, `evtOidNr`: STRING, `evtRef`: ARRAY<STRUCT<`refDt`: STRING, `refNr`: STRING, `refTypCd`: STRING, `refTypDscTe`: STRING>>, `evtShu`: ARRAY<STRUCT<`ledPkgIr`: STRING, `shuNr`: STRING, `shuRef`: ARRAY<STRUCT<`shuRefDscTe`: STRING, `shuRefDt`: STRING, `shuRefEffDt`: STRING, `shuRefNr`: STRING, `shuRefTe`: STRING, `shuRefTypCd`: STRING>>>>, `evtTypCd`: STRING, `evtUsrNr`: STRING, `evtUtcTcfQy`: STRING, `evtUtcTs`: STRING, `evtWstNa`: STRING, `loc`: ARRAY<STRUCT<`adCnySdvCd`: STRING, `adMunNa`: STRING, `adPslCd`: STRING, `al1Te`: STRING, `al2Te`: STRING, `al3Te`: STRING, `locAdCnyCd`: STRING, `locOgzNr`: STRING, `locXcpDclPorCd`: STRING, `upsDisNr`: STRING, `upsRegNr`: STRING>>, `mltDelOdrNr`: STRING, `mltPrfOfDelNa`: STRING, `mltSmtConNr`: STRING, `mnfOidNr`: STRING, `rpnEntLinNr`: STRING, `rpnEntLvlStsCd`: STRING, `rpnGovAcoTe`: STRING, `rpnInfSrcCrtLclTmZnNa`: STRING, `rpnInfSrcCrtLclTs`: STRING, `rpnInfSrcCrtUtcTcfQy`: STRING, `rpnInfSrcCrtUtcTs`: STRING, `rpnLinLvlStsCd`: STRING, `rpnPgaLinNr`: STRING, `smtDcvDt`: STRING, `smtNr`: STRING, `smtUidNr`: STRING, `xcpCtmDspCd`: STRING, `xcpGovAcoTe`: STRING, `xcpPgmCd`: STRING, `xcpRlvCd`: STRING, `xcpRlvDscTe`: STRING, `xcpRlvLclTmZnNa`: STRING, `xcpRlvLclTs`: STRING, `xcpRlvUtcTcfQy`: STRING, `xcpRlvUtcTs`: STRING, `xcpRsnCd`: STRING, `xcpRsnDscTe`: STRING, `xcpStsCd`: STRING, `xcpStsDscTe`: STRING>>,`msgTs` TIMESTAMP) using delta")
        sql("CREATE TABLE target(`smtUidNr` STRING,`evt` ARRAY<STRUCT<`busLinCd`: STRING, `dclOidNr`: STRING, `evtCd`: STRING, `evtDclUidNr`: STRING, `evtDscTe`: STRING, `evtDt`: STRING, `evtLclTmZnNa`: STRING, `evtLclTs`: STRING, `evtOidNr`: STRING, `evtRef`: ARRAY<STRUCT<`refDt`: STRING, `refNr`: STRING, `refTypCd`: STRING, `refTypDscTe`: STRING>>, `evtShu`: ARRAY<STRUCT<`ledPkgIr`: STRING, `shuNr`: STRING, `shuRef`: ARRAY<STRUCT<`shuRefDscTe`: STRING, `shuRefDt`: STRING, `shuRefEffDt`: STRING, `shuRefNr`: STRING, `shuRefTe`: STRING, `shuRefTypCd`: STRING>>>>, `evtTypCd`: STRING, `evtUsrNr`: STRING, `evtUtcTcfQy`: STRING, `evtUtcTs`: STRING, `evtWstNa`: STRING, `loc`: ARRAY<STRUCT<`adCnySdvCd`: STRING, `adMunNa`: STRING, `adPslCd`: STRING, `al1Te`: STRING, `al2Te`: STRING, `al3Te`: STRING, `locAdCnyCd`: STRING, `locOgzNr`: STRING, `locXcpDclPorCd`: STRING, `upsDisNr`: STRING, `upsRegNr`: STRING>>, `mltDelOdrNr`: STRING, `mltPrfOfDelNa`: STRING, `mltSmtConNr`: STRING, `mnfOidNr`: STRING, `rpnEntLinNr`: STRING, `rpnEntLvlStsCd`: STRING, `rpnGovAcoTe`: STRING, `rpnInfSrcCrtLclTmZnNa`: STRING, `rpnInfSrcCrtLclTs`: STRING, `rpnInfSrcCrtUtcTcfQy`: STRING, `rpnInfSrcCrtUtcTs`: STRING, `rpnLinLvlStsCd`: STRING, `smtDcvDt`: STRING, `smtNr`: STRING, `smtUidNr`: STRING, `xcpCtmDspCd`: STRING, `xcpRlvCd`: STRING, `xcpRlvDscTe`: STRING, `xcpRlvLclTmZnNa`: STRING, `xcpRlvLclTs`: STRING, `xcpRlvUtcTcfQy`: STRING, `xcpRlvUtcTs`: STRING, `xcpRsnCd`: STRING, `xcpRsnDscTe`: STRING, `xcpStsCd`: STRING, `xcpStsDscTe`: STRING, `cmyHdrOidNr`: STRING, `cmyLinNr`: STRING, `coeOidNr`: STRING, `rpnPgaLinNr`: STRING, `xcpGovAcoTe`: STRING, `xcpPgmCd`: STRING>>,`msgTs` TIMESTAMP) using delta")
        // scalastyle:on line.size.limit
        sql(
          s"""
             |MERGE INTO target as r
             |USING source as u
             |ON u.smtUidNr = r.smtUidNr
             |WHEN MATCHED and u.msgTs > r.msgTs THEN
             |  UPDATE SET *
             |WHEN NOT MATCHED THEN
             |  INSERT *
             """.stripMargin)
      }
    }
  }

  Seq(true, false).foreach { partitioned =>
    test(s"User defined _change_type column doesn't get dropped - partitioned=$partitioned") {
      withTable("target") {
        sql(
          s"""CREATE TABLE target USING DELTA
             |${if (partitioned) "PARTITIONED BY (part) " else ""}
             |TBLPROPERTIES (delta.enableChangeDataFeed = false)
             |AS SELECT id, int(id / 10) AS part, 'foo' as _change_type
             |FROM RANGE(1000)
             |""".stripMargin)
        executeMerge(
          target = "target as t",
          source =
            """(
              |  SELECT id * 42 AS id, int(id / 10) AS part, 'bar' as _change_type FROM RANGE(33)
              |) s""".stripMargin,
          condition = "t.id = s.id",
          update = "*",
          insert = "*")

        sql("SELECT id, _change_type FROM target").collect().foreach { row =>
          val _change_type = row.getString(1)
          assert(_change_type === "foo" || _change_type === "bar",
            s"Invalid _change_type for id=${row.get(0)}")
        }
      }
    }
  }
}

trait MergeIntoSQLColumnMappingSuiteBase extends DeltaColumnMappingSelectedTestMixin {
  override protected def runOnlyTests: Seq[String] =
    Seq("schema evolution - new nested column with update non-* and insert * - " +
      "array of struct - longer target")
}

class MergeIntoSQLIdColumnMappingSuite extends MergeIntoSQLSuite
  with DeltaColumnMappingEnableIdMode
  with MergeIntoSQLColumnMappingSuiteBase

class MergeIntoSQLNameColumnMappingSuite extends MergeIntoSQLSuite
  with DeltaColumnMappingEnableNameMode
  with MergeIntoSQLColumnMappingSuiteBase
