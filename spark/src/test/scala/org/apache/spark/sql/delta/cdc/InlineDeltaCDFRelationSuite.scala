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

package org.apache.spark.sql.delta.cdc

import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.{AddFile, RemoveFile}
import org.apache.spark.sql.delta.commands.cdc.CDCReader.DeltaCDFRelation
import org.apache.spark.sql.delta.files.{CdcAddFileIndex, TahoeChangeFileIndex, TahoeRemoveFileIndex}
import org.apache.spark.sql.delta.schema.SchemaMergingUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.{DeltaSQLCommandTest, DeltaSQLTestUtils}

import org.apache.spark.{SparkConf, SparkThrowable}
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{FileSourceScanExec, RowDataSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

/**
 * Tests for [[org.apache.spark.sql.delta.optimizer.InlineDeltaCDFRelation]], which replaces a
 * batch CDF relation with the file scans that read its change data. Most tests run a query with
 * the rule disabled (the RDD-based scan) and enabled, and check that both give the same result.
 */
class InlineDeltaCDFRelationSuite extends QueryTest
  with SharedSparkSession
  with CDCEnabled
  with DeltaSQLTestUtils
  with DeltaSQLCommandTest {

  // Validate every plan change, so a rewrite that breaks plan integrity (e.g. an expression ID
  // with conflicting types) fails here even where the build doesn't enable it.
  override protected def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.PLAN_CHANGE_VALIDATION.key, "true")

  private val inlineKey = DeltaSQLConf.DELTA_CDF_BATCH_INLINE_SCAN.key

  /** Number of [[DeltaCDFRelation]]s left in `plan`, including in subqueries. */
  private def cdfRelationCount(plan: LogicalPlan): Int = plan.collectWithSubqueries {
    case relation: LogicalRelation if relation.relation.isInstanceOf[DeltaCDFRelation] => relation
  }.size

  /** File scans over CDF file indexes in `plan`, before adaptive execution wraps the plan. */
  private def cdfFileScans(plan: SparkPlan): Seq[FileSourceScanExec] = plan.collect {
    case scan: FileSourceScanExec if (scan.relation.location match {
      case _: CdcAddFileIndex | _: TahoeChangeFileIndex | _: TahoeRemoveFileIndex => true
      case _ => false
    }) => scan
  }

  private def rowDataSourceScans(plan: SparkPlan): Seq[RowDataSourceScanExec] = plan.collect {
    case scan: RowDataSourceScanExec => scan
  }

  /**
   * Runs `query` with the rule disabled and enabled. Checks that both return the same rows, that
   * the disabled run reads through the CDF relation and that the enabled run inlines every CDF
   * relation.
   */
  private def checkInlinedAnswer(query: => DataFrame): Unit = {
    val expected = withSQLConf(inlineKey -> "false") {
      val df = query
      val rows = df.collect().toSeq
      assert(cdfRelationCount(df.queryExecution.optimizedPlan) > 0)
      rows
    }
    withSQLConf(inlineKey -> "true") {
      val df = query
      checkAnswer(df, expected)
      assert(cdfRelationCount(df.queryExecution.optimizedPlan) === 0,
        df.queryExecution.optimizedPlan.treeString)
    }
  }

  /** The error class of `t`, or its class name if it has none. */
  private def errorKey(t: Throwable): String = t match {
    case s: SparkThrowable if s.getErrorClass != null => s.getErrorClass
    case other => other.getClass.getName
  }

  /**
   * Runs `query` with the rule disabled and enabled, and checks that both runs either return the
   * same rows or fail with the same error. Returns whether the enabled run inlined the relation,
   * or None if the query failed.
   */
  private def checkSameOutcome(query: => DataFrame): Option[Boolean] = {
    def run(inline: Boolean): Try[(Seq[Row], Boolean)] =
      withSQLConf(inlineKey -> inline.toString) {
        Try {
          val df = query
          val rows = df.collect().toSeq
          (rows, cdfRelationCount(df.queryExecution.optimizedPlan) == 0)
        }
      }
    (run(inline = false), run(inline = true)) match {
      case (Success((expected, _)), Success((actual, inlined))) =>
        QueryTest.sameRows(expected, actual).foreach(fail(_))
        Some(inlined)
      case (Failure(expected), Failure(actual)) =>
        assert(errorKey(actual) === errorKey(expected))
        None
      case (expected, actual) =>
        fail(s"RDD-based scan: $expected, inlined scan: $actual")
    }
  }

  private def latestVersion(tableName: String): Long =
    DeltaLog.forTable(spark, TableIdentifier(tableName)).update().version

  /**
   * Creates a table whose history has every kind of CDF change: inserts, updates, deletes, a
   * MERGE with all three, a no-data-change OPTIMIZE and an overwrite.
   */
  private def createTableWithChanges(tableName: String, partitioned: Boolean): Unit = {
    val partitionClause = if (partitioned) "PARTITIONED BY (part)" else ""
    sql(s"""CREATE TABLE $tableName (id INT, part INT, value STRING) USING delta
           |$partitionClause
           |TBLPROPERTIES ('${DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key}' = 'false')
           |""".stripMargin)
    sql(s"""INSERT INTO $tableName
           |SELECT /*+ REPARTITION(3) */ id, id % 3, CAST(id AS STRING) FROM range(0, 30)
           |""".stripMargin)
    sql(s"UPDATE $tableName SET value = 'updated' WHERE id % 5 = 0")
    sql(s"DELETE FROM $tableName WHERE id % 7 = 0")
    sql(s"""MERGE INTO $tableName t
           |USING (SELECT CAST(id AS INT) AS id, CAST(id % 3 AS INT) AS part FROM range(25, 35)) s
           |ON t.id = s.id
           |WHEN MATCHED AND s.id % 2 = 0 THEN UPDATE SET value = 'merged'
           |WHEN MATCHED THEN DELETE
           |WHEN NOT MATCHED THEN INSERT (id, part, value) VALUES (s.id, s.part, 'inserted')
           |""".stripMargin)
    sql(s"OPTIMIZE $tableName")
    sql(s"INSERT OVERWRITE $tableName SELECT id, id % 3, 'overwritten' FROM range(0, 5)")
  }

  test("table_changes and readChangeFeed reads are planned as file scans") {
    val tbl = "tbl"
    withTable(tbl) {
      createTableWithChanges(tbl, partitioned = false)
      val path = DeltaLog.forTable(spark, TableIdentifier(tbl)).dataPath.toString
      val reads: Seq[() => DataFrame] = Seq(
        () => sql(s"SELECT * FROM table_changes('$tbl', 0)"),
        () => sql(s"SELECT * FROM table_changes_by_path('$path', 1, 4)"),
        () => spark.read.format("delta")
          .option("readChangeFeed", "true")
          .option("startingVersion", 1)
          .option("endingVersion", 4)
          .table(tbl))
      reads.foreach { read =>
        withSQLConf(inlineKey -> "true") {
          val df = read()
          df.collect()
          assert(cdfRelationCount(df.queryExecution.optimizedPlan) === 0)
          assert(rowDataSourceScans(df.queryExecution.sparkPlan).isEmpty)
          assert(cdfFileScans(df.queryExecution.sparkPlan).nonEmpty)
        }
        withSQLConf(inlineKey -> "false") {
          val df = read()
          df.collect()
          assert(cdfRelationCount(df.queryExecution.optimizedPlan) === 1)
          assert(rowDataSourceScans(df.queryExecution.sparkPlan).size === 1)
        }
      }
    }
  }

  for (partitioned <- DeltaTestUtils.BOOLEAN_DOMAIN) {
    test(s"results match the RDD-based scan for all change types (partitioned=$partitioned)") {
      val tbl = "tbl"
      withTable(tbl) {
        createTableWithChanges(tbl, partitioned)
        val latest = latestVersion(tbl)
        val changes = s"table_changes('$tbl', 0)"
        Seq(
          s"SELECT * FROM $changes",
          s"SELECT * FROM table_changes('$tbl', 2, 4)",
          s"SELECT * FROM table_changes('$tbl', $latest, $latest)",
          s"SELECT id, _change_type FROM $changes WHERE id < 20",
          s"SELECT _change_type, _commit_version FROM $changes",
          s"SELECT _change_type, _commit_version, count(*) FROM $changes GROUP BY 1, 2",
          s"SELECT count(*) FROM $changes",
          s"SELECT * FROM $changes WHERE _change_type = 'delete'",
          s"SELECT * FROM $changes WHERE _commit_version = 4",
          s"SELECT * FROM $changes WHERE part = 1 AND _change_type LIKE 'update%'",
          s"SELECT value, max(_commit_timestamp) FROM $changes GROUP BY value"
        ).foreach { query =>
          withClue(s"Query: $query\n") {
            checkInlinedAnswer(sql(query))
          }
        }
        checkInlinedAnswer(
          spark.read.format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", 1)
            .option("endingVersion", 4)
            .table(tbl)
            .filter(col("value") === "merged" || col("_change_type") === "delete")
            .select("id", "_change_type", "_commit_version"))
      }
    }
  }

  test("results match the RDD-based scan when the changes include deletion vectors") {
    val tbl = "tbl"
    withTable(tbl) {
      withSQLConf(
          DeltaSQLConf.DELETE_USE_PERSISTENT_DELETION_VECTORS.key -> "true",
          DeltaSQLConf.UPDATE_USE_PERSISTENT_DELETION_VECTORS.key -> "true",
          DeltaSQLConf.MERGE_USE_PERSISTENT_DELETION_VECTORS.key -> "true") {
        sql(s"""CREATE TABLE $tbl (id INT, value STRING) USING delta
               |TBLPROPERTIES ('${DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key}' = 'true')
               |""".stripMargin)
        sql(s"""INSERT INTO $tbl
               |SELECT /*+ REPARTITION(2) */ CAST(id AS INT), CAST(id AS STRING)
               |FROM range(0, 100)""".stripMargin)
        // Adds a DV to existing files.
        sql(s"DELETE FROM $tbl WHERE id % 10 = 0")
        // Grows those DVs: each file is removed and re-added with a larger DV in one commit.
        sql(s"DELETE FROM $tbl WHERE id % 10 = 1")
        sql(s"UPDATE $tbl SET value = 'updated' WHERE id % 10 = 2")
        sql(s"""MERGE INTO $tbl t USING (SELECT CAST(id AS INT) AS id FROM range(0, 100)) s
               |ON t.id = s.id
               |WHEN MATCHED AND s.id % 10 = 3 THEN DELETE
               |WHEN MATCHED AND s.id % 10 = 4 THEN UPDATE SET value = 'merged'
               |""".stripMargin)
        // Removes every file, including the ones that carry a DV.
        sql(s"DELETE FROM $tbl")
      }
      val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tbl))
      val latest = deltaLog.update().version
      val actions = deltaLog.getChanges(0).flatMap(_._2).toSeq
      assert(actions.exists {
        case add: AddFile => add.deletionVector != null
        case _ => false
      }, "Expected the history to contain files with deletion vectors")
      assert(actions.exists {
        case remove: RemoveFile => remove.deletionVector != null
        case _ => false
      }, "Expected the history to remove files that carry deletion vectors")

      val ranges = (0L to latest).map(v => (v, v)) ++ Seq((0L, latest), (2L, 4L), (3L, latest))
      ranges.foreach { case (start, end) =>
        withClue(s"Range: [$start, $end]\n") {
          checkInlinedAnswer(sql(s"SELECT * FROM table_changes('$tbl', $start, $end)"))
          checkInlinedAnswer(sql(
            s"""SELECT _change_type, count(*) FROM table_changes('$tbl', $start, $end)
               |WHERE id % 2 = 0 GROUP BY _change_type""".stripMargin))
        }
      }
    }
  }

  for (mode <- Seq(NameMapping.name, IdMapping.name)) {
    test(s"results match the RDD-based scan with column mapping (mode=$mode)") {
      val tbl = "tbl"
      withTable(tbl) {
        sql(s"""CREATE TABLE $tbl (
               |  id INT,
               |  `value with space` STRING,
               |  info STRUCT<a: INT, b: STRING>,
               |  tags ARRAY<STRUCT<k: STRING>>,
               |  props MAP<STRING, STRUCT<v: INT>>)
               |USING delta
               |TBLPROPERTIES ('${DeltaConfigs.COLUMN_MAPPING_MODE.key}' = '$mode')
               |""".stripMargin)
        sql(s"""INSERT INTO $tbl SELECT
               |  CAST(id AS INT),
               |  CAST(id AS STRING),
               |  named_struct('a', CAST(id AS INT), 'b', CAST(id AS STRING)),
               |  array(named_struct('k', CAST(id AS STRING))),
               |  map(CAST(id AS STRING), named_struct('v', CAST(id AS INT)))
               |FROM range(0, 20)""".stripMargin)
        sql(s"UPDATE $tbl SET info.a = info.a + 100 WHERE id % 3 = 0")
        sql(s"DELETE FROM $tbl WHERE id % 4 = 0")
        sql(s"ALTER TABLE $tbl ADD COLUMNS (extra INT)")
        sql(s"""INSERT INTO $tbl SELECT 100, '100', named_struct('a', 100, 'b', '100'),
               |  array(named_struct('k', '100')), map('100', named_struct('v', 100)), 1
               |""".stripMargin)
        val changes = s"table_changes('$tbl', 0)"
        Seq(
          s"SELECT * FROM $changes",
          s"SELECT id, info.a, _change_type FROM $changes WHERE info.a > 100",
          s"SELECT `value with space`, extra FROM $changes WHERE `value with space` < '5'",
          s"SELECT tags[0].k, props['3'].v FROM $changes"
        ).foreach { query =>
          withClue(s"Query: $query\n") {
            checkInlinedAnswer(sql(query))
          }
        }
      }
    }
  }

  for (inline <- DeltaTestUtils.BOOLEAN_DOMAIN) {
    test(s"an unspecified ending version is resolved at the first action (inline=$inline)") {
      val tbl = "tbl"
      withTable(tbl) {
        withSQLConf(inlineKey -> inline.toString) {
          sql(s"CREATE TABLE $tbl (id INT) USING delta")
          sql(s"INSERT INTO $tbl VALUES (0)")
          val start = latestVersion(tbl)
          def versions(rows: Array[Row]): Seq[Long] =
            rows.map(_.getAs[Long]("_commit_version")).distinct.sorted.toSeq
          val reads = Seq(
            sql(s"SELECT * FROM table_changes('$tbl', $start)"),
            spark.read.format("delta")
              .option("readChangeFeed", "true")
              .option("startingVersion", start)
              .table(tbl))

          // A commit made after the DataFrame is defined but before its first action is read.
          sql(s"INSERT INTO $tbl VALUES (1)")
          reads.foreach { df =>
            assert(versions(df.collect()) === Seq(start, start + 1))
          }

          sql(s"INSERT INTO $tbl VALUES (2)")
          reads.foreach { df =>
            // Re-running the same DataFrame reuses its plan, so it does not see the new commit...
            assert(versions(df.collect()) === Seq(start, start + 1))
            // ...but a DataFrame derived from it is planned again, so it does.
            assert(versions(df.filter("true").collect()) === Seq(start, start + 1, start + 2))
            assert(df.count() === 3)
            assert(cdfRelationCount(df.queryExecution.optimizedPlan) === (if (inline) 0 else 1))
          }
        }
      }
    }
  }

  test("projections and filters are pushed into the inlined file scans") {
    val tbl = "tbl"
    withTable(tbl) {
      withSQLConf(inlineKey -> "true") {
        sql(s"""CREATE TABLE $tbl
               |(id LONG, part LONG, value STRING, info STRUCT<a: LONG, b: STRING>)
               |USING delta PARTITIONED BY (part)""".stripMargin)
        sql(s"""INSERT INTO $tbl
               |SELECT id, id % 3, CAST(id AS STRING), named_struct('a', id, 'b', 'x')
               |FROM range(0, 30)""".stripMargin)
        sql(s"UPDATE $tbl SET value = 'updated' WHERE id % 5 = 0")
        val query =
          s"""SELECT id, info.a, _change_type FROM table_changes('$tbl', 0)
             |WHERE id < 10 AND part = 1""".stripMargin
        checkInlinedAnswer(sql(query))
        val df = sql(query)
        df.collect()
        val scans = cdfFileScans(df.queryExecution.sparkPlan)
        assert(scans.nonEmpty)
        scans.foreach { scan =>
          assert(scan.partitionFilters.exists(_.references.exists(_.name == "part")),
            scan.partitionFilters)
          assert(scan.dataFilters.exists(_.references.exists(_.name == "id")), scan.dataFilters)
          // Only the referenced columns are read, and only the referenced field of the struct.
          assert(!scan.requiredSchema.fieldNames.contains("value"), scan.requiredSchema)
          assert(scan.requiredSchema("info").dataType.asInstanceOf[StructType].fieldNames.toSeq ===
            Seq("a"), scan.requiredSchema)
        }
      }
    }
  }

  test("the inlined plan has statistics from the change files") {
    val tbl = "tbl"
    withTable(tbl) {
      sql(s"CREATE TABLE $tbl (id INT) USING delta")
      sql(s"INSERT INTO $tbl VALUES (0), (1), (2)")
      val defaultSize = spark.sessionState.conf.defaultSizeInBytes
      def sizeInBytes(inline: Boolean): BigInt = withSQLConf(inlineKey -> inline.toString) {
        val df = sql(s"SELECT * FROM table_changes('$tbl', 0)")
        df.queryExecution.optimizedPlan.stats.sizeInBytes
      }
      assert(sizeInBytes(inline = false) === defaultSize)
      assert(sizeInBytes(inline = true) < defaultSize)
    }
  }

  test("the same CDF read used more than once in a query") {
    val tbl = "tbl"
    withTable(tbl) {
      createTableWithChanges(tbl, partitioned = false)
      val changes = s"table_changes('$tbl', 1, 4)"
      checkInlinedAnswer(sql(
        s"""SELECT a.id, a._change_type, b._change_type FROM $changes a JOIN $changes b
           |ON a.id = b.id AND a._commit_version < b._commit_version""".stripMargin))
      checkInlinedAnswer(sql(
        s"""SELECT id FROM $changes WHERE _change_type = 'insert'
           |UNION ALL
           |SELECT id FROM $changes WHERE _change_type = 'delete'""".stripMargin))
      checkInlinedAnswer {
        val cdf = spark.read.format("delta")
          .option("readChangeFeed", "true")
          .option("startingVersion", 1)
          .table(tbl)
        cdf.as("a").join(cdf.as("b"), Seq("id", "_commit_version")).select("id", "a.value")
      }
    }
  }

  test("CDF reads in subqueries") {
    val tbl = "tbl"
    withTable(tbl) {
      createTableWithChanges(tbl, partitioned = false)
      val changes = s"table_changes('$tbl', 1, 4)"
      Seq(
        s"SELECT * FROM range(0, 40) WHERE id IN (SELECT id FROM $changes " +
          "WHERE _change_type = 'delete')",
        s"SELECT * FROM range(0, 40) r WHERE EXISTS (SELECT 1 FROM $changes c WHERE c.id = r.id)",
        s"SELECT (SELECT count(*) FROM $changes WHERE _change_type = 'update_postimage')"
      ).foreach { query =>
        withClue(s"Query: $query\n") {
          checkInlinedAnswer(sql(query))
        }
      }
    }
  }

  test("the legacy nondeterministic filter setting keeps the RDD-based scan") {
    val tbl = "tbl"
    withTable(tbl) {
      sql(s"CREATE TABLE $tbl (id INT) USING delta")
      sql(s"INSERT INTO $tbl VALUES (0), (1)")
      withSQLConf(
          inlineKey -> "true",
          DeltaSQLConf.DELTA_CDF_SKIP_NONDETERMINISTIC_FILTER_PUSHDOWN.key -> "false") {
        val df = sql(s"SELECT * FROM table_changes('$tbl', 0) WHERE rand() >= 0")
        assert(df.collect().length === 2)
        assert(cdfRelationCount(df.queryExecution.optimizedPlan) === 1)
      }
    }
  }

  test("an empty CDF range is not inlined") {
    val tbl = "tbl"
    withTable(tbl) {
      sql(s"CREATE TABLE $tbl (id INT) USING delta")
      sql(s"INSERT INTO $tbl VALUES (0)")
      withSQLConf(
          inlineKey -> "true",
          DeltaSQLConf.DELTA_CDF_ALLOW_OUT_OF_RANGE_TIMESTAMP.key -> "true") {
        val df = spark.read.format("delta")
          .option("readChangeFeed", "true")
          .option("startingTimestamp", "2999-01-01 00:00:00")
          .table(tbl)
        checkAnswer(df, Seq.empty[Row])
        assert(cdfRelationCount(df.queryExecution.optimizedPlan) === 1)
      }
    }
  }

  test("errors match the RDD-based scan and are raised at the same point") {
    val tbl = "tbl"
    withTable(tbl) {
      sql(s"""CREATE TABLE $tbl (id INT) USING delta
             |TBLPROPERTIES ('${DeltaConfigs.CHANGE_DATA_FEED.key}' = 'false')""".stripMargin)
      sql(s"INSERT INTO $tbl VALUES (0)")
      sql(s"ALTER TABLE $tbl SET TBLPROPERTIES ('${DeltaConfigs.CHANGE_DATA_FEED.key}' = 'true')")
      sql(s"INSERT INTO $tbl VALUES (1)")
      // Version 1 was written without CDF, so a range that includes it can't be read.
      val outcomes = DeltaTestUtils.BOOLEAN_DOMAIN.map { inline =>
        withSQLConf(inlineKey -> inline.toString) {
          Try(sql(s"SELECT * FROM table_changes('$tbl', 1)")) match {
            case Failure(e) => ("definition", errorKey(e))
            case Success(df) => ("first action", errorKey(intercept[Exception](df.collect())))
          }
        }
      }
      assert(outcomes.distinct.size === 1, outcomes)
    }
  }

  test("tightened nullability: same outcome as the RDD-based scan") {
    val tbl = "tbl"
    withTable(tbl) {
      sql(s"""CREATE TABLE $tbl (id INT NOT NULL, email STRING, contact STRUCT<email: STRING>)
             |USING delta""".stripMargin)
      sql(s"INSERT INTO $tbl VALUES (1, NULL, named_struct('email', NULL))")
      val startVersion = latestVersion(tbl)
      sql(s"UPDATE $tbl SET email = 'a@x.com', contact.email = 'a@x.com'")
      val beforeConstraintVersion = latestVersion(tbl)
      // Delta does not support SET NOT NULL, so commit the metadata change directly.
      val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tbl))
      deltaLog.withNewTransaction { txn =>
        val schema = SchemaMergingUtils.transformColumns(txn.metadata.schema) { (path, field, _) =>
          val fieldPath = path :+ field.name
          if (fieldPath == Seq("email") || fieldPath == Seq("contact", "email")) {
            field.copy(nullable = false)
          } else {
            field
          }
        }
        txn.commit(txn.metadata.copy(schemaString = schema.json) :: Nil,
          DeltaOperations.ManualUpdate)
      }
      val endVersion = latestVersion(tbl)
      for {
        relax <- DeltaTestUtils.BOOLEAN_DOMAIN
        (start, end) <- Seq(
          (startVersion, endVersion),
          (startVersion, beforeConstraintVersion),
          (endVersion, endVersion))
        filter <- Seq("true", "email IS NULL", "contact.email IS NULL")
      } {
        withSQLConf(
            DeltaSQLConf.DELTA_CDF_RELAX_TIGHTENED_NULLABILITY_ENABLED.key -> relax.toString) {
          withClue(s"relax=$relax, range=[$start, $end], filter=$filter\n") {
            checkSameOutcome(sql(
              s"SELECT id, email, contact FROM table_changes('$tbl', $start, $end) WHERE $filter"))
          }
        }
      }
    }
  }
}
