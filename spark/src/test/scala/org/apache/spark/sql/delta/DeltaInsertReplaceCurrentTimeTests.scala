/*
 * Copyright (2026) The Delta Lake Project Authors.
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

import scala.collection.mutable

import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.TableIdentifier

/** Current-time regressions shared by SQL and replaceOn/replaceUsing DataFrame writers. */
trait DeltaInsertReplaceCurrentTimeTests
  extends DeltaInsertReplaceOnOrUsingTestUtils
  with DeltaTableProvider {

  protected def writeReplaceWithCurrentTimeDF(sourceDF: DataFrame, target: String): Unit

  // Use a string key so the tests also run when this optional safeguard is unavailable.
  protected val disableSourceMaterializationNotAllowedConfKey =
    "spark.databricks.delta.merge.disableSourceMaterializationNotAllowed"

  /** Number of keys in the 1 ms timestamp grid used by the current-time tests below. */
  protected val currentTimeGridNumKeys = 200000L

  /**
   * Delays the DELETE step, so that the DELETE and the INSERT step of INSERT REPLACE ON/USING plan
   * the source at clearly different wall-clock times whether or not they run in parallel.
   */
  protected class DelayingDeleteObserver extends InsertAtomicReplaceExecutionObserver {
    override def delete[T](f: => T): T = {
      Thread.sleep(500)
      f
    }
    override def insert[T](f: => T): T = f
    override def commit[T](f: => T): T = f
  }

  /**
   * Checks that DELETE and INSERT select the same keys as time advances between their scans.
   *
   * - Target and source timestamps are 1 ms apart. For example, `ts <= T` replaces keys [0, 1]
   *   when T falls between keys 1 and 2; later keys keep their target rows.
   * - If DELETE selects [0, 1, 2] but INSERT selects [0, 1], key 2 is lost; reversing those sets
   *   duplicates key 2. A shared cutoff leaves every key once; `ts >= T` reverses the two sides.
   * - Returns total rows, distinct keys, source rows, kept target rows, and whether the two
   *   timestamp ranges meet at one cutoff without a gap.
   */
  private def runReplaceWithCurrentTimeFilter(
      gridStartSql: String,
      sourceFilter: String,
      sourceSelectsRowsBeforeCutoff: Boolean): Row = {
    val numKeys = currentTimeGridNumKeys
    createTable(
      tableName = "target",
      tableCols = Seq("k bigint", "ts timestamp", "row_origin string"))
    sql(
      s"""
         |INSERT INTO target
         |SELECT id, $gridStartSql + make_dt_interval(0, 0, 0, id / 1000.0), 'target'
         |FROM range(0, $numKeys)
         |""".stripMargin)
    createTable(
      tableName = "source",
      tableCols = Seq("k bigint", "ts timestamp", "row_origin string"))
    sql("INSERT INTO source SELECT k, ts, 'source' FROM target")

    InsertAtomicReplaceExecutionObserver.withObserver(new DelayingDeleteObserver) {
      writeReplaceWithCurrentTimeDF(
        sql(s"SELECT * FROM source WHERE $sourceFilter"),
        spark.sessionState.catalog.getTableMetadata(TableIdentifier("target")).location.toString)
    }

    // Keys increase with timestamps: ts <= cutoff replaces the earlier rows, while ts >= cutoff
    // replaces the later rows. The two groups must meet without any missing or duplicate keys.
    val (rowOriginBeforeCutoff, rowOriginAfterCutoff) =
      if (sourceSelectsRowsBeforeCutoff) ("source", "target") else ("target", "source")
    sql(
      s"""
         |SELECT
         |  count(*),
         |  count(DISTINCT k),
         |  count_if(row_origin = 'source'),
         |  count_if(row_origin = 'target'),
         |  max(k) FILTER (WHERE row_origin = '$rowOriginBeforeCutoff') + 1 =
         |    min(k) FILTER (WHERE row_origin = '$rowOriginAfterCutoff')
         |FROM target
         |""".stripMargin).head()
  }

  /**
   * Checks that the statement used one instant: every key is present exactly once, both the
   * replaced and the kept keys are non-empty (the cutoff falls inside the grid), and the replaced
   * keys are exactly those on one side of a single cutoff.
   */
  private def checkSingleInstantResult(result: Row): Unit = {
    val numKeys = currentTimeGridNumKeys
    assert(result.getLong(0) === numKeys, s"missing or duplicate rows: $result")
    assert(result.getLong(1) === numKeys, s"missing keys: $result")
    assert(result.getLong(2) > 0 && result.getLong(3) > 0, s"cutoff not inside grid: $result")
    assert(result.getBoolean(4), s"replaced keys are not cut at a single instant: $result")
  }

  for ((filterName, gridStartSql, sourceFilter, sourceSelectsRowsBeforeCutoff) <- Seq(
      ("ts <= current_timestamp()", "current_timestamp()", "ts <= current_timestamp()", true),
      ("lookback ts >= now() - INTERVAL 1 DAY", "now() - INTERVAL 1 DAY",
        "ts >= now() - INTERVAL 1 DAY", false))) {
    test(s"INSERT REPLACE ON/USING evaluates the current time once, $filterName",
      ChecksDeltaInternals("uses V1 DELETE/INSERT phase observers")) {
      withSQLConf(
          DeltaSQLConf.INSERT_REPLACE_ON_OR_USING_PIN_STATEMENT_TIME_ENABLED.key -> "true",
          DeltaSQLConf.INSERT_REPLACE_ON_OR_USING_MATERIALIZE_SOURCE.key -> "none",
          disableSourceMaterializationNotAllowedConfKey -> "false") {
        withTable("target", "source") {
          checkSingleInstantResult(
            runReplaceWithCurrentTimeFilter(
              gridStartSql, sourceFilter, sourceSelectsRowsBeforeCutoff))
        }
      }
    }
  }

  test("INSERT REPLACE ON/USING evaluates the current time once, ts <= current_timestamp() " +
      "(gate off reproduces the bug)",
    ChecksDeltaInternals("uses V1 DELETE/INSERT phase observers")) {
    withSQLConf(
        DeltaSQLConf.INSERT_REPLACE_ON_OR_USING_PIN_STATEMENT_TIME_ENABLED.key -> "false",
        DeltaSQLConf.INSERT_REPLACE_ON_OR_USING_MATERIALIZE_SOURCE.key -> "none",
        disableSourceMaterializationNotAllowedConfKey -> "false") {
      withTable("target", "source") {
        val result = runReplaceWithCurrentTimeFilter(
          gridStartSql = "current_timestamp()",
          sourceFilter = "ts <= current_timestamp()",
          sourceSelectsRowsBeforeCutoff = true)
        // The DELETE and the INSERT step fold the current time to different instants, so the keys
        // between the two instants are lost (parallel steps) or duplicated (sequential steps).
        val numKeys = currentTimeGridNumKeys
        val missingKeys = numKeys - result.getLong(1)
        val duplicateRows = result.getLong(0) - result.getLong(1)
        assert(missingKeys + duplicateRows > 0, s"expected lost or duplicated keys: $result")
      }
    }
  }

  for (pinStatementTime <- Seq(false, true)) {
    test(s"replace retains its source clock across a materialization retry, " +
        s"pinStatementTime=$pinStatementTime",
      ChecksDeltaInternals("drops a V1 materialized source checkpoint")) {
      withSQLConf(
          DeltaSQLConf.INSERT_REPLACE_ON_OR_USING_PIN_STATEMENT_TIME_ENABLED.key ->
            pinStatementTime.toString,
          DeltaSQLConf.INSERT_REPLACE_ON_OR_USING_MATERIALIZE_SOURCE.key -> "all") {
        withTempDir { targetDir =>
          val targetPath = targetDir.getAbsolutePath
          sql("SELECT id AS k, CAST(NULL AS TIMESTAMP) AS ts FROM range(0, 10)")
            .write.format(writeFormat).save(targetPath)

          // - First attempt: source rows capture 10:00:00; the observer drops their checkpoint.
          // - Retry: the enabled flag must still insert 10:00:00, rather than a new time.
          val checkpointIds = mutable.Set.empty[Int]
          var cacheDroppedAtMicros: Option[Long] = None
          val dropFirstCheckpoint = new InsertAtomicReplaceExecutionObserver {
            private def beforePhase(): Unit = synchronized {
              val checkpoints = spark.sparkContext.getPersistentRDDs.values
                .filter(_.name == "mergeMaterializedSource").toSeq
              assert(checkpoints.size <= 1, s"expected one source checkpoint: $checkpoints")
              checkpoints.headOption match {
                case Some(checkpoint) =>
                  assert(checkpoint.isCheckpointed)
                  checkpointIds += checkpoint.id
                  if (cacheDroppedAtMicros.isEmpty) {
                    cacheDroppedAtMicros = Some(
                      sql("SELECT unix_micros(current_timestamp())").head().getLong(0))
                    checkpoint.unpersist(blocking = true)
                  }
                case None =>
                  // The other phase can reach the observer after the first cache was dropped.
                  assert(cacheDroppedAtMicros.isDefined)
              }
            }

            override def delete[T](f: => T): T = {
              beforePhase()
              f
            }

            override def insert[T](f: => T): T = {
              beforePhase()
              f
            }

            override def commit[T](f: => T): T = f
          }

          InsertAtomicReplaceExecutionObserver.withObserver(dropFirstCheckpoint) {
            writeReplaceWithCurrentTimeDF(
              sql("SELECT id AS k, current_timestamp() AS ts FROM range(0, 10)"),
              targetPath)
          }

          assert(checkpointIds.size === 2, s"expected one retry: $checkpointIds")
          val replaced = spark.read.format(writeFormat).load(targetPath)
          checkAnswer(replaced.select("k"), (0L until 10L).map(Row(_)))
          val timestamps = replaced.selectExpr("unix_micros(ts)").distinct().collect()
          assert(timestamps.length === 1 && !timestamps.head.isNullAt(0),
            s"expected one source timestamp: ${timestamps.toSeq}")
          val insertedAtMicros = timestamps.head.getLong(0)
          val droppedAtMicros = cacheDroppedAtMicros.get
          if (pinStatementTime) {
            assert(insertedAtMicros < droppedAtMicros,
              s"statement time $insertedAtMicros was refreshed after cache loss $droppedAtMicros")
          } else {
            assert(insertedAtMicros > droppedAtMicros,
              s"expected the retry to refresh time after cache loss $droppedAtMicros, " +
                s"got $insertedAtMicros")
          }
          assert(!spark.sparkContext.getPersistentRDDs.values
            .exists(_.name == "mergeMaterializedSource"))
        }
      }
    }
  }
}
