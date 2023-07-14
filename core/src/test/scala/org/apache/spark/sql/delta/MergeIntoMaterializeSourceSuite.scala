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

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.sql.delta.DeltaTestUtils._
import org.apache.spark.sql.delta.commands.MergeStats
import org.apache.spark.sql.delta.commands.merge.{MergeIntoMaterializeSourceError, MergeIntoMaterializeSourceErrorType, MergeIntoMaterializeSourceReason}
import org.apache.spark.sql.delta.commands.merge.MergeIntoMaterializeSource.mergeMaterializedSourceRddBlockLostErrorRegex
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.JsonUtils
import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.{FilterExec, LogicalRDD, RDDScanExec, SQLExecution}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils

trait MergeIntoMaterializeSourceTests
    extends QueryTest
    with SharedSparkSession
    with DeltaSQLCommandTest
    with SQLTestUtils
    with DeltaTestUtilsBase
  {

  import testImplicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    // trigger source materialization in all tests
    spark.conf.set(DeltaSQLConf.MERGE_MATERIALIZE_SOURCE.key, "all")
  }


  // Test error message that we check if blocks of materialized source RDD were evicted.
  test("missing RDD blocks error message") {
    val checkpointedDf = sql("select * from range(10)")
      .localCheckpoint(eager = false)
    val rdd = checkpointedDf.queryExecution.analyzed.asInstanceOf[LogicalRDD].rdd
    checkpointedDf.collect() // trigger lazy materialization
    rdd.unpersist()
    val ex = intercept[Exception] {
      checkpointedDf.collect()
    }
    assert(ex.isInstanceOf[SparkException], ex)
    assert(
      ex.getMessage().matches(mergeMaterializedSourceRddBlockLostErrorRegex(rdd.id)),
      s"RDD id ${rdd.id}: Message: ${ex.getMessage}")
  }


  def getHints(df: => DataFrame): Seq[(Seq[ResolvedHint], JoinHint)] = {
    val plans = withAllPlansCaptured(spark) {
      df
    }
    var plansWithMaterializedSource = 0
    val hints = plans.flatMap { p =>
      val materializedSourceExists = p.analyzed.exists {
        case l: LogicalRDD if l.rdd.name == "mergeMaterializedSource" => true
        case _ => false
      }
      if (materializedSourceExists) {
        // If it is a plan with materialized source, there should be exactly one join
        // of target and source. We collect resolved hints from analyzed plans, and the hint
        // applied to the join from optimized plan.
        plansWithMaterializedSource += 1
        val hints = p.analyzed.collect {
          case h: ResolvedHint => h
        }
        val joinHints = p.optimized.collect {
          case j: Join => j.hint
        }
        assert(joinHints.length == 1, s"Got $joinHints")
        val joinHint = joinHints.head

        // Only preserve join strategy hints, because we are testing with these.
        // Other hints may be added by MERGE internally, e.g. hints to force DFP/DPP, that
        // we don't want to be considering here.
        val retHints = hints
          .filter(_.hints.strategy.nonEmpty)
        def retJoinHintInfo(hintInfo: Option[HintInfo]): Option[HintInfo] = hintInfo match {
          case Some(h) if h.strategy.nonEmpty => Some(HintInfo(strategy = h.strategy))
          case _ => None
        }
        val retJoinHint = joinHint.copy(
          leftHint = retJoinHintInfo(joinHint.leftHint),
          rightHint = retJoinHintInfo(joinHint.rightHint)
        )

        Some((retHints, retJoinHint))
      } else {
        None
      }
    }
    assert(plansWithMaterializedSource == 2,
      s"2 plans should have materialized source, but got: $plans")
    hints
  }

  for (eager <- BOOLEAN_DOMAIN)
  test(s"materialize source preserves dataframe hints - eager=$eager") {
    withTable("A", "B", "T") {
      sql("select id, id as v from range(50000)").write.format("delta").saveAsTable("T")
      sql("select id, id+2 as v from range(10000)").write.format("csv").saveAsTable("A")
      sql("select id, id*2 as v from range(1000)").write.format("csv").saveAsTable("B")

      // Manually added broadcast hint will mess up the expected hints hence disable it
      withSQLConf(
        DeltaSQLConf.MERGE_MATERIALIZE_SOURCE_EAGER.key -> eager.toString,
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
        // Simple BROADCAST hint
        val hSimple = getHints(
          sql("MERGE INTO T USING (SELECT /*+ BROADCAST */ * FROM A) s ON T.id = s.id" +
            " WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
        )
        hSimple.foreach { case (hints, joinHint) =>
          assert(hints.length == 1)
          assert(hints.head.hints == HintInfo(strategy = Some(BROADCAST)))
          assert(joinHint == JoinHint(Some(HintInfo(strategy = Some(BROADCAST))), None))
        }

        // Simple MERGE hint
        val hSimpleMerge = getHints(
          sql("MERGE INTO T USING (SELECT /*+ MERGE */ * FROM A) s ON T.id = s.id" +
            " WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
        )
        hSimpleMerge.foreach { case (hints, joinHint) =>
          assert(hints.length == 1)
          assert(hints.head.hints == HintInfo(strategy = Some(SHUFFLE_MERGE)))
          assert(joinHint == JoinHint(Some(HintInfo(strategy = Some(SHUFFLE_MERGE))), None))
        }

        // Aliased hint
        val hAliased = getHints(
          sql("MERGE INTO T USING " +
            "(SELECT /*+ BROADCAST(FOO) */ * FROM (SELECT * FROM A) FOO) s ON T.id = s.id" +
            " WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
        )
        hAliased.foreach { case (hints, joinHint) =>
          assert(hints.length == 1)
          assert(hints.head.hints == HintInfo(strategy = Some(BROADCAST)))
          assert(joinHint == JoinHint(Some(HintInfo(strategy = Some(BROADCAST))), None))
        }

        // Aliased hint - hint propagation does not work from under an alias
        // (remove if this ever gets implemented in the hint framework)
        val hAliasedInner = getHints(
          sql("MERGE INTO T USING " +
            "(SELECT /*+ BROADCAST(A) */ * FROM (SELECT * FROM A) FOO) s ON T.id = s.id" +
            " WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
        )
        hAliasedInner.foreach { case (hints, joinHint) =>
          assert(hints.length == 0)
          assert(joinHint == JoinHint(None, None))
        }

        // This hint applies to the join inside the source, not to the source as a whole
        val hJoinInner = getHints(
          sql("MERGE INTO T USING " +
            "(SELECT /*+ BROADCAST(A) */ A.* FROM A JOIN B WHERE A.id = B.id) s ON T.id = s.id" +
            " WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
        )
        hJoinInner.foreach { case (hints, joinHint) =>
          assert(hints.length == 0)
          assert(joinHint == JoinHint(None, None))
        }

        // Two hints - top one takes effect
        val hTwo = getHints(
          sql("MERGE INTO T USING (SELECT /*+ BROADCAST, MERGE */ * FROM A) s ON T.id = s.id" +
            " WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
        )
        hTwo.foreach { case (hints, joinHint) =>
          assert(hints.length == 2)
          assert(hints(0).hints == HintInfo(strategy = Some(BROADCAST)))
          assert(hints(1).hints == HintInfo(strategy = Some(SHUFFLE_MERGE)))
          // top one takes effect
          assert(joinHint == JoinHint(Some(HintInfo(strategy = Some(BROADCAST))), None))
        }
      }
    }
  }

  // FIXME: Tests can be removed once Delta adopts Spark 3.4 as constraints and statistics are
  // automatically propagated when materializing
  // The following test should fail as soon as statistics are correctly propagated, and acts as a
  // reminder to remove the manually added filter and broadcast hint once Spark 3.4 is adopted
  test("Source in materialized merge has missing stats") {
    // AQE has to be disabled as we might not find the Join in the adaptive plan
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withTable("A", "T") {
        sql("select id, id as v from range(50)").write.format("delta").saveAsTable("T")
        sql("select id, id+2 as v from range(10)").write.format("csv").saveAsTable("A")
        val plans = DeltaTestUtils.withAllPlansCaptured(spark) {
          sql("MERGE INTO T USING A as s ON T.id = s.id" +
            " WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
        }
        plans.map(_.optimized).foreach { p =>
          p.foreach {
            case j: Join =>
              // The source is very small, the only way we'd be above the broadcast join threshold
              // is if we lost statistics on the size of the source.
              val sourceStats = j.left.stats.sizeInBytes
              val broadcastJoinThreshold = spark.conf.get(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD)
              assert(sourceStats >= broadcastJoinThreshold)
            case _ =>
          }
        }
      }
    }
  }

  test("Filter gets added if there is a constraint") {
    // AQE has to be disabled as we might not find the filter in the adaptive plan
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withTable("A", "T") {
        spark.range(50).toDF("tgtid").write.format("delta").saveAsTable("T")
        spark.range(50).toDF("srcid").write.format("delta").saveAsTable("A")

        val plans = DeltaTestUtils.withAllPlansCaptured(spark) {
          sql("MERGE INTO T USING (SELECT * FROM A WHERE srcid = 10) as s ON T.tgtid = s.srcid" +
            " WHEN MATCHED THEN UPDATE SET tgtid = s.srcid" +
            " WHEN NOT MATCHED THEN INSERT (tgtid) values (s.srcid)")
        }
        // Check whether the executed plan contains a filter that filters by tgtId that could be
        // used to infer constraints  lost during materialization
        val hastgtIdCondition = (condition: Expression) => {
          condition.find {
            case EqualTo(AttributeReference("tgtid", _, _, _), Literal(10, _)) => true
            case _ => false
          }.isDefined
        }
        val touchedFilesPlan = getfindTouchedFilesJobPlans(plans)
        val filter = touchedFilesPlan.find {
          case f: FilterExec => hastgtIdCondition(f.condition)
          case _ => false
        }
        assert(filter.isDefined,
          s"Didn't find Filter on tgtid=10 in touched files plan:\n$touchedFilesPlan")
      }
    }
  }

  test("Broadcast hint gets added when there is a small source table") {
    withTable("A", "T") {
      sql("select id, id as v from range(50000)").write.format("delta").saveAsTable("T")
      sql("select id, id+2 as v from range(10000)").write.format("csv").saveAsTable("A")
      val hints = getHints(
        sql("MERGE INTO T USING A as s ON T.id = s.id" +
          " WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
      )
      hints.foreach { case (hints, joinHint) =>
        assert(hints.length == 1)
        assert(hints.head.hints == HintInfo(strategy = Some(BROADCAST)))
        assert(joinHint == JoinHint(Some(HintInfo(strategy = Some(BROADCAST))), None))
      }
    }
  }

  test("Broadcast hint does not get added when there is a large table") {
    withTable("A", "T") {
      sql("select id, id as v from range(50000)").write.format("delta").saveAsTable("T")
      sql("select id, id+2 as v from range(10000)").write.format("csv").saveAsTable("A")
      withSQLConf((SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "1KB")) {
        val hints = getHints(
          sql("MERGE INTO T USING A as s ON T.id = s.id" +
            " WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
        )
        hints.foreach { case (hints, joinHint) =>
          assert(hints.length == 0)
          assert(joinHint == JoinHint(None, None))
        }
      }
    }
  }
}

// MERGE + materialize
class MergeIntoMaterializeSourceSuite extends MergeIntoMaterializeSourceTests

