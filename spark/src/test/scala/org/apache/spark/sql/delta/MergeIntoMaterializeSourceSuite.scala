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
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import com.databricks.spark.util.{Log4jUsageLogger, MetricDefinitions, UsageRecord}
import org.apache.spark.sql.delta.DeltaTestUtils._
import org.apache.spark.sql.delta.commands.merge.{MergeIntoMaterializeSourceError, MergeIntoMaterializeSourceErrorType, MergeIntoMaterializeSourceReason, MergeStats}
import org.apache.spark.sql.delta.commands.merge.MergeIntoMaterializeSource.mergeMaterializedSourceRddBlockLostErrorRegex
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaSQLTestUtils
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
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils

trait MergeIntoMaterializeSourceTests
    extends QueryTest
    with SharedSparkSession
    with DeltaSQLCommandTest
    with DeltaSQLTestUtils
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



  for {
    eager <- BOOLEAN_DOMAIN
    materialized <- BOOLEAN_DOMAIN
  }  test(s"merge logs out of disk errors - eager=$eager, materialized=$materialized") {
    import DeltaSQLConf.MergeMaterializeSource
    withSQLConf(
        DeltaSQLConf.MERGE_MATERIALIZE_SOURCE_EAGER.key -> eager.toString,
        DeltaSQLConf.MERGE_MATERIALIZE_SOURCE.key ->
          (if (materialized) MergeMaterializeSource.AUTO else MergeMaterializeSource.NONE)) {
      val injectEx = new java.io.IOException("No space left on device")
      testWithCustomErrorInjected[SparkException](injectEx) { (thrownEx, errorOpt) =>
        // Compare messages instead of instances, since the equals method for these exceptions
        // takes more into account.
        assert(thrownEx.getCause.getMessage === injectEx.getMessage)
        if (materialized) {
          assert(errorOpt.isDefined)
          val error = errorOpt.get
          assert(error.errorType == MergeIntoMaterializeSourceErrorType.OUT_OF_DISK.toString)
          assert(error.attempt == 1)
          val storageLevel = StorageLevel.fromString(
              spark.conf.get(DeltaSQLConf.MERGE_MATERIALIZE_SOURCE_RDD_STORAGE_LEVEL))
          assert(error.materializedSourceRDDStorageLevel == storageLevel.toString)
        } else {
          assert(errorOpt.isEmpty)
        }
      }
    }
  }

  test("merge rethrows arbitrary errors") {
    val injectEx = new RuntimeException("test")
    testWithCustomErrorInjected[SparkException](injectEx) { (thrownEx, error) =>
      // Compare messages instead of instances, since the equals method for these exceptions
      // takes more into account.
      assert(thrownEx.getCause.getMessage === injectEx.getMessage)
      assert(error.isEmpty)
    }
  }

  private def testWithCustomErrorInjected[Intercept >: Null <: Exception with AnyRef : ClassTag](
      inject: Exception)(
      handle: (Intercept, Option[MergeIntoMaterializeSourceError]) => Unit): Unit = {
    {
      val tblName = "target"
      withTable(tblName) {
        val targetDF = spark.range(10).toDF("id").withColumn("value", rand())
        targetDF.write.format("delta").saveAsTable(tblName)
        spark
          .range(10)
          .mapPartitions { x =>
            throw inject
            x
          }
          .toDF("id")
          .withColumn("value", rand())
          .createOrReplaceTempView("s")
        var thrownException: Intercept = null
        val events = Log4jUsageLogger
          .track {
            thrownException = intercept[Intercept] {
              sql(s"MERGE INTO $tblName t USING s ON t.id = s.id " +
                s"WHEN MATCHED THEN DELETE WHEN NOT MATCHED THEN INSERT *")
            }
          }
          .filter { e =>
            e.metric == MetricDefinitions.EVENT_TAHOE.name &&
            e.tags.get("opType").contains(MergeIntoMaterializeSourceError.OP_TYPE)
          }
        val error = events.headOption
          .map(e => JsonUtils.fromJson[MergeIntoMaterializeSourceError](e.blob))
        handle(thrownException, error)
      }
    }
  }

  // Runs a merge query with source materialization, while a killer thread tries to unpersist it.
  private def testMergeMaterializedSourceUnpersist(
      tblName: String, numKills: Int): Seq[UsageRecord] = {
    val maxAttempts = spark.conf.get(DeltaSQLConf.MERGE_MATERIALIZE_SOURCE_MAX_ATTEMPTS)

    // when we ask to join the killer thread, it should exit in the next iteration.
    val killerThreadJoinTimeoutMs = 10000
    // sleep between attempts to unpersist
    val killerIntervalMs = 1

    // Data does not need to be big; there is enough latency to unpersist even with small data.
    val targetDF = spark.range(100).toDF("id")
    targetDF.write.format("delta").saveAsTable(tblName)
    spark.range(90, 120).toDF("id").createOrReplaceTempView("s")
    val mergeQuery =
      s"MERGE INTO $tblName t USING s ON t.id = s.id " +
      "WHEN MATCHED THEN DELETE WHEN NOT MATCHED THEN INSERT *"

    // Killer thread tries to unpersist any persisted mergeMaterializedSource RDDs,
    // until it has seen more than numKills distinct ones (from distinct Merge retries)
    @volatile var finished = false
    @volatile var invalidStorageLevel: Option[String] = None
    val killerThread = new Thread() {
      override def run(): Unit = {
        val seenSources = mutable.Set[Int]()
        while (!finished) {
          sparkContext.getPersistentRDDs.foreach { case (rddId, rdd) =>
            if (rdd.name == "mergeMaterializedSource") {
              if (!seenSources.contains(rddId)) {
                logInfo(s"First time seeing mergeMaterializedSource with id=$rddId")
                seenSources.add(rddId)
              }
              if (seenSources.size > numKills) {
                // already unpersisted numKills different source materialization attempts,
                // the killer can retire
                logInfo(s"seenSources.size=${seenSources.size}. Proceeding to finish.")
                finished = true
              } else {
                // Need to wait until it is actually checkpointed, otherwise if we try to unpersist
                // before it starts to actually persist it fails with
                // java.lang.AssertionError: assumption failed:
                // Storage level StorageLevel(1 replicas) is not appropriate for local checkpointing
                // (this wouldn't happen in real world scenario of losing the block because executor
                // was lost; there nobody manipulates with StorageLevel; if failure happens during
                // computation of the materialized rdd, the task would be reattempted using the
                // regular task retry mechanism)
                if (rdd.isCheckpointed) {
                  // Use this opportunity to test if the source has the correct StorageLevel.
                  val expectedStorageLevel = StorageLevel.fromString(
                    if (seenSources.size == 1) {
                      spark.conf.get(DeltaSQLConf.MERGE_MATERIALIZE_SOURCE_RDD_STORAGE_LEVEL)
                    } else if (seenSources.size == 2) {
                      spark.conf.get(
                        DeltaSQLConf.MERGE_MATERIALIZE_SOURCE_RDD_STORAGE_LEVEL_FIRST_RETRY)
                    } else {
                      spark.conf.get(DeltaSQLConf.MERGE_MATERIALIZE_SOURCE_RDD_STORAGE_LEVEL_RETRY)
                    }
                  )
                  val rddStorageLevel = rdd.getStorageLevel
                  if (rddStorageLevel != expectedStorageLevel) {
                    invalidStorageLevel =
                      Some(s"For attempt ${seenSources.size} of materialized source expected " +
                        s"$expectedStorageLevel but got ${rddStorageLevel}")
                    finished = true
                  }
                  logInfo(s"Unpersisting mergeMaterializedSource with id=$rddId")
                  // don't make it blocking, so that the killer turns around quickly and is ready
                  // for the next kill when Merge retries
                  rdd.unpersist(blocking = false)
                }
              }
            }
          }
          Thread.sleep(killerIntervalMs)
        }
        logInfo(s"seenSources.size=${seenSources.size}. Proceeding to finish.")
      }
    }
    killerThread.start()

    val events = Log4jUsageLogger.track {
      try {
        sql(mergeQuery)
      } catch {
        case NonFatal(ex) =>
          if (numKills < maxAttempts) {
            // The merge should succeed with retries
            throw ex
          }
      } finally {
        finished = true // put the killer to rest, if it didn't retire already
        killerThread.join(killerThreadJoinTimeoutMs)
        assert(!killerThread.isAlive)
      }
    }.filter(_.metric == MetricDefinitions.EVENT_TAHOE.name)

    // If killer thread recorded an invalid StorageLevel, throw it here
    assert(invalidStorageLevel.isEmpty, invalidStorageLevel.toString)

    events
  }

  private def testMergeMaterializeSourceUnpersistRetries = {
    val maxAttempts = DeltaSQLConf.MERGE_MATERIALIZE_SOURCE_MAX_ATTEMPTS.defaultValue.get
    val tblName = "target"

    // For 1 to maxAttempts - 1 RDD block lost failures, merge should retry and succeed.
    for {
      eager <- BOOLEAN_DOMAIN
      kills <- 1 to maxAttempts - 1
    } {
      test(s"materialize source unpersist with $kills kill attempts succeeds - eager=$eager") {
        withTable(tblName) {
          withSQLConf(DeltaSQLConf.MERGE_MATERIALIZE_SOURCE_EAGER.key -> eager.toString) {
            val allDeltaEvents = testMergeMaterializedSourceUnpersist(tblName, kills)
            val events =
              allDeltaEvents.filter(_.tags.get("opType").contains("delta.dml.merge.stats"))
            assert(events.length == 1, s"allDeltaEvents:\n$allDeltaEvents")
            val mergeStats = JsonUtils.fromJson[MergeStats](events(0).blob)
            assert(mergeStats.materializeSourceAttempts.isDefined, s"MergeStats:\n$mergeStats")
            assert(
              mergeStats.materializeSourceAttempts.get == kills + 1,
              s"MergeStats:\n$mergeStats")

            // Check query result after merge
            val tab = sql(s"select * from $tblName order by id")
              .collect()
              .map(row => row.getLong(0))
              .toSeq
            assert(tab == (0L until 90L) ++ (100L until 120L))
          }
        }
      }
    }

    // Eventually it should fail after exceeding maximum number of attempts.
    for (eager <- BOOLEAN_DOMAIN) {
      test(s"materialize source unpersist with $maxAttempts kill attempts fails - eager=$eager") {
        withSQLConf(DeltaSQLConf.MERGE_MATERIALIZE_SOURCE_EAGER.key -> eager.toString) {
          withTable(tblName) {
            val allDeltaEvents = testMergeMaterializedSourceUnpersist(tblName, maxAttempts)
            val events = allDeltaEvents
              .filter(_.tags.get("opType").contains(MergeIntoMaterializeSourceError.OP_TYPE))
            assert(events.length == 1, s"allDeltaEvents:\n$allDeltaEvents")
            val error = JsonUtils.fromJson[MergeIntoMaterializeSourceError](events(0).blob)
            assert(error.errorType == MergeIntoMaterializeSourceErrorType.RDD_BLOCK_LOST.toString)
            assert(error.attempt == maxAttempts)
          }
        }
      }
    }
  }
  testMergeMaterializeSourceUnpersistRetries

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

  test("materialize source for non-deterministic source formats") {
    val targetSchema = StructType(Array(
      StructField("id", IntegerType, nullable = false),
      StructField("value", StringType, nullable = true)))
    val targetData = Seq(
      Row(1, "update"),
      Row(2, "skip"),
      Row(3, "delete"))
    val sourceData = Seq(1, 3, 4).toDF("id")
    val expectedResult = Seq(
      Row(1, "new"), // Updated
      Row(2, "skip"), // Copied
      // 3 is deleted
      Row(4, "new")) // Inserted

    // There are more, but these are easiest to test for.
    val nonDeterministicFormats = List("parquet", "json")

    // Return MergeIntoMaterializeSourceReason string
    def executeMerge(sourceDf: DataFrame): String = {
      val sourceDfWithAction = sourceDf.withColumn("value", lit("new"))
      var materializedSource: String = ""
      withTable("target") {
        val targetRdd = spark.sparkContext.parallelize(targetData)
        val targetDf = spark.createDataFrame(targetRdd, targetSchema)
        targetDf.write.format("delta").mode("overwrite").saveAsTable("target")
        val targetTable = io.delta.tables.DeltaTable.forName("target")

        val events: Seq[UsageRecord] = Log4jUsageLogger.track {
          targetTable.merge(sourceDfWithAction, col("target.id") === sourceDfWithAction("id"))
            .whenMatched(col("target.value") === lit("update")).updateAll()
            .whenMatched(col("target.value") === lit("delete")).delete()
            .whenNotMatched().insertAll()
            .execute()
        }

        // Can't return values out of withTable.
        materializedSource = mergeSourceMaterializeReason(events)

        checkAnswer(
          spark.read.format("delta").table("target"),
          expectedResult)
      }
      materializedSource
    }

    def checkSourceMaterialization(
        format: String,
        reason: String): Unit = {
      // Test once by name and once using path, as they produce different plans.
      withTable("source") {
        sourceData.write.format(format).saveAsTable("source")
        val sourceDf = spark.read.format(format).table("source")
        assert(executeMerge(sourceDf) == reason, s"Wrong materialization reason for $format")
      }

      withTempPath { sourcePath =>
        sourceData.write.format(format).save(sourcePath.toString)
        val sourceDf = spark.read.format(format).load(sourcePath.toString)
        assert(executeMerge(sourceDf) == reason, s"Wrong materialization reason for $format")
      }
    }

    withSQLConf(DeltaSQLConf.MERGE_MATERIALIZE_SOURCE.key -> "auto") {
      for (format <- nonDeterministicFormats) {
        checkSourceMaterialization(
          format,
          reason = MergeIntoMaterializeSourceReason.NON_DETERMINISTIC_SOURCE_NON_DELTA.toString)
      }

      // Delta should not materialize source.
      checkSourceMaterialization(
        "delta", reason = MergeIntoMaterializeSourceReason.NOT_MATERIALIZED_AUTO.toString)
    }

    // Test with non-Delta sources in subqueries.
    def checkSourceMaterializationSubquery(
        delta: String,
        filterSub: String,
        projectSub: String,
        nestedFilterSub: String,
        nestedProjectSub: String,
        testType: String,
        reason: String): Unit = {
      val df = spark.sql(
        s"""
           |SELECT
           |  CASE WHEN id IN
           |    (SELECT id kk FROM $projectSub WHERE id IN (SELECT * FROM $nestedFilterSub))
           |  THEN id ELSE -1 END AS id,
           |  0.5 AS value
           |FROM $delta
           |WHERE id IN
           |  (SELECT CASE WHEN id IN (SELECT * FROM $nestedProjectSub) THEN id ELSE -1 END kk
           |   FROM $filterSub)
           |""".stripMargin)
      assert(executeMerge(df) == reason, s"Wrong materialization reason with $testType subquery")
    }

    def checkSourceMaterializationSubqueries(deltaSource: String, nonDeltaSource: String): Unit = {
      checkSourceMaterializationSubquery(
        delta = deltaSource,
        filterSub = deltaSource,
        projectSub = deltaSource,
        nestedFilterSub = deltaSource,
        nestedProjectSub = deltaSource,
        testType = "all Delta",
        reason = MergeIntoMaterializeSourceReason.NOT_MATERIALIZED_AUTO.toString)

      checkSourceMaterializationSubquery(
        delta = deltaSource,
        filterSub = nonDeltaSource,
        projectSub = deltaSource,
        nestedFilterSub = deltaSource,
        nestedProjectSub = deltaSource,
        testType = "non-Delta filter",
        reason = MergeIntoMaterializeSourceReason.NON_DETERMINISTIC_SOURCE_NON_DELTA.toString)

      checkSourceMaterializationSubquery(
        delta = deltaSource,
        filterSub = deltaSource,
        projectSub = nonDeltaSource,
        nestedFilterSub = deltaSource,
        nestedProjectSub = deltaSource,
        testType = "non-Delta project",
        reason = MergeIntoMaterializeSourceReason.NON_DETERMINISTIC_SOURCE_NON_DELTA.toString)

      checkSourceMaterializationSubquery(
        delta = deltaSource,
        filterSub = deltaSource,
        projectSub = deltaSource,
        nestedFilterSub = nonDeltaSource,
        nestedProjectSub = deltaSource,
        testType = "non-Delta nested filter",
        reason = MergeIntoMaterializeSourceReason.NON_DETERMINISTIC_SOURCE_NON_DELTA.toString)

      checkSourceMaterializationSubquery(
        delta = deltaSource,
        filterSub = deltaSource,
        projectSub = deltaSource,
        nestedFilterSub = deltaSource,
        nestedProjectSub = nonDeltaSource,
        testType = "non-Delta nested project",
        reason = MergeIntoMaterializeSourceReason.NON_DETERMINISTIC_SOURCE_NON_DELTA.toString)
    }

    withSQLConf(DeltaSQLConf.MERGE_MATERIALIZE_SOURCE.key -> "auto") {
      // Test once by name and once using path, as they produce different plans.
      withTable("deltaSource", "nonDeltaSource") {
        sourceData.write.format("delta").saveAsTable("deltaSource")
        sourceData.write.format("parquet").saveAsTable("nonDeltaSource")
        checkSourceMaterializationSubqueries("deltaSource", "nonDeltaSource")
      }

      withTempPath { deltaSourcePath =>
        sourceData.write.format("delta").save(deltaSourcePath.toString)
        withTempPath { nonDeltaSourcePath =>
          sourceData.write.format("parquet").save(nonDeltaSourcePath.toString)
          checkSourceMaterializationSubqueries(
            s"delta.`$deltaSourcePath`", s"parquet.`$nonDeltaSourcePath`")
        }
      }
    }

    // Mixed safe/unsafe queries should materialize source.
    def checkSourceMaterializationForMixedSources(
        format1: String,
        format2: String,
        shouldMaterializeSource: Boolean): Unit = {

      def checkWithSources(source1Df: DataFrame, source2Df: DataFrame): Unit = {
        val sourceDf = source1Df.union(source2Df)
        val materializeReason = executeMerge(sourceDf)
        if (shouldMaterializeSource) {
          assert(materializeReason ==
            MergeIntoMaterializeSourceReason.NON_DETERMINISTIC_SOURCE_NON_DELTA.toString,
            s"$format1 union $format2 are not deterministic as a source and should materialize.")
        } else {
          assert(materializeReason ==
            MergeIntoMaterializeSourceReason.NOT_MATERIALIZED_AUTO.toString,
            s"$format1 union $format2 is deterministic as a source and should not materialize.")
        }
      }

      // Test once by name and once using path, as they produce different plans.
      withTable("source1", "source2") {
        sourceData.filter(col("id") < 2).write.format(format1).saveAsTable("source1")
        val source1Df = spark.read.format(format1).table("source1")
        sourceData.filter(col("id") >= 2).write.format(format2).saveAsTable("source2")
        val source2Df = spark.read.format(format2).table("source2")
        checkWithSources(source1Df, source2Df)
      }

      withTempPaths(2) { case Seq(source1, source2) =>
        sourceData.filter(col("id") < 2).write
          .mode("overwrite").format(format1).save(source1.toString)
        val source1Df = spark.read.format(format1).load(source1.toString)
        sourceData.filter(col("id") >= 2).write
          .mode("overwrite").format(format2).save(source2.toString)
        val source2Df = spark.read.format(format2).load(source2.toString)
        checkWithSources(source1Df, source2Df)
      }
    }

    withSQLConf(DeltaSQLConf.MERGE_MATERIALIZE_SOURCE.key -> "auto") {
      val allFormats = "delta" :: nonDeterministicFormats
      // Try all combinations
      for {
        format1 <- allFormats
        format2 <- allFormats
      } checkSourceMaterializationForMixedSources(
        format1 = format1,
        format2 = format2,
        shouldMaterializeSource = !(format1 == "delta" && format2 == "delta"))
    }

    withSQLConf(DeltaSQLConf.MERGE_MATERIALIZE_SOURCE.key -> "none") {
      // With "none", it should not materialize, even though parquet is non-deterministic.
      checkSourceMaterialization(
        "parquet",
        reason = MergeIntoMaterializeSourceReason.NOT_MATERIALIZED_NONE.toString)
    }

    withSQLConf(DeltaSQLConf.MERGE_MATERIALIZE_SOURCE.key -> "all") {
      // With "all"", it should materialize, even though Delta is deterministic.
      checkSourceMaterialization(
        "delta",
        reason = MergeIntoMaterializeSourceReason.MATERIALIZE_ALL.toString)
    }
  }

  test("materialize source for non-deterministic source queries - udf") {
    {
      val targetSchema = StructType(Array(
        StructField("id", IntegerType, nullable = false),
        StructField("value", IntegerType, nullable = true)))
      val targetData = Seq(
        Row(1, 0),
        Row(2, 0),
        Row(3, 0))
      val sourceData = Seq(1, 3).toDF("id")
      withSQLConf(DeltaSQLConf.MERGE_MATERIALIZE_SOURCE.key -> "auto") {
        withTable("target", "source") {
          val targetRdd = spark.sparkContext.parallelize(targetData)
          val targetDf = spark.createDataFrame(targetRdd, targetSchema)
          targetDf.write.format("delta").mode("overwrite").saveAsTable("target")
          val targetTable = io.delta.tables.DeltaTable.forName("target")

          sourceData.write.format("delta").mode("overwrite").saveAsTable("source")
          val f = udf { () => 1L }
          val sourceDf = spark.table("source").withColumn("value", f())

          val events: Seq[UsageRecord] = Log4jUsageLogger.track {
            targetTable
              .merge(sourceDf, col("target.id") === sourceDf("id"))
              .whenMatched(col("target.value") > sourceDf("value")).delete()
              .whenMatched().updateAll()
              .whenNotMatched().insertAll()
              .execute()
          }

          val materializeReason = mergeSourceMaterializeReason(events)
          assert(materializeReason == MergeIntoMaterializeSourceReason.
            NON_DETERMINISTIC_SOURCE_WITH_DETERMINISTIC_UDF.toString,
            "Source has a udf and merge should have materialized the source.")
        }
      }
    }
  }

  test("materialize source for non-deterministic source queries - rand expr") {
    val targetSchema = StructType(Array(
      StructField("id", IntegerType, nullable = false),
      StructField("value", FloatType, nullable = true)))
    val targetData = Seq(
      Row(1, 0.5f),
      Row(2, 0.3f),
      Row(3, 0.8f))
    val sourceData = Seq(1, 3).toDF("id")
    withSQLConf(DeltaSQLConf.MERGE_MATERIALIZE_SOURCE.key -> "auto") {

      def executeMerge(sourceDf: DataFrame): Unit = {
        val nonDeterministicSourceDf = sourceDf.withColumn("value", rand())
        withTable("target") {
          val targetRdd = spark.sparkContext.parallelize(targetData)
          val targetDf = spark.createDataFrame(targetRdd, targetSchema)
          targetDf.write.format("delta").mode("overwrite").saveAsTable("target")
          val targetTable = io.delta.tables.DeltaTable.forName("target")

          val events: Seq[UsageRecord] = Log4jUsageLogger.track {
            targetTable
              .merge(nonDeterministicSourceDf, col("target.id") === nonDeterministicSourceDf("id"))
              .whenMatched(col("target.value") > nonDeterministicSourceDf("value")).delete()
              .whenMatched().updateAll()
              .whenNotMatched().insertAll()
              .execute()
          }

          val materializeReason = mergeSourceMaterializeReason(events)
          assert(materializeReason ==
              MergeIntoMaterializeSourceReason.NON_DETERMINISTIC_SOURCE_OPERATORS.toString,
            "Source has non deterministic operations and should have materialized source.")
        }
      }

      // Test once by name and once using path, as they produce different plans.
      withTable("source") {
        sourceData.write.format("delta").saveAsTable("source")
        val sourceDf = spark.read.format("delta").table("source")
        executeMerge(sourceDf)
      }

      withTempPath { sourcePath =>
        sourceData.write.format("delta").save(sourcePath.toString)
        val sourceDf = spark.read.format("delta").load(sourcePath.toString)
        executeMerge(sourceDf)
      }
    }
  }

  test("don't materialize source for deterministic source queries with current_date") {
    val targetSchema = StructType(Array(
      StructField("id", IntegerType, nullable = false),
      StructField("date", DateType, nullable = true)))
    val targetData = Seq(
      Row(1, java.sql.Date.valueOf("2022-01-01")),
      Row(2, java.sql.Date.valueOf("2022-02-01")),
      Row(3, java.sql.Date.valueOf("2022-03-01")))
    val sourceData = Seq(1, 3).toDF("id")
    withSQLConf(DeltaSQLConf.MERGE_MATERIALIZE_SOURCE.key -> "auto") {

      def executeMerge(sourceDf: DataFrame): Unit = {
        val nonDeterministicSourceDf = sourceDf.withColumn("date", current_date())
        withTable("target") {
          val targetRdd = spark.sparkContext.parallelize(targetData)
          val targetDf = spark.createDataFrame(targetRdd, targetSchema)
          targetDf.write.format("delta").mode("overwrite").saveAsTable("target")
          val targetTable = io.delta.tables.DeltaTable.forName("target")

          val events: Seq[UsageRecord] = Log4jUsageLogger.track {
            targetTable
              .merge(nonDeterministicSourceDf, col("target.id") === nonDeterministicSourceDf("id"))
              .whenMatched(col("target.date") < nonDeterministicSourceDf("date")).delete()
              .whenMatched().updateAll()
              .whenNotMatched().insertAll()
              .execute()
          }

          val materializeReason = mergeSourceMaterializeReason(events)
          assert(materializeReason ==
            MergeIntoMaterializeSourceReason.NOT_MATERIALIZED_AUTO.toString,
            "Source query is deterministic and should not be materialized.")
        }
      }

      // Test once by name and once using path, as they produce different plans.
      withTable("source") {
        sourceData.write.format("delta").saveAsTable("source")
        val sourceDf = spark.read.format("delta").table("source")
        executeMerge(sourceDf)
      }

      withTempPath { sourcePath =>
        sourceData.write.format("delta").save(sourcePath.toString)
        val sourceDf = spark.read.format("delta").load(sourcePath.toString)
        executeMerge(sourceDf)
      }
    }
  }

  test("materialize source for non-deterministic source queries - subquery") {
    val sourceDataFrame = spark.range(0, 10)
      .toDF("id")
      .withColumn("value", rand())

    val targetDataFrame = spark.range(0, 5)
      .toDF("id")
      .withColumn("value", rand())

    withSQLConf(DeltaSQLConf.MERGE_MATERIALIZE_SOURCE.key -> "auto") {

      // Return MergeIntoMaterializeSourceReason
      def executeMerge(sourceDf: DataFrame, clue: String): Unit = {
        withTable("target") {
          targetDataFrame.write
            .format("delta")
            .saveAsTable("target")
          val targetTable = io.delta.tables.DeltaTable.forName("target")

          val events: Seq[UsageRecord] = Log4jUsageLogger.track {
            targetTable.merge(sourceDf, col("target.id") === sourceDf("id"))
              .whenMatched(col("target.value") > sourceDf("value")).delete()
              .whenMatched().updateAll()
              .whenNotMatched().insertAll()
              .execute()
          }

          val materializeReason = mergeSourceMaterializeReason(events)
          assert(materializeReason ==
            MergeIntoMaterializeSourceReason.NON_DETERMINISTIC_SOURCE_OPERATORS.toString,
            s"Source query has non deterministic subqueries and should materialize ($clue).")
        }
      }

      def checkSubquery(from: String, subquery: String): Unit = {
        // check subquery in filter
        val sourceDfFilterSubquery = spark.sql(
          s"""
             |SELECT id, 0.5 AS value
             |FROM $from WHERE id IN ($subquery)
             |""".stripMargin)
        executeMerge(sourceDfFilterSubquery,
          s"reading from `$from`, subquery `$subquery` in filter")

        // check subquery in project
        val sourceDfProjectSubquery = spark.sql(
          s"""
             |SELECT CASE WHEN id IN ($subquery) THEN id ELSE -1 END AS id, 0.5 AS value
             |FROM $from
             |""".stripMargin)
        executeMerge(sourceDfProjectSubquery,
          s"reading from `$from`, subquery `$subquery` in project")
      }

      def checkSubqueries(from: String): Unit = {
        // check non-deterministic plan
        checkSubquery(from, s"SELECT id FROM $from WHERE id < rand() * 10")

        // check too complex plan in subquery, even though plan.deterministic is true
        val subqueryComplex = s"SELECT A.id kk FROM $from A JOIN $from B ON A.id = B.id"
        assert(spark.sql(subqueryComplex).queryExecution.analyzed.deterministic,
          "We want the subquery plan to be deterministic for this test.")
        checkSubquery(from, subqueryComplex)

        // check nested subquery
        val subqueryNestedFilter = s"SELECT id AS kk FROM $from WHERE id IN ($subqueryComplex)"
        checkSubquery(from, subqueryNestedFilter)
        val subqueryNestedProject =
          s"SELECT CASE WHEN id IN ($subqueryComplex) THEN id ELSE -1 END AS kk FROM $from"
        checkSubquery(from, subqueryNestedProject)

        // check correlated subquery
        val subqueryCorrelated = s"SELECT kk FROM (SELECT id AS kk from $from) WHERE kk = id"
        checkSubquery(from, subqueryCorrelated)
      }

      // Test once by name and once using path, as they produce different plans.
      withTable("source") {
        sourceDataFrame.write.format("delta").saveAsTable("source")
        checkSubqueries("source")
      }

      withTempPath { sourcePath =>
        sourceDataFrame.write.format("delta").save(sourcePath.toString)
        checkSubqueries(s"delta.`${sourcePath.toString}`")
      }
    }
  }

  test("don't materialize insert only merge") {
    val tblName = "mergeTarget"
    withTable(tblName) {
      val targetDF = spark.range(100).toDF("id")
      targetDF.write.format("delta").saveAsTable(tblName)
      spark.range(90, 120).toDF("id").createOrReplaceTempView("s")
      val mergeQuery =
        s"MERGE INTO $tblName t USING s ON t.id = s.id WHEN NOT MATCHED THEN INSERT *"
      val events: Seq[UsageRecord] = Log4jUsageLogger.track {
        withSQLConf(DeltaSQLConf.MERGE_MATERIALIZE_SOURCE.key -> "auto") {
          sql(mergeQuery)
        }
      }

      assert(mergeSourceMaterializeReason(events) ==
        MergeIntoMaterializeSourceReason.NOT_MATERIALIZED_AUTO_INSERT_ONLY.toString)

      checkAnswer(
        spark.read.format("delta").table(tblName),
        (0 until 120).map(i => Row(i.toLong)))
    }
  }

  test("don't unpersist locally checkpointed RDDs") {
    val tblName = "mergeTarget"

    withTable(tblName) {
      val targetDF = Seq(
        ("2023-01-01", "trade1", 100.0, "buy", "user1", "2023-01-01 10:00:00"),
        ("2023-01-02", "trade2", 200.0, "sell", "user2", "2023-01-02 11:00:00")
      ).toDF("block_date", "unique_trade_id", "transaction_amount", "transaction_type",
        "user_id", "timestamp")
      targetDF.write.format("delta").saveAsTable(tblName)

      Seq(
        ("2023-01-01", "trade1", 150.0, "buy", "user1_updated", "2023-01-01 12:00:00"),
        ("2023-01-03", "trade3", 300.0, "buy", "user3", "2023-01-03 10:00:00")
      ).toDF("block_date", "unique_trade_id", "transaction_amount", "transaction_type",
        "user_id", "timestamp").createOrReplaceTempView("s")

      val mergeQuery =
        s"""MERGE INTO $tblName t USING s
           |ON t.block_date = s.block_date AND t.unique_trade_id = s.unique_trade_id
           |WHEN MATCHED THEN UPDATE SET *
           |WHEN NOT MATCHED THEN INSERT *""".stripMargin

      Log4jUsageLogger.track {
        withSQLConf(DeltaSQLConf.MERGE_MATERIALIZE_SOURCE.key -> "auto") {
          sql(mergeQuery)
        }
      }

      // Check if the source RDDs have been locally checkpointed and not unpersisted
      assert(sparkContext.getPersistentRDDs.values.nonEmpty, "Source RDDs" +
        " should be locally checkpointed")

      checkAnswer(
        spark.read.format("delta").table(tblName),
        Seq(
          Row("2023-01-01", "trade1", 150.0, "buy", "user1_updated", "2023-01-01 12:00:00"),
          Row("2023-01-02", "trade2", 200.0, "sell", "user2", "2023-01-02 11:00:00"),
          Row("2023-01-03", "trade3", 300.0, "buy", "user3", "2023-01-03 10:00:00"))
      )
    }
  }

  private def mergeStats(events: Seq[UsageRecord]): MergeStats = {
    val mergeStats = events.filter { e =>
      e.metric == MetricDefinitions.EVENT_TAHOE.name &&
        e.tags.get("opType").contains("delta.dml.merge.stats")
    }
    assert(mergeStats.size == 1)
    JsonUtils.fromJson[MergeStats](mergeStats.head.blob)
  }

  private def mergeSourceMaterializeReason(events: Seq[UsageRecord]): String = {
    val stats = mergeStats(events)
    assert(stats.materializeSourceReason.isDefined)
    stats.materializeSourceReason.get
  }
}

// MERGE + materialize
class MergeIntoMaterializeSourceSuite extends MergeIntoMaterializeSourceTests
{
  override protected def sparkConf: SparkConf = super.sparkConf
    .set(DeltaSQLConf.MERGE_USE_PERSISTENT_DELETION_VECTORS.key, "false")
}

