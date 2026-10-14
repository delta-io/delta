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

import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils
import org.apache.spark.sql.delta.coordinatedcommits.CatalogOwnedTestBaseSuite
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.cic.{
  CreateSequenceRequest,
  DropSequenceRequest,
  LocalIdentitySequenceService,
  ReserveIdsRequest,
  ReserveIdsResponse
}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession

trait ConcurrentIdentityColumnSuiteBase extends QueryTest
  with SharedSparkSession
  with DeltaTestUtilsForTempViews
  with DeltaDMLTestUtilsPathBased
  with MergeIntoSQLTestUtils
  with DeltaSQLCommandTest
  with CatalogOwnedTestBaseSuite {

  // CIC requires a catalog-managed (UC coordinated-commits) table. Therefore we rely on the
  // `CatalogOwnedTestBaseSuite`. It sets the feature conf and registers an in-memory commit
  // coordinator. The `catalogOwnedCoordinatorBackfillBatchSize` needs to be non-zero, because it
  // toggles whether or not to create tables as catalog-managed by default.
  override def catalogOwnedCoordinatorBackfillBatchSize: Option[Int] = Some(100)

  override protected def sparkConf: SparkConf = super.sparkConf
    // Ensure the feature is enabled for the CIC suites regardless of its default.
    .set(DeltaSQLConf.CONCURRENT_IDENTITY_COLUMN_ENABLED.key, "true")
    .set(
      DeltaSQLConf.CONCURRENT_IDENTITY_COLUMN_SERVICE_CLASS_NAME.key,
      classOf[SharedLocalIdentitySequenceService].getName)
    // Small cold-start reserve so every reserve-continue test drains its initial range and
    // exercises the reserve-more / async-refill path. Set here (not per-case) because the reserve
    // sizing confs are static (driver-scoped) and reject a withSQLConf override; the exact value is
    // not asserted anywhere, only that a small reserve forces repeated reserves.
    .set(DeltaSQLConf.CONCURRENT_IDENTITY_COLUMN_RESERVE_DRIVER_INITIAL_SIZE.key, "2")
    // A SQL `VALUES` / `range` INSERT is a LocalRelation whose partition count is
    // min(numRows, leafNodeDefaultParallelism). A multi-core test session would split a small
    // insert across tasks and fire one cold reserve per task, breaking exact reserve-count
    // assertions.
    .set("spark.sql.leafNodeDefaultParallelism", "1")

  /** The shared local backend the injected wrapper forwards to; suites assert on it. */
  protected def localService: LocalIdentitySequenceService =
    SharedLocalIdentitySequenceService.shared

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Disable INSERT ONLY MERGE to test concurrency in the actual Low-Shuffle-Merge.
    spark.conf.set(DeltaSQLConf.MERGE_INSERT_ONLY_ENABLED.key, "false")
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    // Start each test with a clean shared backend so invocation counters reflect only this test.
    // Safe because these suites run sequentially (no ParallelTestExecution).
    localService.reset()
  }

  /** A throwable together with its transitive causes, nearest first. */
  protected def causeChain(t: Throwable): Seq[Throwable] =
    Iterator.iterate(t)(_.getCause).takeWhile(_ != null).toSeq

  /** True if any message in the cause chain contains `substring`. */
  protected def messageChainContains(t: Throwable, substring: String): Boolean =
    causeChain(t).exists(c => Option(c.getMessage).exists(_.contains(substring)))

  /** The cause-chain messages joined for failure diagnostics. */
  protected def causeChainMessages(t: Throwable): String =
    causeChain(t).flatMap(c => Option(c.getMessage)).mkString(" | ")

  /**
   * The [[ConcurrentIdentityColumnReservationException]] in `t`'s cause chain. Guard errors thrown
   * from the executor generator reach the driver wrapped in a `SparkException`, so tests unwrap
   * before asserting on the typed error with `checkError`.
   */
  protected def cicReservationCause(t: Throwable): ConcurrentIdentityColumnReservationException =
    causeChain(t).collectFirst {
      case e: ConcurrentIdentityColumnReservationException => e
    }.getOrElse(fail(s"no reservation exception in cause chain: ${causeChainMessages(t)}", t))

  protected def assertCorrectIdentityColumn(
      idColumn: DataFrame,
      expectedSize: Long,
      expectedMin: Long = Long.MinValue,
      expectedMax: Long = Long.MaxValue): Unit = {
    assert(idColumn.count() == expectedSize,
      "Identity column has a different size than expected.")

    assert(idColumn.count() == idColumn.distinct().count(),
      s"""Identity column has duplicates:
        ${idColumn.sort().collect().toSeq.mkString("\n")}""".stripMargin)

    val colName = idColumn.columns.head

    val minVal = idColumn.agg(min(colName)).head().getLong(0)
    assert(expectedMin <= minVal,
      "Identity Column contains values smaller than the expected min.")

    val maxVal = idColumn.agg(max(colName)).head().getLong(0)
    assert(expectedMax >= maxVal,
      "Identity Column contains values greater than the expected max.")
  }

  /**
   * Overload for GENERATED ALWAYS columns: also verifies all values follow the stride.
   * Pass `Long.MinValue` / `Long.MaxValue` for `expectedMin` / `expectedMax` to skip those bounds.
   */
  protected def assertCorrectIdentityColumn(
      idColumn: DataFrame,
      expectedSize: Long,
      start: Long,
      step: Long,
      expectedMin: Long,
      expectedMax: Long): Unit = {
    assertCorrectIdentityColumn(idColumn, expectedSize, expectedMin, expectedMax)
    val colName = idColumn.columns.head
    val violations = idColumn.filter((col(colName) - start) % step =!= 0).count()
    assert(violations === 0L,
      s"$violations value(s) in identity column not congruent with start=$start step=$step.")
  }

  protected def createTargetTableStatement(
      columns: Seq[String],
      catalogManaged: Boolean = true): String = {
    def feature(f: TableFeature, value: String): String =
      s"${TableFeatureProtocolUtils.propertyKey(f)} = '$value'"
    val properties =
      (if (catalogManaged) Seq(feature(CatalogOwnedTableFeature, "supported")) else Nil) ++
        Seq(
          feature(ConcurrentIdentityColumnsTableFeature, "enabled"),
          feature(DomainMetadataTableFeature, "enabled"),
          feature(IdentityColumnsTableFeature, "enabled"))
    s"""
       |CREATE TABLE target (
       |${columns.mkString(", ")})
       |USING DELTA
       |LOCATION '$tempPath'
       |tblproperties(${properties.mkString(", ")})
       |""".stripMargin
  }
}

/**
 * Test backend injected by class name via `identityColumn.concurrent.serviceClassName`.
 * `IdentitySequenceServices.resolve` reflectively constructs a fresh instance per call (no
 * process-wide singleton), so every instance forwards to one shared
 * [[LocalIdentitySequenceService]] in the companion. Suites read invocation counts and reset via
 * [[localService]] (the same shared instance), so assertions see exactly what the production
 * resolve path did.
 */
class SharedLocalIdentitySequenceService extends LocalIdentitySequenceService {
  private val delegate = SharedLocalIdentitySequenceService.shared
  override def createSequence(req: CreateSequenceRequest): Unit =
    delegate.createSequence(req)
  override def reserveIds(req: ReserveIdsRequest): ReserveIdsResponse =
    delegate.reserveIds(req)
  override def dropSequence(req: DropSequenceRequest): Unit =
    delegate.dropSequence(req)
}

object SharedLocalIdentitySequenceService {
  val shared: LocalIdentitySequenceService = new LocalIdentitySequenceService()
}
