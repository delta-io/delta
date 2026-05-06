/*
 * Copyright (2025) The Delta Lake Project Authors.
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

import org.apache.spark.sql.delta.catalog.InMemoryDeltaCatalog
import org.apache.spark.SparkConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tag for tests that access Delta internals (e.g., DeltaLog, physical scans, usage logs)
 * and are therefore incompatible with the DSv2 InMemoryTable test path.
 * Tests tagged with this are automatically skipped when [[InMemoryTestTableMixin]] is active.
 */
case class DSv2Incompatible(reason: String) extends org.scalatest.Tag("DSv2Incompatible")

/**
 * Tag for tests that exercise some features that are _currently_ not implemented for DSv2, but
 * should be implemented sometime in the future.
 * Not [[DSv2Incompatible]] -- that one is for tests that are completely unsupported and would
 * never pass with DSv2.
 * Tests tagged with this are automatically skipped when [[InMemoryTestTableMixin]] is active.
 */
case class DSv2TemporarilyIncompatible(reason: String)
  extends org.scalatest.Tag("DSv2TemporarilyIncompatible")

/**
 * Tag for tests that assert the physical Delta execution plan shape.
 * Tests tagged with this are automatically skipped when [[InMemoryTestTableMixin]] is active.
 */
case class ChecksPhysicalDeltaPlan(reason: String = "checks physical Delta plan")
  extends org.scalatest.Tag("ChecksPhysicalDeltaPlan")

/**
 * Tag for tests that inspect or mutate Delta internals such as DeltaLog or file stats.
 * Tests tagged with this are automatically skipped when [[InMemoryTestTableMixin]] is active.
 */
case class ChecksDeltaInternals(reason: String = "checks Delta internals")
  extends org.scalatest.Tag("ChecksDeltaInternals")

/**
 * Tag for tests that inspect Delta metrics.
 * Test tagged with this are automatically skipped when [[InMemoryTestTableMixin]] is active.
 */
case class ChecksDeltaMetrics(reason: String = "checks Delta metrics")
  extends org.scalatest.Tag("ChecksDeltaMetrics")

/**
 * Tag for tests that exercise DSv2 DML schema evolution. Schema evolution in DSv2 DML commands
 * is only available starting with Spark 4.2, so tests tagged with this are automatically skipped
 * when running against Spark 4.0/4.1.
 */
case object DSv2DMLSchemaEvolution extends org.scalatest.Tag("DSv2DMLSchemaEvolution")

/**
 * Mixin trait that configures the session catalog to use [[InMemoryDeltaCatalog]],
 * routing DML operations through Spark's V2 execution path via [[InMemorySparkTable]].
 */
trait InMemoryTestTableMixin extends SharedSparkSession with InMemoryTestTableMixinShims  {

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.spark_catalog", classOf[InMemoryDeltaCatalog].getName)

  override protected def test
      (testName: String, testTags: org.scalatest.Tag*)
      (testFun: => Any)
      (implicit pos: org.scalactic.source.Position): Unit = {
    testTags.collectFirst {
      case t: DSv2Incompatible =>
        "DSv2Incompatible" -> t.reason
      case t: DSv2TemporarilyIncompatible =>
        "DSv2TemporarilyIncompatible" -> t.reason
      case t: ChecksPhysicalDeltaPlan =>
        "ChecksPhysicalDeltaPlan" -> t.reason
      case t: ChecksDeltaInternals =>
        "ChecksDeltaInternals" -> t.reason
      case DSv2DMLSchemaEvolution if !v2DmlSchemaEvolutionSupported =>
        "DSV2DMLSchemaEvolution" -> "DSv2 DML schema evolution not supported on this spark version"
    } match {
      case Some((tagName, reason)) =>
        ignore(testName + s" ($tagName: $reason)", testTags: _*)(testFun)
      case None =>
        super.test(testName, testTags: _*)(testFun)
    }
  }
}
