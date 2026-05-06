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
  extends org.scalatest.Tag("DSv2TemporariltyIncompatible")

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
    for (tag <- testTags) {
      tag match {
        case t: DSv2Incompatible =>
          ignore(testName + s" (DSv2Incompatible: $t.reason)", testTags: _*)(testFun)
          return
        case t: DSv2TemporarilyIncompatible =>
          ignore(testName + s" (DSv2TemporarilyIncompatible: $t.reason)", testTags: _*)(testFun)
          return
        case DSv2DMLSchemaEvolution if !v2DmlSchemaEvolutionSupported =>
          ignore(testName + s" (DSv2DMLSchemaEvolution: DSv2 DML schema evolution not supported " +
            "on this Spark version", testTags: _*)(testFun)
          return
      }
    }
    super.test(testName, testTags: _*)(testFun)
  }
}
