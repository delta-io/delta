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

package org.apache.spark.sql.delta.test

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.test.DeltaTestImplicits._

import org.apache.spark.sql.execution.{ColumnarToRowExec, FileSourceScanExec, InputAdapter, SparkPlan}
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.test.SQLTestUtils

/**
 * Provides utilities for testing StatisticsCollection.
 */
trait TestsStatistics { self: SQLTestUtils =>

  /** A function to get the reconciled statistics DataFrame from the DeltaLog */
  protected var getStatsDf: (DeltaLog, Seq[Column]) => DataFrame = _

  /**
   * Creates the correct `getStatsDf` to be used by the `testFun` and executes the `testFun`.
   */
  protected def statsTest(testName: String, testTags: org.scalatest.Tag*)(testFun: => Any): Unit = {
    import testImplicits._

    test(testName, testTags: _*) {
      getStatsDf = (deltaLog, columns) => {
        val snapshot = deltaLog.snapshot
        snapshot.allFiles
          .withColumn("stats", from_json($"stats", snapshot.statsSchema))
          .select("stats.*")
          .select(columns: _*)
      }
      testFun
    }
  }

  /**
   * A util to match a physical file scan node.
   */
  object FileScanExecNode {
    def unapply(plan: SparkPlan): Option[FileSourceScanExec] = plan match {
      case f: FileSourceScanExec => Some(f)
      case InputAdapter(f: FileSourceScanExec) => Some(f)
      case ColumnarToRowExec(InputAdapter(f: FileSourceScanExec)) => Some(f)
      case _ => None
    }
  }
}
