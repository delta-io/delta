package org.apache.spark.sql.delta.test

import org.apache.spark.sql.delta.DeltaLog

import org.apache.spark.sql.execution.{ColumnarToRowExec, FileSourceScanExec, InputAdapter, SparkPlan}
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.test.SQLTestUtils

/**
 * Provides utilities for testing DataSkippingReader.
 */
trait TestsStatistics { self: SQLTestUtils =>

  /** A function to get the reconciled statistics DataFrame from the DeltaLog */
  protected var getStatsDf: (DeltaLog, Seq[Column]) => DataFrame = _

  /**
   * Runs the given test code with DataSkippingReaderV2 enabled and disabled.
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

  // A util to match a physical file scan node.
  object FileScanExecNode {
    def unapply(plan: SparkPlan): Option[FileSourceScanExec] = plan match {
      case f: FileSourceScanExec => Some(f)
      case InputAdapter(f: FileSourceScanExec) => Some(f)
      case ColumnarToRowExec(InputAdapter(f: FileSourceScanExec)) => Some(f)
      case _ => None
    }
  }
}

