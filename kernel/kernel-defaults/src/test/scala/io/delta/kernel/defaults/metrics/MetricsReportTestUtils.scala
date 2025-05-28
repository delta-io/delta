/*
 * Copyright (2024) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults.metrics

import java.util

import scala.collection.mutable.ArrayBuffer

import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.defaults.utils.TestUtils
import io.delta.kernel.engine._
import io.delta.kernel.metrics.MetricsReport

import org.apache.hadoop.conf.Configuration

/**
 * Test utilities for testing the Kernel-API created [[MetricsReports]]s.
 *
 * We test [[MetricsReport]]s in the defaults package so we can use real tables and avoid having
 * to mock both file listings AND file contents.
 */
trait MetricsReportTestUtils extends TestUtils {

  override lazy val defaultEngine = DefaultEngine.create(new Configuration() {
    {
      // Set the batch sizes to small so that we get to test the multiple batch scenarios.
      set("delta.kernel.default.parquet.reader.batch-size", "2");
      set("delta.kernel.default.json.reader.batch-size", "2");
    }
  })

  // For now this just uses the default engine since we have no need to override it, if we would
  // like to use a specific engine in the future for other tests we can simply add another arg here
  /**
   * Executes [[f]] using a special engine implementation to collect and return metrics reports.
   * If [[expectException]], catches any exception thrown by [[f]] and returns it with the reports.
   */
  def collectMetricsReports(
      f: Engine => Unit,
      expectException: Boolean): (Seq[MetricsReport], Option[Exception]) = {
    // Initialize a buffer for any metric reports and wrap the engine so that they are recorded
    val reports = ArrayBuffer.empty[MetricsReport]
    if (expectException) {
      val e = intercept[Exception](
        f(new EngineWithInMemoryMetricsReporter(reports, defaultEngine)))
      (reports.toSeq, Some(e))
    } else {
      f(new EngineWithInMemoryMetricsReporter(reports, defaultEngine))
      (reports.toSeq, Option.empty)
    }
  }

  /**
   * Wraps an {@link Engine} to implement the metrics reporter such that it appends any reports
   * to the provided in memory buffer.
   */
  class EngineWithInMemoryMetricsReporter(buf: ArrayBuffer[MetricsReport], baseEngine: Engine)
      extends Engine {

    private val inMemoryMetricsReporter = new MetricsReporter {
      override def report(report: MetricsReport): Unit = buf.append(report)
    }

    private val metricsReporters = new util.ArrayList[MetricsReporter]() {
      {
        addAll(baseEngine.getMetricsReporters)
        add(inMemoryMetricsReporter)
      }
    }

    override def getExpressionHandler: ExpressionHandler = baseEngine.getExpressionHandler

    override def getJsonHandler: JsonHandler = baseEngine.getJsonHandler

    override def getFileSystemClient: FileSystemClient = baseEngine.getFileSystemClient

    override def getParquetHandler: ParquetHandler = baseEngine.getParquetHandler

    override def getMetricsReporters(): java.util.List[MetricsReporter] = {
      metricsReporters
    }
  }
}
