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

package io.delta.kernel.defaults.benchmarks;

import io.delta.kernel.engine.*;
import io.delta.kernel.metrics.MetricsReport;
import io.delta.kernel.metrics.ScanMetricsResult;
import io.delta.kernel.metrics.ScanReport;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.profile.InternalProfiler;
import org.openjdk.jmh.results.*;
import org.openjdk.jmh.util.SampleBuffer;

/**
 * JMH profiler that extracts and reports Delta Kernel metrics during benchmark execution.
 *
 * <p>This profiler collects metrics reports from the Delta Kernel during benchmark runs and
 * converts them into JMH secondary results for analysis. It works by wrapping the benchmark engine
 * with a {@link BenchmarkingEngine} that captures all metrics reports.
 *
 * <p>The profiler extracts various scan metrics including planning duration, file counts, and other
 * performance-related measurements that can be analyzed alongside the primary benchmark timing
 * results.
 */
public class KernelMetricsProfiler implements InternalProfiler {

  public KernelMetricsProfiler() {}

  public static final List<MetricsReport> reports = new ArrayList<>();

  /**
   * Adds a metrics report to the collection for processing during benchmark execution.
   *
   * @param newReport the metrics report to add
   */
  public static void addReport(MetricsReport newReport) {
    reports.add(newReport);
  }

  @Override
  public String getDescription() {
    return "Extracts metrics from the Delta Kernel metrics reports.";
  }

  @Override
  public void beforeIteration(BenchmarkParams benchmarkParams, IterationParams iterationParams) {
    reports.clear();
  }

  /**
   * Generates a scalar result with average aggregation policy for count metrics.
   *
   * @param name the name of the metric
   * @param value the metric value
   * @return a ScalarResult configured for count metrics with average aggregation
   */
  private static ScalarResult generateAvgScalarCount(String name, double value) {
    return new ScalarResult(name, value, "count", AggregationPolicy.AVG);
  }

  /**
   * Generates a time sample result for timing metrics.
   *
   * @param name the name of the timing metric
   * @param value the timing value
   * @param unit the time unit for the value
   * @return a SampleTimeResult for the timing metric
   */
  private static SampleTimeResult generateTimeSample(String name, long value, TimeUnit unit) {
    SampleBuffer buf = new SampleBuffer();
    buf.add(value);
    return new SampleTimeResult(ResultRole.SECONDARY, name, buf, unit);
  }

  /**
   * Extracts metrics from a scan report and converts them to JMH results.
   *
   * @param report the scan report containing metrics to extract
   * @return a stream of JMH Result objects representing the extracted metrics
   */
  private Stream<? extends Result> extractMetrics(ScanReport report) {
    ScanMetricsResult scanReport = report.getScanMetrics();
    Stream<? extends Result> out =
        Stream.of(
            generateTimeSample(
                "scan.scan_metrics.total_planning_duration_ns",
                scanReport.getTotalPlanningDurationNs(),
                TimeUnit.NANOSECONDS),
            generateAvgScalarCount(
                "scan.scan_metrics.num_active_add_files", scanReport.getNumActiveAddFiles()),
            generateAvgScalarCount(
                "scan.scan_metrics.num_add_files_seen", scanReport.getNumAddFilesSeen()),
            generateAvgScalarCount(
                "scan.scan_metrics.num_add_files_seen_from_delta_files",
                scanReport.getNumAddFilesSeenFromDeltaFiles()),
            generateAvgScalarCount(
                "scan.scan_metrics.num_duplicate_add_files", scanReport.getNumDuplicateAddFiles()),
            generateAvgScalarCount(
                "scan.scan_metrics.num_remove_files_seen_from_delta_files",
                scanReport.getNumRemoveFilesSeenFromDeltaFiles()));

    return out;
  }

  @Override
  public Collection<? extends Result> afterIteration(
      BenchmarkParams benchmarkParams, IterationParams iterationParams, IterationResult result) {
    Stream<? extends Result> out = Stream.empty();
    for (MetricsReport report : reports) {
      if (report instanceof ScanReport) {
        out = Stream.concat(out, extractMetrics((ScanReport) report));
      }
    }
    return out.collect(Collectors.toList());
  }

  /**
   * An {@link Engine} implementation that wraps an existing engine and delegates all engine tasks.
   * The BenchmarkingEngine sends all metrics reports to the KernelMetricsProfiler for collection
   * during benchmarks. The metrics reports can then be extracted and reported by the
   * KernelMetricsProfiler.
   */
  public static class BenchmarkingEngine implements Engine {
    private final Engine delegate;
    static final BenchmarkMetricsReporter BENCHMARK_METRICS_REPORTER =
        new BenchmarkMetricsReporter();

    /**
     * Creates a new BenchmarkingEngine that wraps the provided engine.
     *
     * @param delegate the engine to wrap for metrics collection
     */
    BenchmarkingEngine(Engine delegate) {
      this.delegate = delegate;
    }

    @Override
    public ExpressionHandler getExpressionHandler() {
      return delegate.getExpressionHandler();
    }

    @Override
    public JsonHandler getJsonHandler() {
      return delegate.getJsonHandler();
    }

    @Override
    public FileSystemClient getFileSystemClient() {
      return delegate.getFileSystemClient();
    }

    @Override
    public ParquetHandler getParquetHandler() {
      return delegate.getParquetHandler();
    }

    @Override
    public List<MetricsReporter> getMetricsReporters() {
      return Collections.singletonList(BENCHMARK_METRICS_REPORTER);
    }

    /**
     * Wraps an engine with benchmarking metrics collection capabilities.
     *
     * @param engine the engine to wrap
     * @return a BenchmarkingEngine that collects metrics from the wrapped engine
     */
    public static BenchmarkingEngine wrapEngine(Engine engine) {
      return new BenchmarkingEngine(engine);
    }

    /** Metrics reporter implementation that forwards all metrics reports to the profiler. */
    private static final class BenchmarkMetricsReporter implements MetricsReporter {
      @Override
      public void report(MetricsReport report) {
        addReport(report);
      }
    }
  }
}
