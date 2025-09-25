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

public class KernelMetricsProfiler implements InternalProfiler {

  public KernelMetricsProfiler() {}

  public static final List<MetricsReport> reports = new ArrayList<>();

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

  private static ScalarResult generateAvgScalarCount(String name, double value) {
    return new ScalarResult(name, value, "count", AggregationPolicy.AVG);
  }

  private static SampleTimeResult generateTimeSample(String name, long value, TimeUnit unit) {
    SampleBuffer buf = new SampleBuffer();
    buf.add(value);
    return new SampleTimeResult(ResultRole.SECONDARY, name, buf, unit);
  }

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

    BenchmarkingEngine(Engine inner) {
      this.delegate = inner;
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

    public static BenchmarkingEngine wrapEngine(Engine engine) {
      return new BenchmarkingEngine(engine);
    }

    private static final class BenchmarkMetricsReporter implements MetricsReporter {
      @Override
      public void report(MetricsReport report) {
        addReport(report);
      }
    }
  }
}
