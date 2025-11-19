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

package io.delta.kernel.benchmarks;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.kernel.benchmarks.models.WorkloadSpec;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.results.*;
import org.openjdk.jmh.results.BenchmarkResult;
import org.openjdk.jmh.results.IterationResult;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.format.OutputFormat;
import org.openjdk.jmh.runner.format.OutputFormatFactory;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.openjdk.jmh.util.Statistics;

/**
 * Custom JMH output format that generates structured JSON benchmark reports.
 *
 * <p>This output format captures benchmark results and generates a comprehensive JSON report
 * containing execution environment details, benchmark configuration, timing metrics, and secondary
 * metrics. The report includes detailed percentile analysis and is written to the working directory
 * as {@code benchmark_report.json}.
 *
 * <p>This format also delegates to JMH's standard text output format to print progress during
 * benchmark execution.
 *
 * <p>The generated report structure includes:
 *
 * <ul>
 *   <li>Report metadata (generation time, JMH version, etc.)
 *   <li>Execution environment (JVM, OS, hardware details)
 *   <li>Benchmark configuration and parameters
 *   <li>Benchmark details (spec, additional params, timing metrics, secondary metrics)
 * </ul>
 */
public class WorkloadOutputFormat implements OutputFormat {
  private final OutputFormat delegate =
      OutputFormatFactory.createFormatInstance(System.out, VerboseMode.NORMAL);
  private final Path outputPath =
      Paths.get(System.getProperty("user.dir"), "benchmark_report.json");

  private static final double[] PERCENTILES = {0.5, 0.9, 0.95, 0.99, 0.999, 0.9999, 1.0};

  /** Metadata about the benchmark report itself. Json formatted */
  private static class ReportMetadata {
    @JsonProperty("generated_at")
    private final String generated_at;

    @JsonProperty("jmh_version")
    private final String jmh_version;

    @JsonProperty("report_version")
    private final String report_version;

    @JsonProperty("benchmark_suite")
    private final String benchmark_suite;

    ReportMetadata(
        String generated_at, String jmh_version, String report_version, String benchmark_suite) {
      this.generated_at = generated_at;
      this.jmh_version = jmh_version;
      this.report_version = report_version;
      this.benchmark_suite = benchmark_suite;
    }

    public String toString() {
      return String.format(
          "ReportMetadata{generated_at='%s', jmh_version='%s',"
              + " report_version='%s', benchmark_suite='%s'}",
          generated_at, jmh_version, report_version, benchmark_suite);
    }
  }

  public static class ExecutionEnvironment {
    @JsonProperty("jvm")
    private final String jvm;

    @JsonProperty("heap_size_mb")
    private final String heapSizeMB;

    @JsonProperty("jdk_version")
    private final String jdk_version;

    @JsonProperty("vm_name")
    private final String vm_name;

    @JsonProperty("vm_version")
    private final String vm_version;

    @JsonProperty("cpu_model")
    private final String cpuModel;

    @JsonProperty("cpu_arch")
    private final String cpuArch;

    @JsonProperty("cpu_cores")
    private final Long cpuCores;

    @JsonProperty("os_name")
    private final String osName;

    @JsonProperty("os_version")
    private final String osVersion;

    @JsonProperty("max_memory_mb")
    private final Long maxMemoryMb;

    public ExecutionEnvironment() {
      this.jvm = System.getProperty("java.vm.name");
      this.heapSizeMB = Runtime.getRuntime().maxMemory() / (1024 * 1024) + " MB";
      this.jdk_version = System.getProperty("java.version");
      this.vm_name = System.getProperty("java.vm.name");
      this.vm_version = System.getProperty("java.vm.version");
      this.cpuModel = System.getProperty("os.arch");
      this.cpuArch = System.getProperty("os.arch");
      this.cpuCores = (long) Runtime.getRuntime().availableProcessors();
      this.osName = System.getProperty("os.name");
      this.osVersion = System.getProperty("os.version");
      this.maxMemoryMb = Runtime.getRuntime().maxMemory() / (1024 * 1024);
    }

    public String toString() {
      return String.format(
          "ExecutionEnvironment{jvm='%s', heapSizeMB='%s', jdk_version='%s',"
              + " vm_name='%s', vm_version='%s', cpuModel='%s', cpuArch='%s',"
              + " cpuCores=%d, osName='%s', osVersion='%s'}",
          jvm,
          heapSizeMB,
          jdk_version,
          vm_name,
          vm_version,
          cpuModel,
          cpuArch,
          cpuCores,
          osName,
          osVersion);
    }
  }

  private static class BenchmarkDetails {
    @JsonProperty("spec")
    private WorkloadSpec spec;

    @JsonProperty("additional_params")
    private HashMap<String, String> additionalParams;

    @JsonProperty("time")
    private TimingMetric time;

    @JsonProperty("secondary_metrics")
    private HashMap<String, Object> secondary_metrics;

    BenchmarkDetails(
        WorkloadSpec spec,
        HashMap<String, String> additionalParams,
        TimingMetric time,
        HashMap<String, Object> secondary_metrics) {
      this.spec = spec;
      this.additionalParams = additionalParams;
      this.time = time;
      this.secondary_metrics = secondary_metrics;
    }

    public String toString() {
      return String.format(
          "BenchmarkDetails{spec=%s, additionalParams=%s, time=%s, secondary_metrics=%s}",
          spec, additionalParams.toString(), time.toString(), secondary_metrics.toString());
    }
  }

  private static class BenchmarkReport {
    @JsonProperty("report_metadata")
    private ReportMetadata reportMetadata;

    @JsonProperty("execution_environment")
    private ExecutionEnvironment executionEnvironment;

    @JsonProperty("benchmark_configuration")
    private HashMap<String, String> benchmarkConfiguration;

    @JsonProperty("benchmarks")
    private HashMap<String, BenchmarkDetails> benchmarks;

    BenchmarkReport(
        ReportMetadata reportMetadata,
        ExecutionEnvironment executionEnvironment,
        HashMap<String, String> benchmarkConfiguration,
        HashMap<String, BenchmarkDetails> benchmarks) {
      this.reportMetadata = reportMetadata;
      this.executionEnvironment = executionEnvironment;
      this.benchmarkConfiguration = benchmarkConfiguration;
      this.benchmarks = benchmarks;
    }

    public String toString() {
      return String.format(
          "BenchmarkReport{reportMetadata=%s, executionEnvironment=%s,"
              + " benchmarkConfiguration=%s, benchmarks=%s}",
          reportMetadata.toString(),
          executionEnvironment.toString(),
          benchmarkConfiguration.toString(),
          benchmarks.toString());
    }
  }

  private static class TimingMetric {
    @JsonProperty("score")
    private final double score;

    @JsonProperty("score_unit")
    private final String score_unit;

    @JsonProperty("score_error")
    private final double score_error;

    @JsonProperty("score_confidence")
    private final double[] score_confidence;

    @JsonProperty("sample_count")
    private final long sample_count;

    @JsonProperty("percentiles")
    private final HashMap<String, Double> percentiles;

    TimingMetric(
        double score,
        String score_unit,
        double score_error,
        double[] score_confidence,
        long sample_count,
        HashMap<String, Double> percentiles) {
      this.score = score;
      this.score_unit = score_unit;
      this.score_error = score_error;
      this.score_confidence = score_confidence;
      this.sample_count = sample_count;
      this.percentiles = percentiles;
    }

    public static TimingMetric fromResult(Result result) {
      HashMap<String, Double> percentiles = new HashMap<>();
      Statistics stats = result.getStatistics();
      for (double p : PERCENTILES) {
        String key = String.format("p%.2f", p);
        percentiles.put(key, stats.getPercentile(p));
      }
      return new TimingMetric(
          result.getScore(),
          result.getScoreUnit(),
          result.getScoreError(),
          result.getScoreConfidence(),
          result.getSampleCount(),
          percentiles);
    }

    public String toString() {
      return String.format(
          "TimingMetric{score=%f, score_unit='%s', score_error=%f,"
              + " score_confidence=[%s], sample_count=%d, percentiles=%s}",
          score,
          score_unit,
          score_error,
          String.join(
              ", ",
              new String[] {
                String.valueOf(score_confidence[0]), String.valueOf(score_confidence[1])
              }),
          sample_count,
          percentiles.toString());
    }
  }

  @Override
  public void iteration(BenchmarkParams benchParams, IterationParams params, int iteration) {
    delegate.iteration(benchParams, params, iteration);
  }

  @Override
  public void iterationResult(
      BenchmarkParams benchParams, IterationParams params, int iteration, IterationResult data) {
    delegate.iterationResult(benchParams, params, iteration, data);
  }

  @Override
  public void startBenchmark(BenchmarkParams benchParams) {
    delegate.startBenchmark(benchParams);
  }

  @Override
  public void endBenchmark(BenchmarkResult result) {
    delegate.endBenchmark(result);
  }

  @Override
  public void startRun() {
    delegate.startRun();
  }

  @Override
  public void endRun(Collection<RunResult> result) {
    println("\n=== Generating JSON Benchmark Report ===");
    ReportMetadata metadata =
        new ReportMetadata(
            String.valueOf(System.currentTimeMillis()),
            org.openjdk.jmh.Main.class.getPackage().getImplementationVersion(),
            "1.0",
            "Delta Kernel Benchmarks");
    ExecutionEnvironment env = new ExecutionEnvironment();
    HashMap<String, String> benchConfig = new HashMap<>();

    HashMap<String, BenchmarkDetails> benchmarks = new HashMap<>();

    for (RunResult res : result) {
      BenchmarkResult br = res.getAggregatedResult();
      try {
        WorkloadSpec spec =
            WorkloadSpec.fromJsonString(br.getParams().getParam("workloadSpecJson"));
        HashMap<String, String> additionalParams = new HashMap<>();
        additionalParams.put("engine", br.getParams().getParam("engineName"));

        HashMap<String, Object> secondaryMetrics = new HashMap<>();
        for (String resultKey : br.getSecondaryResults().keySet()) {
          Result r = br.getSecondaryResults().get(resultKey);
          if (r instanceof org.openjdk.jmh.results.SampleTimeResult) {
            secondaryMetrics.put(r.getLabel(), TimingMetric.fromResult(r));
          } else if (r instanceof org.openjdk.jmh.results.ScalarResult) {
            ScalarResult scalarResult = (ScalarResult) r;
            if (scalarResult.getScoreUnit().equals("count")) {
              // Convert count metrics to long integers to avoid decimal representation in JSON
              // output (e.g., report "42" instead of "42.0" for file counts)
              secondaryMetrics.put(r.getLabel(), (long) r.getScore());
            } else {
              secondaryMetrics.put(r.getLabel(), r.getScore());
            }
          }
        }

        BenchmarkDetails details =
            new BenchmarkDetails(
                spec,
                additionalParams,
                TimingMetric.fromResult(br.getPrimaryResult()),
                secondaryMetrics);
        benchmarks.put(spec.getFullName(), details);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    BenchmarkReport report = new BenchmarkReport(metadata, env, benchConfig, benchmarks);

    // Write report to user.dir
    ObjectMapper mapper = new ObjectMapper();
    try {
      String jsonReport = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(report);

      println("Generated benchmark report:\n" + jsonReport);
      println("Writing benchmark report to " + outputPath);

      Files.write(outputPath, jsonReport.getBytes());
    } catch (IOException e) {
      throw new RuntimeException("Failed to serialize benchmark report to JSON", e);
    }
  }

  @Override
  public void print(String s) {
    delegate.print(s);
  }

  @Override
  public void println(String s) {
    delegate.println(s);
  }

  @Override
  public void flush() {
    delegate.flush();
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public void verbosePrintln(String s) {
    delegate.verbosePrintln(s);
  }

  @Override
  public void write(int b) {
    delegate.write(b);
  }

  @Override
  public void write(byte[] b) throws IOException {
    delegate.write(b);
  }
}
