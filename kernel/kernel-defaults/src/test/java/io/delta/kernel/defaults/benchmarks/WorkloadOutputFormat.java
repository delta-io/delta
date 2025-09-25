package io.delta.kernel.defaults.benchmarks;

import java.io.IOException;
import java.util.Collection;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.results.BenchmarkResult;
import org.openjdk.jmh.results.IterationResult;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.format.OutputFormat;

public class WorkloadOutputFormat implements OutputFormat {
  public WorkloadOutputFormat() {}

  @Override
  public void iteration(BenchmarkParams benchParams, IterationParams params, int iteration) {}

  @Override
  public void iterationResult(
      BenchmarkParams benchParams, IterationParams params, int iteration, IterationResult data) {}

  @Override
  public void startBenchmark(BenchmarkParams benchParams) {}

  @Override
  public void endBenchmark(BenchmarkResult result) {}

  @Override
  public void startRun() {}

  @Override
  public void endRun(Collection<RunResult> result) {
    System.out.println("End run results:");
    for (RunResult res : result) {
      for (BenchmarkResult br : res.getBenchmarkResults()) {
        System.out.println("Metadata: " + br.getMetadata().getMeasurementTime());
        System.out.println("Data: " + br.getBenchmarkResults());
        System.out.println("Primary result: " + br.getPrimaryResult());
        System.out.println("Secondary result: " + br.getSecondaryResults());
        System.out.println(br.getParams().getParam("workloadSpecJson"));
        System.out.println(br.getScoreUnit());
      }
    }
  }

  @Override
  public void print(String s) {}

  @Override
  public void println(String s) {}

  @Override
  public void flush() {}

  @Override
  public void close() {}

  @Override
  public void verbosePrintln(String s) {}

  @Override
  public void write(int b) {}

  @Override
  public void write(byte[] b) throws IOException {}
}
