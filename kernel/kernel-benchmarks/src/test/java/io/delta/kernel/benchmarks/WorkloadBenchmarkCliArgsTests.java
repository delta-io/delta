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
 *
 * Author: Anudeep Konaboina <krantianudeep@gmail.com>
 */

package io.delta.kernel.benchmarks;

import static org.junit.Assert.*;

import org.junit.Test;

public class WorkloadBenchmarkCliArgsTests {

  @Test
  public void defaultsMatchCurrentHardcodedValues() {
    WorkloadBenchmark.CommandLineArgs args =
        WorkloadBenchmark.CommandLineArgs.parse(new String[] {});
    assertEquals(1, args.forks);
    assertEquals(3, args.warmupIterations);
    assertEquals(5, args.measurementIterations);
    assertEquals(1, args.warmupTimeSeconds);
    assertEquals(1, args.measurementTimeSeconds);
    assertTrue(args.includeTestSubstrings.isEmpty());
  }

  @Test
  public void includeTestUsesSubstringMatchAndIsRepeatable() {
    WorkloadBenchmark.CommandLineArgs args =
        WorkloadBenchmark.CommandLineArgs.parse(
            new String[] {"--include-test", "read_metadata", "--include-test", "snapshot"});

    assertTrue(args.shouldInclude("basic_append/read_metadata/read"));
    assertTrue(args.shouldInclude("basic_append/snapshot_latest/snapshot_construction"));
    assertFalse(args.shouldInclude("basic_append/write_appends/write"));
  }

  @Test
  public void parsesNumericOverrides() {
    WorkloadBenchmark.CommandLineArgs args =
        WorkloadBenchmark.CommandLineArgs.parse(
            new String[] {
              "--forks",
              "2",
              "--warmup-iterations",
              "7",
              "--measurement-iterations",
              "9",
              "--warmup-time-seconds",
              "3",
              "--measurement-time-seconds",
              "4"
            });

    assertEquals(2, args.forks);
    assertEquals(7, args.warmupIterations);
    assertEquals(9, args.measurementIterations);
    assertEquals(3, args.warmupTimeSeconds);
    assertEquals(4, args.measurementTimeSeconds);
  }

  @Test
  public void supportsWarmupIterationsTypoAlias() {
    WorkloadBenchmark.CommandLineArgs args =
        WorkloadBenchmark.CommandLineArgs.parse(new String[] {"--warum-iterations", "10"});
    assertEquals(10, args.warmupIterations);
  }

  @Test
  public void invalidArgsThrowHelpfulError() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> WorkloadBenchmark.CommandLineArgs.parse(new String[] {"--forks", "0"}));
    assertTrue(e.getMessage().contains("--forks"));
  }
}
