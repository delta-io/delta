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

package io.delta.kernel.defaults.benchmarks;

import io.delta.kernel.defaults.utils.ExpressionUtils;
import io.delta.kernel.expressions.Expression;
import java.io.IOException;
import java.util.Optional;
import org.openjdk.jmh.annotations.*;

/** Dynamic benchmark state that holds the workload specification. */
@State(Scope.Benchmark)
public class WorkloadBenchmarkState {

  @Param({}) // Will be populated dynamically by the runner
  public String workloadPath;

  public WorkloadSpec workload;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    workload = WorkloadReader.loadWorkloadFromFile(workloadPath);
    System.out.println("Setting up workload: " + workload);

    System.out.println("about to check predicate");
    if (workload instanceof ReadMetadata) {
      ReadMetadata readMetadataWorkload = (ReadMetadata) workload;
      if (readMetadataWorkload.getPredicate() == null) {
        return;
      }

      Optional<Expression> p =
          ExpressionUtils.parseSqlPredicate(readMetadataWorkload.getPredicate());
      System.out.println("Parsing the predicate: " + p.get());
    }
  }

  public WorkloadSpec getWorkload() {
    return workload;
  }
}
