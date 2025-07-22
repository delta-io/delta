
/*
 * Copyright (2023) The Delta Lake Project Authors.
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

package io.delta.kernel.internal.fs.benchmarks;

import io.delta.kernel.internal.fs.Path;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

/**
 * Benchmark to measure the performance of initializing/normalizing path objects.
 *
 * <ul>
 *   <li>
 *       <pre>{@code
 * build/sbt sbt:delta> project kernel
 * sbt:delta> set fork in run := true sbt:delta>
 * sbt:delta> test:runMain \
 *   io.delta.kernel.internal.fs.benchmarks.PathNormalizationBenchmarks.
 *
 * }</pre>
 * </ul>
 *
 * }</pre>
 */
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class PathNormalizationBenchmarks {
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void benchmarkNoNormalizationNeeded(Blackhole blackhole) throws Exception {
        blackhole.consume(new Path("s3://bucket-name/table-path/metadata/data/some_file.parquet"));
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void benchmarkNormalizationNeeded(Blackhole blackhole) throws Exception {
        blackhole.consume(new Path("s3://bucket-name/table-path/metadata/data//some_file.parquet"));
    }

    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }
}
