/*
 * Copyright (2020-present) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.typesafe.tools.mima.core._

/**
 * The list of Mima errors to exclude in the Flink project.
 */
object FlinkMimaExcludes {
  val ignoredABIProblems = Seq(
    // scalastyle:off line.size.limit

    // Changes when adding kernel snapshots
    ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.flink.source.internal.enumerator.supplier.BoundedSnapshotSupplierFactory.create"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.flink.source.internal.enumerator.supplier.BoundedSourceSnapshotSupplier.this"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.flink.source.internal.enumerator.supplier.ContinuousSnapshotSupplierFactory.create"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.flink.source.internal.enumerator.supplier.ContinuousSourceSnapshotSupplier.this"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.flink.source.internal.enumerator.supplier.SnapshotSupplierFactory.create"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("io.delta.flink.source.internal.enumerator.supplier.SnapshotSupplierFactory.create"),
    // scalastyle:on line.size.limit
  )
}
