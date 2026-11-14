/*
 * Copyright (2026) The Delta Lake Project Authors.
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

import com.typesafe.tools.mima.core._

/**
 * The list of Mima errors to exclude in the Kernel projects.
 */
object KernelMimaExcludes {
  val ignoredABIProblems = Seq(
    // scalastyle:off line.size.limit

    // Changes in 4.3.0
    ProblemFilters.exclude[ReversedMissingMethodProblem]("io.delta.kernel.CommitRange.getActions"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("io.delta.kernel.CommitRange.getCommitActions"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.kernel.exceptions.UnsupportedProtocolVersionException.this"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.kernel.internal.DeltaErrors.unsupportedReaderProtocol"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.kernel.internal.DeltaErrors.unsupportedWriterProtocol"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("io.delta.kernel.statistics.SnapshotStatistics.getIncrementalChecksumLoadCost"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("io.delta.kernel.statistics.SnapshotStatistics.getTableStats"),
    ProblemFilters.exclude[MissingFieldProblem]("io.delta.kernel.types.GeographyType.DEFAULT_SRID"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.kernel.types.GeographyType.ofSRID"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.kernel.types.GeographyType.getSRID"),
    ProblemFilters.exclude[MissingFieldProblem]("io.delta.kernel.types.GeometryType.DEFAULT_SRID"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.kernel.types.GeometryType.ofSRID"),
    ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.kernel.types.GeometryType.getSRID")

    // scalastyle:on line.size.limit
  )
}
