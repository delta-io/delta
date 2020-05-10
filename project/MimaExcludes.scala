/*
 * Copyright (2020) The Delta Lake Project Authors.
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
import com.typesafe.tools.mima.core.ProblemFilters._

/**
 * The list of Mima errors to exclude.
 */
object MimaExcludes {
  val ignoredABIProblems = Seq(
      ProblemFilters.exclude[Problem]("org.*"),
      ProblemFilters.exclude[Problem]("io.delta.sql.parser.*"),
      ProblemFilters.exclude[Problem]("io.delta.tables.execution.*"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.tables.DeltaTable.apply"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.tables.DeltaTable.executeHistory"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.tables.DeltaTable.this"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.tables.DeltaTable.deltaLog"),

      // Changes in 0.6.0
      ProblemFilters.exclude[IncompatibleResultTypeProblem]("io.delta.tables.DeltaTable.makeUpdateTable"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("io.delta.tables.DeltaMergeBuilder.withClause"),
      ProblemFilters.exclude[IncompatibleMethTypeProblem]("io.delta.tables.DeltaTable.this"),

      // ... removed unnecessarily public methods in DeltaMergeBuilder
      ProblemFilters.exclude[MissingTypesProblem]("io.delta.tables.DeltaMergeBuilder"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.tables.DeltaMergeBuilder.recordUsage"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.tables.DeltaMergeBuilder.recordUsage$default$3"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.tables.DeltaMergeBuilder.recordOperation$default$7"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.tables.DeltaMergeBuilder.recordUsage$default$6"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.tables.DeltaMergeBuilder.logError"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.tables.DeltaMergeBuilder.log"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.tables.DeltaMergeBuilder.recordDeltaOperation$default$3"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.tables.DeltaMergeBuilder.recordOperation$default$4"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.tables.DeltaMergeBuilder.recordEvent$default$3"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.tables.DeltaMergeBuilder.logName"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.tables.DeltaMergeBuilder.recordDeltaEvent"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.tables.DeltaMergeBuilder.withStatusCode$default$3"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.tables.DeltaMergeBuilder.recordOperation"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.tables.DeltaMergeBuilder.isTraceEnabled"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.tables.DeltaMergeBuilder.withStatusCode"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.tables.DeltaMergeBuilder.recordEvent"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.tables.DeltaMergeBuilder.recordDeltaEvent$default$4"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.tables.DeltaMergeBuilder.logDebug"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.tables.DeltaMergeBuilder.logInfo"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.tables.DeltaMergeBuilder.logInfo"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.tables.DeltaMergeBuilder.recordUsage$default$5"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.tables.DeltaMergeBuilder.recordOperation$default$6"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.tables.DeltaMergeBuilder.logTrace"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.tables.DeltaMergeBuilder.initializeLogIfNecessary"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.tables.DeltaMergeBuilder.recordOperation$default$9"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.tables.DeltaMergeBuilder.recordEvent$default$2"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.tables.DeltaMergeBuilder.recordUsage$default$4"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.tables.DeltaMergeBuilder.logWarning"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.tables.DeltaMergeBuilder.recordUsage$default$7"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.tables.DeltaMergeBuilder.recordDeltaEvent$default$3"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.tables.DeltaMergeBuilder.recordOperation$default$2"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.tables.DeltaMergeBuilder.recordDeltaOperation"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.tables.DeltaMergeBuilder.logConsole"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.tables.DeltaMergeBuilder.recordOperation$default$5"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.tables.DeltaMergeBuilder.recordEvent$default$4"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.tables.DeltaMergeBuilder.recordOperation$default$8"),
      ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.tables.DeltaMergeBuilder.initializeLogIfNecessary$default$2")
  )
}

