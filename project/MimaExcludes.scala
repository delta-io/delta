/*
 * Copyright 2019 Databricks, Inc.
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
      ProblemFilters.exclude[DirectMissingMethodProblem]("io.delta.tables.DeltaTable.deltaLog"))
}

