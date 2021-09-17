/*
 * Copyright (2020) The Delta Lake Project Authors.
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
import com.typesafe.tools.mima.core.ProblemFilters._

/**
 * The list of Mima errors to exclude in the Standalone project.
 */
object StandaloneMimaExcludes {
  val ignoredABIProblems = Seq(
    // scalastyle:off line.size.limit

    // Ignore changes to internal Scala codes
    ProblemFilters.exclude[Problem]("io.delta.standalone.internal.*"),

    // Public API changes in 0.2.0 -> 0.3.0
    ProblemFilters.exclude[ReversedMissingMethodProblem]("io.delta.standalone.DeltaLog.getChanges"),
    ProblemFilters.exclude[ReversedMissingMethodProblem]("io.delta.standalone.DeltaLog.startTransaction")

    // scalastyle:on line.size.limit
  )
}
