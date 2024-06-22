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
  // scalastyle:off line.size.limit

  val ignoredABIProblems = Seq(
    // We can ignore internal changes
    ProblemFilters.exclude[Problem]("io.delta.standalone.internal.*")
  )
}
