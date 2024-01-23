/*
 * Copyright (2021) The Delta Lake Project Authors.
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

name := "benchmarks"
scalaVersion := "2.12.17"

lazy val root = (project in file("."))
  .settings(
    name := "benchmarks",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided",
    libraryDependencies += "com.github.scopt" %% "scopt" % "4.0.1",
    libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.13.1",

    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x => MergeStrategy.first
    }
  )
  
